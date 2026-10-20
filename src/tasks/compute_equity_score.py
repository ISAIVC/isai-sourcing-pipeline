"""
Compute equity_score (Equity Story) for a batch of companies.

Score composite additif sur la dynamique et la qualite de la trajectoire de
financement (rythme des levees, efficacite capitalistique, tendance des
montants leves). Distinct de Maturity Fit (stade/age, pas encore implemente)
et de Traction (croissance operationnelle) : ne regarde que le sujet
"levees de fonds".

Regles convenues avec Emmanuel (2026-09-08, revues le meme jour apres analyse
Cowork -- cf. analyse-equity-story-suite.md et equity-story-spec-v2-sans-bloc-c.md
dans le projet Cowork) :
- Le tiering des investisseurs (ex Bloc C) est retire du score composite :
  gere a part via la table Supabase vc_funds + un futur filtre dedie, pas
  ici. Ne pas reintroduire un tiering ad hoc dans ce fichier.
- 4 sous-blocs independants, chacun sur 0-2 points :
    A1 - recence du dernier round (dispo si un round date existe) ; en
         jours reels, sweet spot 6-12 mois (cf. recalibrage 2026-10-14)
    A2 - cadence historique (dispo seulement si >= 2 rounds)
    B  - efficacite capitalistique : montant leve / age (dispo si
         total_amount_raised ET inc_date connus)
    D  - dynamique du montant entre rounds : dernier round vs round
         precedent, up-round vs down-round (dispo si >= 2 rounds ET
         montant connu pour les deux derniers)
- Score final = ratio (points obtenus / points possibles, sur les seuls
  sous-blocs disponibles), mappe sur une echelle 1-5 (1 = meilleure equity
  story, 5 = moins bonne) -- meme convention que Solution Fit, pour rester
  harmonise entre les scores et simplifier l'agregation dans le futur
  Global Score.
- Garde-fou de couverture : si moins de MIN_BLOCKS_FOR_TRUSTED_SCORE
  sous-blocs sont disponibles (en pratique : un seul bloc dispo, le cas 0
  etant deja traite a part), le score est force a NEUTRAL_SCORE (3, milieu
  de l'echelle 1-5) plutot que de faire confiance a un seul indice isole --
  flag low_confidence=True.
- Si AUCUN sous-bloc n'est disponible (pas de levee du tout, boot-strap),
  equity_score = None + flag no_funding=True : ce n'est pas une "mauvaise"
  equity story, c'est une absence de signal, donc pas de valeur numerique.

Recalibrage v1.1 (2026-10-14, regle de recence validee par Simon, cf. page
Notion "Equity Story") :
- A1 en jours reels depuis le dernier round : 6-12 mois = 2 points (sweet
  spot), 12-24 mois = 1 point, < 6 mois ou > 24 mois = 0 point.
- Plafonds explicites : pas de note 1 si le dernier round a moins de 6 mois
  ou plus de 3 ans (un 1 est ramene a 2).

A CONFIRMER / A COMPLETER avant mise en prod :
- Verifie sur Supabase (projet "Data Driven Sourcing", 2026-09-08) :
  equity_score (integer) existe deja dans business_computed_values, mais
  equity_score_coverage / no_funding / low_confidence n'existent PAS --
  migration a faire avant que cette tache puisse upserter quoi que ce
  soit (sinon erreur Postgrest "colonne inconnue").
- Bloc B : total_amount_raised confirme en dollars bruts (verifie sur
  Supabase, 2026-09-08 : openai.com = 180 020 000 000.0, mediane sur
  71 199 societes = 2 955 284 -- coherent uniquement si la colonne est
  en dollars bruts, pas en millions) -- converti en millions ici avant
  les seuils (10, 3 M USD/an). Plus une hypothese, confirme sur donnee
  reelle.
- Seuils (annees, M USD/an, ratio de step-up du Bloc D) repris du draft du
  collegue / proposes par analogie de forme, non calibres sur donnee
  reelle -- a valider empiriquement.
- La cadence (A2), la recence (A1) et le step-up (D) sont calcules ici
  directement depuis funding_rounds (source la plus fournie entre
  crunchbase et traxcn, dates+montants pris sur le meme lot) plutot que
  reutilises depuis business_computed_values.total_number_of_funding_rounds
  / first_vc_round_date / last_vc_round_date -- ces colonnes-la melangent
  les deux sources sans dedup pour les dates alors que le compte ne vient
  que d'une seule source, ce qui aurait fausse le calcul (cf.
  analyse-equity-story-draft.md / analyse-equity-story-suite.md dans le
  projet Cowork).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Optional

from prefect import task
from prefect.tasks import exponential_backoff

from src.config.clients import get_supabase_client
from src.utils.db import fetch_in_batches, upsert_in_batches
from src.utils.logger import get_logger

MIN_BLOCKS_FOR_TRUSTED_SCORE = 2
NEUTRAL_SCORE = 3  # milieu de l'echelle 1-5, force si un seul bloc dispo

# --- Bloc A1 (recence) : paliers en jours, regle de Simon (2026-10-14) ---
# Sweet spot entre 6 et 12 mois ; un tour trop recent (< 6 mois) ne vaut pas
# plus qu'un tour trop ancien (> 24 mois).
FRESH_DAYS = 183  # 6 mois
SWEET_SPOT_END_DAYS = 365  # 12 mois
STALE_DAYS = 730  # 24 mois
# Plafonds explicites : pas de note 1 si le dernier tour a moins de 6 mois ou
# plus de 3 ans -> la note est ramenee a 2.
CAP_FRESH_DAYS = FRESH_DAYS
CAP_STALE_DAYS = 1095  # 3 ans
BEST_SCORE_WHEN_CAPPED = 2


def _recency_points(days_since_last: int) -> int:
    """0-2 points selon l'anciennete du dernier tour (en jours reels)."""
    if days_since_last < FRESH_DAYS:  # < 6 mois
        return 0
    if days_since_last < SWEET_SPOT_END_DAYS:  # 6-12 mois : sweet spot
        return 2
    if days_since_last < STALE_DAYS:  # 12-24 mois
        return 1
    return 0  # > 24 mois


def _to_date(value) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _select_best_source_rounds(rounds: list[dict]) -> list[dict]:
    """
    Meme logique que compute_funding_metrics.py pour choisir la source
    (crunchbase vs traxcn) la plus fournie -- mais ici on prend le compte,
    les dates ET les montants depuis le MEME lot, pour que A1/A2/D restent
    coherents entre eux (cf. note en tete de fichier sur le bug de cadence
    documente dans compute_funding_metrics.py).
    """
    dated = [r for r in rounds if r.get("date")]
    cb = [r for r in dated if r.get("source") == "crunchbase"]
    tx = [r for r in dated if r.get("source") == "traxcn"]
    best = cb if len(cb) >= len(tx) else tx
    best = sorted(best, key=lambda r: r["date"])
    return best


def _score_equity_story(company: dict, rounds: list[dict], today: date) -> dict:
    domain = company["domain"]
    best_rounds = _select_best_source_rounds(rounds)
    n = len(best_rounds)

    available: list[tuple[int, int]] = []  # (points obtenus, points max) par sous-bloc

    # --- A1 : recence du dernier round ---
    last_date = _to_date(best_rounds[-1]["date"]) if best_rounds else None
    days_since_last: Optional[int] = None
    if last_date is not None:
        days_since_last = (today - last_date).days
        available.append((_recency_points(days_since_last), 2))

    # --- A2 : cadence historique (besoin d'au moins 2 rounds) ---
    if n >= 2:
        first_date = _to_date(best_rounds[0]["date"])
        if first_date is not None and last_date is not None:
            avg_gap_years = ((last_date - first_date).days / 365.25) / (n - 1)
            a2 = 2 if avg_gap_years < 1.5 else 1 if avg_gap_years <= 2.5 else 0
            available.append((a2, 2))

    # --- B : efficacite capitalistique ---
    total_raised = company.get("total_amount_raised")
    inc_date = company.get("inc_date")
    if total_raised is not None and inc_date:
        # total_amount_raised confirme en dollars bruts (cf. note en tete
        # de fichier, verifie sur Supabase) -- converti en millions pour
        # matcher les seuils M USD/an.
        total_raised_m = total_raised / 1_000_000
        age = max(today.year - int(inc_date), 1)
        raise_per_year = total_raised_m / age
        b = 2 if raise_per_year >= 10 else 1 if raise_per_year >= 3 else 0
        available.append((b, 2))

    # --- D : dynamique du montant entre rounds (dernier vs precedent) ---
    # Ratio entre deux montants de la meme colonne/source : l'unite
    # (dollars ou millions) n'a pas d'importance ici, contrairement au
    # Bloc B.
    if n >= 2:
        last_amount = best_rounds[-1].get("amount")
        prev_amount = best_rounds[-2].get("amount")
        if last_amount is not None and prev_amount is not None and prev_amount > 0:
            step_up_ratio = last_amount / prev_amount
            d = 2 if step_up_ratio >= 1.5 else 1 if step_up_ratio >= 0.8 else 0
            available.append((d, 2))

    coverage = len(available)

    if coverage == 0:
        return {
            "domain": domain,
            "equity_score": None,
            "equity_score_coverage": 0,
            "no_funding": True,
            "low_confidence": False,
        }

    if coverage < MIN_BLOCKS_FOR_TRUSTED_SCORE:
        return {
            "domain": domain,
            "equity_score": NEUTRAL_SCORE,
            "equity_score_coverage": coverage,
            "no_funding": False,
            "low_confidence": True,
        }

    raw = sum(points for points, _ in available)
    max_possible = sum(max_points for _, max_points in available)
    ratio = raw / max_possible

    # 1 = meilleure equity story, 5 = moins bonne -- meme convention que
    # Solution Fit. Seuils repris tels quels du draft d'origine (non
    # recalibres pour le passage Bloc C -> Bloc D).
    if ratio >= 0.75:
        score = 1
    elif ratio >= 0.55:
        score = 2
    elif ratio >= 0.35:
        score = 3
    elif ratio >= 0.15:
        score = 4
    else:
        score = 5

    # Plafonds explicites : pas de 1 si le dernier tour est trop recent
    # (< 6 mois) ou trop ancien (> 3 ans).
    if (
        score == 1
        and days_since_last is not None
        and (days_since_last < CAP_FRESH_DAYS or days_since_last > CAP_STALE_DAYS)
    ):
        score = BEST_SCORE_WHEN_CAPPED

    return {
        "domain": domain,
        "equity_score": score,
        "equity_score_coverage": coverage,
        "no_funding": False,
        "low_confidence": False,
    }


@task(
    name="compute_equity_score",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=4),
)
def compute_equity_score(domains: list[str]):
    logger = get_logger()
    client = get_supabase_client()
    today = date.today()

    logger.info(f"Starting compute_equity_score for {len(domains)} domains")

    # Step 1 – Fetch companies (fields needed for Bloc B)
    company_rows = fetch_in_batches(
        client,
        "companies",
        "domain",
        domains,
        select="id, domain, total_amount_raised, inc_date",
    )
    company_map = {r["domain"]: r for r in company_rows}
    id_to_domain = {r["id"]: r["domain"] for r in company_rows}
    logger.info(f"Fetched {len(company_map)} companies")

    if not company_map:
        logger.info("No companies found — nothing to score")
        return

    # Step 2 – Fetch funding rounds for these companies. date + amount +
    # source only : le tiering investisseurs (ex Bloc C) ne fait plus
    # partie de ce score, lead_investors/all_investors n'ont donc plus
    # besoin d'etre fetches ici.
    company_ids = list(id_to_domain.keys())
    funding_rows = fetch_in_batches(
        client,
        "funding_rounds",
        "company_id",
        company_ids,
        select="company_id, date, amount, source",
    )
    rounds_by_domain: dict[str, list[dict]] = defaultdict(list)
    for r in funding_rows:
        d = id_to_domain.get(r["company_id"])
        if d:
            rounds_by_domain[d].append(r)
    logger.info(
        f"Fetched {len(funding_rows)} funding rounds across {len(rounds_by_domain)} domains"
    )

    # Step 3 – Score each company
    records = [
        _score_equity_story(company, rounds_by_domain.get(domain, []), today)
        for domain, company in company_map.items()
    ]

    # Step 4 – Upsert
    if records:
        upsert_in_batches(
            client,
            "business_computed_values",
            records,
            on_conflict="domain",
            logger=logger,
        )
        logger.info(f"Upserted {len(records)} equity_score records")
    else:
        logger.info("No records to upsert")

    logger.info("compute_equity_score complete")
