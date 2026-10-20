"""
Compute maturity_fit_cg / maturity_fit_by (Maturity Fit) pour un lot de societes.

Score composite additif sur le cycle de vie de la societe (stade + age +
headcount + montant total leve), decline en 2 versions -- une par fonds
(CG = Capgemini, BY = Bouygues) -- car les deux fonds ne visent pas la meme
fenetre de stade (BY : seed -> Series C, CG : Series A -> C/D). Distinct
d'Equity Story (trajectoire/dynamique de financement) et de Traction
(croissance operationnelle) : ne regarde que "ou en est la boite dans son
cycle de vie, par rapport a la cible de chaque fonds".

Regles convenues avec Emmanuel (2026-09-08, session Cowork -- cf.
maturity-fit-draft.md, analyse-maturity-fit-confrontation-code.md dans le
projet Cowork "ISAI-sourcing-fix") :
- 4 sous-blocs independants, chacun avec son propre max de points (le stade
  pese plus lourd que les autres, volontairement) :
    stade      - 0 a 4 points, base recentree par fonds sur la fenetre
                 cible (4 = coeur de cible, 0 = loin en dehors) --
                 disponible si `vc_current_stage` normalise vers un stade
                 canonique connu.
    age        - 0 a 2 points, disponible si `inc_date` present et
                 plausible (1900 < inc_date <= annee courante). Signal
                 d'anciennete suspecte pour le stade affiche (ex. societe
                 fondee il y a 23 ans encore etiquetee "seed").
    headcount  - 0 a 3 points, disponible si `headcount` > 0 (Dealroom,
                 `employees_latest`).
    montant     - 0 a 2 points, disponible si `total_amount_raised` connu.
                 Fourchette absolue (proxy de stade, pas un taux ni une
                 tendance -- cf. analyse-maturity-fit-confrontation-code.md
                 section 5 pour la justification du pourquoi ce signal ne
                 duplique pas les blocs B/D d'Equity Story). Sweet spot
                 different par fonds : 5-20M$ pour CG, 3-20M$ pour BY.
- Score final par fonds = ratio (points obtenus / points possibles, sur les
  seuls sous-blocs disponibles), mappe sur une echelle 1-5 (1 = le mieux
  positionne pour ce fonds, 5 = le moins bien positionne) -- meme
  convention que Solution Fit / Equity Story.
- coverage (nombre de sous-blocs disponibles) est IDENTIQUE pour CG et BY :
  la disponibilite d'un bloc ne depend pas du fonds, seuls les points
  attribues a l'interieur du bloc stade/montant different.
- Garde-fou de couverture :
    coverage == 0 : maturity_fit_cg/by = None, low_confidence = False
                    (pas de signal du tout, pas une "mauvaise" note).
    coverage == 1 et le seul bloc dispo est le stade : le stade seul est
                    juge assez fiable pour ne PAS forcer une note neutre --
                    maturity = conversion directe des points stade
                    (STAGE_MAX_POINTS + 1 - points), low_confidence = True.
                    C'est une divergence assumee par rapport a Equity Story
                    (ou un seul sous-bloc isole force toujours la note
                    neutre) : ici le stade seul est deja le signal
                    dominant du score, contrairement aux sous-blocs
                    d'Equity Story qui sont tous d'importance comparable.
    coverage == 1 et c'est un autre bloc (stade inconnu) : maturity =
                    NEUTRAL_SCORE (3), low_confidence = True -- meme
                    logique qu'Equity Story, un seul indice isole hors
                    stade n'est pas jugee fiable.
    coverage >= 2 : ratio -> seuils 1-5 (repris tels quels d'Equity Story :
                    0.75 / 0.55 / 0.35 / 0.15 -- non recalibres pour ce
                    score, a valider empiriquement).

Recalibrage v1.1 (2026-10-14, cf. page Notion "Maturity Fit", a faire valider
par Simon) -- les regles ci-dessus sont remplacees par :
- Bloc montant : 0-4 points (au lieu de 0-2), degrade plus raide au-dessus de
  20 M$ (20-50 M$ = 2, 50-100 M$ = 1, > 100 M$ = 0). Total des blocs : 13.
- Bloc headcount : fenetre cible (11-149 = 3, 150-499 = 2, >= 500 = 1,
  <= 10 = 1) au lieu de "plus c'est gros, mieux c'est".
- Seuils de ratio durcis : 0.85 / 0.65 / 0.45 / 0.25 (au lieu de 0.75 / 0.55
  / 0.35 / 0.15).
- Plafonds : total leve > 150 M$ -> note 5 pour les deux fonds ; pas de note
  1 avec moins de 3 blocs disponibles (ramenee a 2).

A CONFIRMER / A COMPLETER avant mise en prod :
- Colonnes Supabase : maturity_fit_cg, maturity_fit_by, maturity_fit_coverage
  et maturity_fit_low_confidence existent deja dans business_computed_values
  (verifie le 2026-10-13), aucune migration necessaire.
- Colonne `maturity_fit` (singulier) : decision du 2026-10-13 (validee par
  Simon), elle est ECRITE par cette tache, a partir de maturity_fit_cg ou
  maturity_fit_by selon le scope de la societe. C'est elle que lisent
  sourcing_view / sourcing_mv (COALESCE(c.maturity_fit_manual,
  bcv.maturity_fit)), ce qui evite de modifier la vue materialisee.
  Resolution du scope : COALESCE(companies.scope_manual,
  business_computed_values.scope) ; BOTH -> maturity_fit_cg (Capgemini par
  defaut) ; scope NULL ou non reconnu -> maturity_fit = None. Meme regle que
  compute_global_score.py (a garder alignees). Limite : si un scope est
  corrige a la main apres coup, `maturity_fit` ne suit qu'au prochain
  recalcul de cette tache (maturity_fit_cg/by, eux, restent toujours ecrits).
- Dependance d'ordre : cette tache lit `vc_current_stage` depuis
  `business_computed_values`, colonne ecrite par `compute_funding_metrics`
  (qui applique le fallback companies -> dernier round de funding_rounds),
  et `scope`, colonne ecrite par `annotate_company_tags`.
  Dans `business_processing_flow`, ces taches sont soumises dans le
  meme batch parallele : `compute_maturity` doit attendre le `.result()`
  de `compute_funding_metrics` ET de `annotate_company_tags` avant de lire
  business_computed_values pour ne pas lire une valeur perimee -- gere
  explicitement dans `src/flows/business_processing.py`, a ne pas casser si
  ce fichier est retouche.
- Mapping stade brut -> stade canonique (STAGE_RAW_TO_CANON) repris du
  draft d'origine, NON exhaustif et NON verifie sur les vraies valeurs de
  `vc_current_stage` / `investment_type` / `round_name` en base (3
  vocabulaires distincts, cf. analyse-maturity-fit-confrontation-code.md
  section 1) -- tout libelle brut non reconnu retombe sur canon=None
  (stade indisponible), pas une valeur par defaut inventee.
- Seuils (age 15/25 ans, headcount 10/150/500, montant 2/3/5/20/150 M$,
  points stade/ratio) non calibres sur echantillon reel -- a valider
  empiriquement, comme pour Equity Story.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Optional

from prefect import task
from prefect.tasks import exponential_backoff

from src.config.clients import get_supabase_client
from src.utils.db import fetch_in_batches, upsert_in_batches
from src.utils.logger import get_logger

NEUTRAL_SCORE = 3  # milieu de l'echelle 1-5, force si 1 seul bloc dispo hors stade

STAGE_MAX_POINTS = 4
AGE_MAX_POINTS = 2
HEADCOUNT_MAX_POINTS = 3
AMOUNT_MAX_POINTS = 4

# Plafonds (recalibrage 2026-10-14)
AMOUNT_CAP_M = 150  # total leve > 150 M$ -> note 5
WORST_SCORE = 5
MIN_BLOCKS_FOR_BEST_SCORE = 3  # pas de note 1 en dessous
BEST_SCORE_WHEN_CAPPED = 2

# --- Bloc stade : mapping libelle brut -> stade canonique -------------------
# Provisoire / non exhaustif, cf. docstring ci-dessus.
STAGE_RAW_TO_CANON: dict[str, str] = {
    # growth+ / exit
    "series f": "growth_plus",
    "series g": "growth_plus",
    "series h": "growth_plus",
    "series i": "growth_plus",
    "series j": "growth_plus",
    "growth": "growth_plus",
    "late stage": "growth_plus",
    "private equity": "growth_plus",
    "mezzanine": "growth_plus",
    "pre ipo": "growth_plus",
    "public": "growth_plus",
    "ipo": "growth_plus",
    "acquired": "growth_plus",
    "m&a": "growth_plus",
    # late VC
    "series c": "late_vc",
    "series d": "late_vc",
    "series e": "late_vc",
    # early VC
    "series a": "early_vc",
    "series b": "early_vc",
    "early stage": "early_vc",
    "venture": "early_vc",
    # seed
    "seed": "seed",
    "convertible note": "seed",
    "grant": "seed",
    # pre-seed
    "pre seed": "pre_seed",
    "angel": "pre_seed",
    "idea": "pre_seed",
    "bootstrapped": "pre_seed",
    "unfunded": "pre_seed",
    "non equity assistance": "pre_seed",
}

# Points par stade canonique, recentres par fonds sur sa fenetre cible
# (4 = coeur de cible, 0 = loin en dehors). Cf. docstring.
STAGE_POINTS: dict[str, dict[str, int]] = {
    "cg": {  # Capgemini : Series A -> C/D
        "growth_plus": 1,
        "late_vc": 4,
        "early_vc": 4,
        "seed": 3,
        "pre_seed": 1,
    },
    "by": {  # Bouygues : seed -> Series C
        "growth_plus": 0,
        "late_vc": 3,
        "early_vc": 4,
        "seed": 4,
        "pre_seed": 2,
    },
}


def normalize_stage(raw: Optional[str]) -> Optional[str]:
    """Normalise un libelle de stade brut vers un stade canonique connu.

    Retourne None si le libelle est vide ou non reconnu -- on n'invente
    jamais un stade par defaut (cf. docstring : mapping provisoire).
    """
    if not raw:
        return None
    s = str(raw).strip().lower()
    s = re.sub(r"[_-]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if s in STAGE_RAW_TO_CANON:
        return STAGE_RAW_TO_CANON[s]
    if s.startswith("post ipo"):
        return "growth_plus"
    if s.endswith("crowdfunding"):
        return "seed"
    return None


def age_points(inc_date, year: int) -> Optional[int]:
    """0-2 points. None si inc_date absent ou implausible (pas une annee
    entre 1900 et l'annee courante -- cf. companies_reconciliation.py, ou
    inc_date est deja caste en annee entiere avant stockage)."""
    if not inc_date:
        return None
    try:
        inc = int(inc_date)
    except (TypeError, ValueError):
        return None
    if not (1900 < inc <= year):
        return None
    age = year - inc
    if age >= 25:
        return 0
    if age >= 15:
        return 1
    return 2


def headcount_points(headcount) -> Optional[int]:
    """0-3 points, fenetre cible (recalibrage 2026-10-14). None si headcount
    absent ou <= 0.

    11-149 = 3 (coeur de cible), 150-499 = 2, >= 500 = 1, <= 10 = 1 (tres
    petite structure : pas un signal negatif en soi)."""
    if not headcount or headcount <= 0:
        return None
    if headcount >= 500:
        return 1
    if headcount >= 150:
        return 2
    if headcount >= 11:
        return 3
    return 1


def amount_points(total_raised, fund: str) -> Optional[int]:
    """0-4 points par fonds. None si total_amount_raised absent.

    Recalibrage 2026-10-14 : bloc doublé (0-4), degrade raide au-dessus de
    20 M$. Le cas > 150 M$ (note 5 forcee) est gere dans _score_maturity.

    total_amount_raised est confirme en dollars bruts (cf.
    equity-story-spec-v2-sans-bloc-c.md, verifie sur Supabase le
    2026-09-08) -- converti en millions ici avant application des seuils,
    meme conversion que le Bloc B d'Equity Story."""
    if total_raised is None:
        return None
    m = total_raised / 1_000_000
    sweet_spot_floor = 3 if fund == "by" else 5
    if m < 2:
        return 0
    if m < sweet_spot_floor:
        return 2
    if m <= 20:
        return 4
    if m <= 50:
        return 2
    if m <= 100:
        return 1
    return 0


def resolve_maturity_scope(
    scope_manual: Optional[str], scope: Optional[str]
) -> Optional[str]:
    """'cg' / 'by' / None : fonds dont on expose la note dans `maturity_fit`.

    COALESCE(companies.scope_manual, business_computed_values.scope).
    BOTH -> 'cg' (Capgemini par defaut, decision du 2026-10-13, validee par
    Simon). NULL ou valeur non reconnue -> None (pas de note exposee). Meme
    regle que `_effective_scope` dans compute_global_score.py."""
    effective = scope_manual if scope_manual else scope
    if effective in ("CG", "BOTH"):
        return "cg"
    if effective == "BY":
        return "by"
    return None


def _ratio_to_maturity(ratio: float) -> int:
    """1 = le mieux positionne pour ce fonds, 5 = le moins bien positionne.
    Seuils durcis le 2026-10-14 (0.85 / 0.65 / 0.45 / 0.25)."""
    if ratio >= 0.85:
        return 1
    if ratio >= 0.65:
        return 2
    if ratio >= 0.45:
        return 3
    if ratio >= 0.25:
        return 4
    return 5


def _score_maturity(
    stage_raw, inc_date, headcount, total_raised, year: int
) -> dict:
    canon = normalize_stage(stage_raw)

    # blocks : nom -> (max_points, points_cg, points_by)
    blocks: dict[str, tuple[int, int, int]] = {}

    if canon is not None:
        blocks["stage"] = (
            STAGE_MAX_POINTS,
            STAGE_POINTS["cg"][canon],
            STAGE_POINTS["by"][canon],
        )

    ap = age_points(inc_date, year)
    if ap is not None:
        blocks["age"] = (AGE_MAX_POINTS, ap, ap)

    hp = headcount_points(headcount)
    if hp is not None:
        blocks["headcount"] = (HEADCOUNT_MAX_POINTS, hp, hp)

    if total_raised is not None:
        blocks["amount"] = (
            AMOUNT_MAX_POINTS,
            amount_points(total_raised, "cg"),
            amount_points(total_raised, "by"),
        )

    coverage = len(blocks)

    if coverage == 0:
        return {
            "maturity_fit_cg": None,
            "maturity_fit_by": None,
            "maturity_fit_coverage": 0,
            "maturity_fit_low_confidence": False,
        }

    if coverage == 1 and "stage" in blocks:
        _, cg_points, by_points = blocks["stage"]
        # Pas de note 1 avec moins de 3 blocs (recalibrage 2026-10-14).
        return {
            "maturity_fit_cg": max(STAGE_MAX_POINTS + 1 - cg_points, BEST_SCORE_WHEN_CAPPED),
            "maturity_fit_by": max(STAGE_MAX_POINTS + 1 - by_points, BEST_SCORE_WHEN_CAPPED),
            "maturity_fit_coverage": coverage,
            "maturity_fit_low_confidence": True,
        }

    if coverage == 1:
        return {
            "maturity_fit_cg": NEUTRAL_SCORE,
            "maturity_fit_by": NEUTRAL_SCORE,
            "maturity_fit_coverage": coverage,
            "maturity_fit_low_confidence": True,
        }

    max_total = sum(b[0] for b in blocks.values())
    cg_total = sum(b[1] for b in blocks.values())
    by_total = sum(b[2] for b in blocks.values())

    cg_score = _ratio_to_maturity(cg_total / max_total)
    by_score = _ratio_to_maturity(by_total / max_total)

    # Plafonds (recalibrage 2026-10-14) :
    # - pas de note 1 avec moins de 3 blocs disponibles -> ramenee a 2 ;
    # - total leve > 150 M$ -> note 5 pour les deux fonds (prioritaire).
    if coverage < MIN_BLOCKS_FOR_BEST_SCORE:
        if cg_score == 1:
            cg_score = BEST_SCORE_WHEN_CAPPED
        if by_score == 1:
            by_score = BEST_SCORE_WHEN_CAPPED
    if total_raised is not None and total_raised / 1_000_000 > AMOUNT_CAP_M:
        cg_score = WORST_SCORE
        by_score = WORST_SCORE

    return {
        "maturity_fit_cg": cg_score,
        "maturity_fit_by": by_score,
        "maturity_fit_coverage": coverage,
        "maturity_fit_low_confidence": False,
    }


def _latest_headcount_by_domain(rows: list[dict]) -> dict[str, Optional[int]]:
    """dealroom_enrichment n'est jamais mis a jour en place (insert_in_batches
    uniquement, cf. dealroom_enrichment.py) : plusieurs lignes peuvent
    s'accumuler par domaine au fil du temps. On garde la plus recente par
    `sourcing_date` (pas `updated_at`, pas garanti fiable sur cette table)."""
    latest: dict[str, dict] = {}
    for row in rows:
        domain = row.get("domain")
        if not domain:
            continue
        current = latest.get(domain)
        if current is None or (row.get("sourcing_date") or "") > (
            current.get("sourcing_date") or ""
        ):
            latest[domain] = row
    return {domain: row.get("headcount") for domain, row in latest.items()}


@task(
    name="compute_maturity",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=4),
)
def compute_maturity(domains: list[str]):
    logger = get_logger()
    client = get_supabase_client()
    year = date.today().year

    logger.info(f"Starting compute_maturity for {len(domains)} domains")

    # Step 1 -- vc_current_stage resolu, ecrit par compute_funding_metrics
    # (companies.vc_current_stage avec fallback dernier round). Doit
    # tourner APRES compute_funding_metrics dans le meme batch -- gere
    # dans business_processing.py, cf. docstring en tete de fichier.
    bcv_rows = fetch_in_batches(
        client,
        "business_computed_values",
        "domain",
        domains,
        select="domain, vc_current_stage, scope",
    )
    stage_by_domain = {r["domain"]: r.get("vc_current_stage") for r in bcv_rows}
    scope_by_domain = {r["domain"]: r.get("scope") for r in bcv_rows}
    logger.info(f"Fetched vc_current_stage for {len(stage_by_domain)} domains")

    # Step 2 -- inc_date / total_amount_raised / scope_manual (companies)
    company_rows = fetch_in_batches(
        client,
        "companies",
        "domain",
        domains,
        select="domain, inc_date, total_amount_raised, scope_manual",
    )
    company_by_domain = {r["domain"]: r for r in company_rows}
    logger.info(f"Fetched {len(company_by_domain)} companies")

    # Step 3 -- headcount (Dealroom), derniere valeur par domaine
    dealroom_rows = fetch_in_batches(
        client,
        "dealroom_enrichment",
        "domain",
        domains,
        select="domain, headcount, sourcing_date",
    )
    headcount_by_domain = _latest_headcount_by_domain(dealroom_rows)
    logger.info(f"Fetched headcount for {len(headcount_by_domain)} domains")

    # Step 4 -- score chaque domaine
    records = []
    for domain in domains:
        company = company_by_domain.get(domain, {})
        record = _score_maturity(
            stage_raw=stage_by_domain.get(domain),
            inc_date=company.get("inc_date"),
            headcount=headcount_by_domain.get(domain),
            total_raised=company.get("total_amount_raised"),
            year=year,
        )
        record["domain"] = domain

        # Colonne unique `maturity_fit` lue par sourcing_view / sourcing_mv :
        # note du fonds resolu par le scope (BOTH -> CG), None si pas de scope.
        fund = resolve_maturity_scope(company.get("scope_manual"), scope_by_domain.get(domain))
        record["maturity_fit"] = record[f"maturity_fit_{fund}"] if fund else None
        records.append(record)

    # Step 5 -- upsert
    if records:
        upsert_in_batches(
            client,
            "business_computed_values",
            records,
            on_conflict="domain",
            logger=logger,
        )
        logger.info(f"Upserted {len(records)} maturity_fit records")
    else:
        logger.info("No records to upsert")

    logger.info("compute_maturity complete")
