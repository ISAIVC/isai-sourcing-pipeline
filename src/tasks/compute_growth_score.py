"""
Compute traction_score (Growth Score) for a batch of companies.

Score composite additif sur la traction commerciale et operationnelle
(preuves de marche : logos, volume de clients ; vitesse de croissance :
trafic web, effectifs). Distinct de Maturity Fit (stade, age, headcount
absolu) et d'Equity Story (trajectoire de financement) : ne regarde que les
*croissances* (`_growth_l12m`) et les *signaux clients*, pas les niveaux
absolus de headcount/financement ni le stade.

Regles convenues avec Emmanuel (session Cowork "ISAI-sourcing-fix",
2026-09-14 -- cf. draft Notion "Plateforme de Sourcing / Scores / Growth
Score" et analyse-growth-score-confrontation-code.md dans le projet Cowork) :
- 4 sous-blocs independants :
    A - logos Global 2000 (0-4 points, POIDS DOUBLE par rapport au draft
        d'origine -- decision Emmanuel du 2026-09-14 : un logo Global 2000
        est un signal plus rare/discriminant qu'une croissance de +15%,
        meme logique que le bloc "stade" de Maturity Fit qui pese aussi
        plus que les autres). Dispo si `global_2000_clients` non NULL
        (liste vide = vrai zero, compte comme signal faible, pas absence
        de donnee). Le champ melange key_clients ET key_partners avant le
        fuzzy-match (cf. fuzzy_matching_metrics.py) -- accepte tel quel,
        valide par Emmanuel : un logo cite comme partenaire tech est un
        signal de credibilite au meme titre qu'un client payant.
    B - volume de clients identifies (0-2 points). Dispo UNIQUEMENT si
        `web_scraping_enrichment.success IS TRUE` -- PAS un test
        `nb_of_clients_identified IS NOT NULL`, qui ne marche pas : le
        champ Pydantic de website_ai_parsing.py est non-nullable et le
        prompt Gemini renvoie 0 si l'info est absente (cf. SYSTEM_PROMPT,
        point 4), donc "vrai zero" et "pas d'info" sont indiscernables
        dans ce champ une fois `success=True`. Verifie sur Supabase
        (projet "Data Driven Sourcing", 2026-09-14) : quand `success` est
        NULL (3597 lignes) ou `false` (20027 lignes), nb_of_clients_identified
        est TOUJOURS NULL (confirme par lecture du bytecode de
        website_ai_parsing.py : la branche `answer is None` n'ecrit que
        `id`/`domain`/`success=False`, jamais ce champ) ; quand `success`
        est `true` (82312 lignes), il est TOUJOURS renseigne (dont 28% a
        exactement 0 -- un vrai signal faible, pas une absence de donnee).
    C - croissance du trafic web (0-2 points), avec garde-fou sur le
        trafic absolu. Dispo si `web_traffic_growth_l12m` non NULL ET
        (`web_traffic` NULL OU >= WEB_FLOOR) -- le garde-fou ne s'applique
        que quand le trafic est CONNU et bas (bruit sur petite base) ; s'il
        est manquant, on ne penalise pas l'absence de donnee (correction
        du 2026-09-14 : le pseudo-code du draft Notion traitait
        `web_traffic` manquant comme 0, ce qui faisait echouer le
        garde-fou et excluait le bloc a tort -- contradiction avec le
        tableau "Inputs" du meme draft qui dit "garde-fou ignore" dans ce
        cas).
    D - croissance des effectifs (0-2 points). Dispo si
        `headcount_growth_l12m` non NULL.
  Seuils C/D en POINTS DE POURCENTAGE, pas en ratio -- verifie sur Supabase
  (dealroom_enrichment, 2026-09-14) : headcount_growth_l12m va de -99 a
  7100 (mediane 2), web_traffic_growth_l12m de -100 a 1 549 900 (mediane
  18) -- des valeurs comme 120 ou 283 n'ont de sens qu'en points de %
  (120 = +120%), pas en ratio (ou elles vaudraient >1, absurde pour un
  ratio de croissance relative). Le suffixe Dealroom `_relative` du draft
  original suggerait un ratio -- infirme par la donnee reelle.
- Score final = ratio (points obtenus / points possibles, sur les seuls
  sous-blocs disponibles), mappe sur une echelle 1-5 (1 = meilleure
  traction), seuils 0.75/0.55/0.35/0.15 repris tels quels d'Equity Story/
  Maturity Fit (non recalibres pour ce score).
- Garde-fou de couverture, regle ASYMETRIQUE decidee par Emmanuel le
  2026-09-14 (differente d'Equity Story/Maturity Fit, qui forcent toujours
  la note neutre sous MIN_BLOCKS) : si coverage < MIN_BLOCKS_FOR_TRUSTED_SCORE
  (2) ET que le score calcule serait "bon" (1 ou 2 -- peu de preuves pour
  l'affirmer), on force NEUTRAL_SCORE (3). Mais si le score calcule est
  deja "mauvais" (3, 4 ou 5) sur ces memes blocs, on NE remonte PAS
  artificiellement vers la moyenne -- un signal faible n'a pas besoin
  d'autant de preuves qu'un signal fort pour etre pris au serieux.
- coverage == 0 (aucun bloc dispo) : traction_score = None, pas un mauvais
  score -- absence de donnee de traction != faible traction (meme logique
  qu'Equity Story `no_funding`).

A CONFIRMER / A COMPLETER avant mise en prod :
- Colonnes Supabase : verifie le 2026-09-14 (projet "Data Driven Sourcing")
  -- business_computed_values.traction_score et companies.traction_score_manual
  existent deja, deja cablees dans sourcing_view/sourcing_mv/
  match_companies_filtered. AUCUNE migration necessaire pour ce score, a la
  difference de Maturity Fit/Equity Story.
- En revanche, PAS de colonnes traction_score_coverage / traction_score_low_confidence
  dans business_computed_values (verifie le 2026-09-14, colonnes absentes)
  -- contrairement a equity_score_coverage/low_confidence et
  maturity_fit_coverage/low_confidence qui existent pour les deux autres
  scores. Cette tache calcule coverage/low_confidence en interne (utile
  pour le logging) mais ne les persiste PAS, pour eviter une erreur
  Postgrest "colonne inconnue" -- pas de migration faite ici (non demandee
  par Emmanuel dans cette session). A ajouter plus tard si on veut la
  meme tracabilite que les deux autres scores.
- Seuils (Global 2000 1/3, nb clients 5/20, croissance web 10/50,
  croissance effectifs 10/40, plancher trafic WEB_FLOOR=1000, poids du
  bloc A) repris du draft Notion / decisions de session, NON calibres sur
  echantillon reel -- a valider empiriquement (hors perimetre du projet
  Cowork "ISAI-sourcing-fix", verification empirique a faire separement).
- `dealroom_enrichment` n'est jamais mis a jour en place (insert_in_batches
  uniquement, cf. dealroom_enrichment.py) : plusieurs lignes peuvent
  s'accumuler par domaine au fil du temps -- on garde la plus recente par
  `sourcing_date`, meme logique que `compute_maturity.py` pour le
  headcount.
- `web_scraping_enrichment` est upsert par `id` (company id), pas par
  domaine (cf. website_ai_parsing.py, on_conflict="id") -- en theorie une
  seule ligne par company id, donc pas d'accumulation dans le temps comme
  pour dealroom_enrichment. On fetch par colonne `domain` malgre tout (les
  autres taches de ce fichier raisonnent par domaine) ; en cas de doublon
  improbable sur un meme domaine, la derniere ligne lue l'emporte -- meme
  simplification que `company_by_domain` dans compute_maturity.py/
  compute_equity_score.py.
"""

from __future__ import annotations

from typing import Optional

from prefect import task
from prefect.tasks import exponential_backoff

from src.config.clients import get_supabase_client
from src.utils.db import fetch_in_batches, upsert_in_batches
from src.utils.logger import get_logger

MIN_BLOCKS_FOR_TRUSTED_SCORE = 2
NEUTRAL_SCORE = 3  # milieu de l'echelle 1-5, force si coverage < MIN_BLOCKS ET score seraiit "bon"
GOOD_SCORE_CEILING = 2  # un score mappe a 1 ou 2 est juge "bon" pour la regle asymetrique ci-dessus

GLOBAL_2000_MAX_POINTS = 4  # poids double vs les autres blocs, decision du 2026-09-14
CLIENTS_MAX_POINTS = 2
WEB_GROWTH_MAX_POINTS = 2
HEADCOUNT_GROWTH_MAX_POINTS = 2

WEB_TRAFFIC_FLOOR = 1000  # garde-fou Bloc C, repris du draft, non calibre


def global_2000_points(global_2000_clients) -> Optional[int]:
    """0/2/4 points. None si `global_2000_clients` est NULL (jamais calcule
    pour ce domaine). Une liste vide compte comme un vrai zero (0 pts)."""
    if global_2000_clients is None:
        return None
    n = len(global_2000_clients)
    if n >= 3:
        return GLOBAL_2000_MAX_POINTS
    if n >= 1:
        return GLOBAL_2000_MAX_POINTS // 2
    return 0


def clients_points(web_scraping_success, nb_of_clients_identified) -> Optional[int]:
    """0/1/2 points. None si le scraping n'a pas reussi (`success` NULL ou
    False) -- PAS un test sur `nb_of_clients_identified IS NULL`, qui ne
    distingue jamais "pas d'info" de "vrai zero" une fois le scraping
    reussi (cf. docstring en tete de fichier)."""
    if web_scraping_success is not True or nb_of_clients_identified is None:
        return None
    if nb_of_clients_identified >= 20:
        return 2
    if nb_of_clients_identified >= 5:
        return 1
    return 0


def web_growth_points(web_traffic_growth_l12m, web_traffic) -> Optional[int]:
    """0/1/2 points, en points de pourcentage (cf. docstring). None si la
    croissance est absente, ou si le trafic absolu est CONNU et sous
    WEB_TRAFFIC_FLOOR (croissance en % sur base minuscule = bruit). Un
    trafic absolu manquant n'exclut PAS le bloc -- seul un trafic connu et
    bas le fait."""
    if web_traffic_growth_l12m is None:
        return None
    if web_traffic is not None and web_traffic < WEB_TRAFFIC_FLOOR:
        return None
    if web_traffic_growth_l12m >= 50:
        return 2
    if web_traffic_growth_l12m >= 10:
        return 1
    return 0


def headcount_growth_points(headcount_growth_l12m) -> Optional[int]:
    """0/1/2 points, en points de pourcentage (cf. docstring). None si
    absent."""
    if headcount_growth_l12m is None:
        return None
    if headcount_growth_l12m >= 40:
        return 2
    if headcount_growth_l12m >= 10:
        return 1
    return 0


def _ratio_to_traction(ratio: float) -> int:
    """1 = meilleure traction, 5 = moins bonne. Seuils repris tels quels
    d'Equity Story / Maturity Fit, non recalibres ici."""
    if ratio >= 0.75:
        return 1
    if ratio >= 0.55:
        return 2
    if ratio >= 0.35:
        return 3
    if ratio >= 0.15:
        return 4
    return 5


def _score_growth(
    global_2000_clients,
    web_scraping_success,
    nb_of_clients_identified,
    web_traffic_growth_l12m,
    web_traffic,
    headcount_growth_l12m,
) -> dict:
    """Calcule traction_score + metadonnees de couverture (coverage,
    low_confidence non persistes -- cf. docstring en tete de fichier)."""
    blocks: list[tuple[int, int]] = []  # (points obtenus, points max) par sous-bloc

    a = global_2000_points(global_2000_clients)
    if a is not None:
        blocks.append((a, GLOBAL_2000_MAX_POINTS))

    b = clients_points(web_scraping_success, nb_of_clients_identified)
    if b is not None:
        blocks.append((b, CLIENTS_MAX_POINTS))

    c = web_growth_points(web_traffic_growth_l12m, web_traffic)
    if c is not None:
        blocks.append((c, WEB_GROWTH_MAX_POINTS))

    d = headcount_growth_points(headcount_growth_l12m)
    if d is not None:
        blocks.append((d, HEADCOUNT_GROWTH_MAX_POINTS))

    coverage = len(blocks)

    if coverage == 0:
        return {
            "traction_score": None,
            "traction_score_coverage": 0,
            "traction_score_low_confidence": False,
        }

    raw = sum(points for points, _ in blocks)
    max_possible = sum(max_points for _, max_points in blocks)
    ratio = raw / max_possible
    mapped = _ratio_to_traction(ratio)

    if coverage < MIN_BLOCKS_FOR_TRUSTED_SCORE:
        if mapped <= GOOD_SCORE_CEILING:
            # Peu de preuves pour affirmer un "bon" score -> on plafonne a
            # la moyenne plutot que de faire confiance a 1-2 signaux forts
            # isoles.
            return {
                "traction_score": NEUTRAL_SCORE,
                "traction_score_coverage": coverage,
                "traction_score_low_confidence": True,
            }
        # Score deja "mauvais" (3/4/5) sur ces memes blocs : pas besoin de
        # plus de preuves pour le prendre au serieux, pas de remontee
        # artificielle vers la moyenne (decision Emmanuel, 2026-09-14).
        return {
            "traction_score": mapped,
            "traction_score_coverage": coverage,
            "traction_score_low_confidence": True,
        }

    return {
        "traction_score": mapped,
        "traction_score_coverage": coverage,
        "traction_score_low_confidence": False,
    }


def _latest_dealroom_rows_by_domain(rows: list[dict]) -> dict[str, dict]:
    """dealroom_enrichment n'est jamais mis a jour en place (insert_in_batches
    uniquement, cf. dealroom_enrichment.py) : plusieurs lignes peuvent
    s'accumuler par domaine au fil du temps. On garde la plus recente par
    `sourcing_date` (meme logique que compute_maturity.py pour le
    headcount)."""
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
    return latest


@task(
    name="compute_growth_score",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=4),
)
def compute_growth_score(domains: list[str]):
    logger = get_logger()
    client = get_supabase_client()

    logger.info(f"Starting compute_growth_score for {len(domains)} domains")

    # Step 1 -- Bloc A : global_2000_clients (business_computed_values)
    bcv_rows = fetch_in_batches(
        client,
        "business_computed_values",
        "domain",
        domains,
        select="domain, global_2000_clients",
    )
    global_2000_by_domain = {r["domain"]: r.get("global_2000_clients") for r in bcv_rows}
    logger.info(f"Fetched global_2000_clients for {len(global_2000_by_domain)} domains")

    # Step 2 -- Bloc B : success + nb_of_clients_identified (web_scraping_enrichment)
    scraping_rows = fetch_in_batches(
        client,
        "web_scraping_enrichment",
        "domain",
        domains,
        select="domain, success, nb_of_clients_identified",
    )
    scraping_by_domain = {r["domain"]: r for r in scraping_rows}
    logger.info(f"Fetched web_scraping_enrichment for {len(scraping_by_domain)} domains")

    # Step 3 -- Blocs C/D : croissances Dealroom, derniere valeur par domaine
    dealroom_rows = fetch_in_batches(
        client,
        "dealroom_enrichment",
        "domain",
        domains,
        select="domain, headcount_growth_l12m, web_traffic_growth_l12m, web_traffic, sourcing_date",
    )
    dealroom_by_domain = _latest_dealroom_rows_by_domain(dealroom_rows)
    logger.info(f"Fetched dealroom growth data for {len(dealroom_by_domain)} domains")

    # Step 4 -- score chaque domaine
    records = []
    coverage_counts: dict[int, int] = {}
    low_confidence_count = 0
    for domain in domains:
        scraping = scraping_by_domain.get(domain, {})
        dealroom = dealroom_by_domain.get(domain, {})
        result = _score_growth(
            global_2000_clients=global_2000_by_domain.get(domain),
            web_scraping_success=scraping.get("success"),
            nb_of_clients_identified=scraping.get("nb_of_clients_identified"),
            web_traffic_growth_l12m=dealroom.get("web_traffic_growth_l12m"),
            web_traffic=dealroom.get("web_traffic"),
            headcount_growth_l12m=dealroom.get("headcount_growth_l12m"),
        )
        coverage_counts[result["traction_score_coverage"]] = (
            coverage_counts.get(result["traction_score_coverage"], 0) + 1
        )
        if result["traction_score_low_confidence"]:
            low_confidence_count += 1
        # Seules domain/traction_score sont persistees : pas de colonnes
        # traction_score_coverage/low_confidence en base (cf. docstring).
        records.append({"domain": domain, "traction_score": result["traction_score"]})

    logger.info(f"Coverage distribution: {coverage_counts}")
    logger.info(f"{low_confidence_count} domains scored with low_confidence (not persisted)")

    # Step 5 -- upsert
    if records:
        upsert_in_batches(
            client,
            "business_computed_values",
            records,
            on_conflict="domain",
            logger=logger,
        )
        logger.info(f"Upserted {len(records)} traction_score records")
    else:
        logger.info("No records to upsert")

    logger.info("compute_growth_score complete")
