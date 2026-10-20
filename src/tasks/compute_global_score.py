"""
Compute global_fund_score (Global Score) for a batch of companies.

Score composite agregeant les scores "calcul de metriques" deja codes
(Equity Story, Maturity Fit, Growth Score), plus un bonus lie au profil
fondateur. Exclut explicitement Solution Fit et Business Fit (methodes
1-NN sur embeddings) -- decision Emmanuel du 2026-09-14 ("on regardera une
autre fois"), pas dans le perimetre de ce score.

Regles convenues avec Emmanuel (session Cowork "ISAI-sourcing-fix",
2026-09-14) :
- Convention : ECHELLE 1-5, 1 = MEILLEUR, identique a Equity Story/
  Maturity Fit/Growth Score/Solution Fit -- PAS d'inversion. Une premiere
  proposition d'inverser la convention pour ce score (haut = bon) a ete
  faite puis explicitement abandonnee par Emmanuel : "si c'est deja
  harmonise avec les fit alors on laisse comme ca (global score bas = bon
  score)".
- 3 composants, poids EGAUX (1/3 chacun) par defaut -- aucune ponderation
  differenciee demandee a ce stade, valeur facilement ajustable (constantes
  ci-dessous) :
    - `equity_score` (business_computed_values, ecrit par compute_equity_score)
    - `maturity_fit_cg` OU `maturity_fit_by` selon le scope de la societe
      (business_computed_values, ecrit par compute_maturity) -- resolution
      du scope identique a l'alias `maturity_fit` DOCUMENTE (pas applique)
      sur la fiche Notion Maturity Fit :
      COALESCE(companies.scope_manual, business_computed_values.scope).
      Decision Emmanuel du 2026-10-13 (accord de Simon obtenu) : scope BOTH
      -> maturity_fit_cg (Capgemini par defaut). Scope NULL / valeur non
      reconnue : composant exclu (traite comme "manquant"), meme regle que
      l'alias maturity_fit documente sur la fiche Notion Maturity Fit.
    - `traction_score` (business_computed_values, ecrit par compute_growth_score)
- PAS de seuil minimum de composants disponibles pour calculer un score
  (contrairement a Equity Story/Maturity Fit/Growth Score en interne).
  Decision explicite d'Emmanuel : "on traite deja ces cas dans le scoring
  en mettant a la moyenne si on a pas assez de donnees donc le global score
  devrait refleter ca" -- chaque composant est deja neutralise/degrade en
  amont quand SA propre donnee est insuffisante, donc le Global Score fait
  simplement la moyenne des composants disponibles, poids renormalises
  (coverage == 0 -> global_fund_score = None, absence de signal, pas un
  mauvais score -- meme logique que partout ailleurs dans ce repo).
- Bonus fondateur serie : +0.5 point d'AMELIORATION si
  `business_computed_values.serial_entrepreneur` est True (colonne deja
  existante, ecrite par compute_founders_values -- AUCUNE modification de
  compute_founders_values.py necessaire, le booleen suffit ; les blocs
  "background"/"education" envisages initialement sont laisses de cote,
  pas de formule definie pour l'instant). PAS de malus si False/absent :
  decision explicite d'Emmanuel ("pas en malus, si pas d'info ca sert a
  rien d'enlever un truc a la note") -- le prompt Gemini de
  compute_founders_values.py renvoie par defaut `false` aussi bien pour
  "confirme non serial" que pour "info insuffisante" (cf. SYSTEM_PROMPT,
  regle "default to serial_entrepreneur = false"), ces deux cas sont
  indiscernables en base ; ne jamais penaliser evite que ce flou ne se
  traduise par une baisse de note injustifiee.
  IMPORTANT sur le sens : l'echelle etant 1=meilleur, "ameliorer" le score
  signifie le FAIRE BAISSER -- le bonus est donc SOUSTRAIT de la moyenne
  (`base - 0.5`, borne a 1 au minimum), pas ajoute arithmetiquement. A
  confirmer que cette lecture correspond bien a l'intention d'Emmanuel.
- Colonne cible : `business_computed_values.global_fund_score`, verifiee
  INTEGER sur Supabase le 2026-09-14 (deja existante, aucune migration
  necessaire pour la colonne elle-meme). Consequence : le resultat
  (moyenne +/- bonus, potentiellement non entier) est arrondi avant
  ecriture -- arrondi standard "half up" (`floor(x + 0.5)`), PAS l'arrondi
  bancaire de `round()` Python (qui arrondirait 2.5 vers 2, pas 3), pour un
  comportement previsible. Consequence pratique : le bonus de 0.5 point ne
  change la valeur finale ecrite QUE quand il fait franchir un seuil
  d'arrondi (ex : base=2.6 -> 3 sans bonus, base=2.1 -> 2 avec bonus :
  effet visible ; base=1.9 -> 2 sans bonus, base=1.4 -> 1 avec bonus :
  effet visible aussi ; mais base=2.2 -> 2 sans bonus, base=1.7 -> 2 avec
  bonus : aucun effet visible). Non calibre/verifie sur echantillon reel.
- Pas de colonne `global_score_coverage` persistee (meme choix que Growth
  Score : colonne absente de business_computed_values, pas de migration
  demandee dans cette session) -- coverage/details calcules en interne pour
  le logging uniquement.

A CONFIRMER / A COMPLETER avant mise en prod :
- Le sens du bonus (soustraction, cf. ci-dessus) n'a pas ete verifie
  explicitement avec un exemple chiffre par Emmanuel -- a reconfirmer.
- Poids egaux (1/3) entre les 3 composants : valeur par defaut choisie en
  l'absence de ponderation differenciee demandee, non calibree.
- Founder Score "complet" (background/education en plus de serial
  entrepreneur) : hors perimetre pour l'instant, pas de formule definie.
- Cas scope BOTH traite comme Capgemini (decision du 2026-10-13) : un BOTH
  au stade seed/pre-seed est note un peu moins bien sur Maturity Fit (CG
  3/1 points sur le bloc stade contre 4/2 pour BY). Scope NULL : composant
  Maturity Fit exclu de la moyenne.
"""

from __future__ import annotations

import math
from typing import Optional

from prefect import task
from prefect.tasks import exponential_backoff

from src.config.clients import get_supabase_client
from src.utils.db import fetch_in_batches, upsert_in_batches
from src.utils.logger import get_logger

MIN_SCORE = 1
MAX_SCORE = 5

# Poids egaux par defaut entre les 3 composants disponibles -- pas de
# ponderation differenciee demandee a ce stade (session du 2026-09-14).
# Non utilises explicitement dans le calcul (une moyenne simple sur les
# composants dispo revient au meme avec des poids egaux), gardes ici en
# commentaire pour documenter l'intention et faciliter une ponderation
# differenciee future si besoin.
# EQUITY_WEIGHT = MATURITY_WEIGHT = GROWTH_WEIGHT = 1 / 3

SERIAL_ENTREPRENEUR_BONUS = 0.5  # amelioration (soustraction), jamais de malus


def _effective_scope(scope_manual: Optional[str], scope: Optional[str]) -> Optional[str]:
    """CG/BY effectif pour choisir maturity_fit_cg vs maturity_fit_by --
    meme resolution que l'alias `maturity_fit` DOCUMENTE (pas applique) sur
    la fiche Notion Maturity Fit :
    COALESCE(companies.scope_manual, business_computed_values.scope).
    BOTH -> "CG" (Capgemini par defaut, decision du 2026-10-13, accord de
    Simon obtenu). Retourne None pour NULL / toute valeur non reconnue
    (composant maturity exclu)."""
    effective = scope_manual if scope_manual else scope
    if effective == "BOTH":
        return "CG"
    if effective in ("CG", "BY"):
        return effective
    return None


def _score_global(
    equity_score: Optional[int],
    maturity_component: Optional[int],
    traction_score: Optional[int],
    serial_entrepreneur: Optional[bool],
) -> Optional[int]:
    """Moyenne simple des composants disponibles (poids egaux), bonus
    fondateur serie soustrait ensuite (ameliore le score car 1=meilleur),
    borne a [MIN_SCORE, MAX_SCORE], puis arrondi half-up (cf. docstring en
    tete de fichier). None si aucun composant disponible -- absence de
    signal, pas un mauvais score."""
    components = [
        v for v in (equity_score, maturity_component, traction_score) if v is not None
    ]
    if not components:
        return None

    base = sum(components) / len(components)

    if serial_entrepreneur is True:
        base -= SERIAL_ENTREPRENEUR_BONUS
    # False ou None (info absente ou non concluante, indiscernables dans ce
    # champ -- cf. docstring) : pas d'ajustement, jamais de malus.

    base = max(MIN_SCORE, min(MAX_SCORE, base))
    return math.floor(base + 0.5)  # arrondi half-up, pas l'arrondi bancaire de round()


@task(
    name="compute_global_score",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=4),
)
def compute_global_score(domains: list[str]):
    logger = get_logger()
    client = get_supabase_client()

    logger.info(f"Starting compute_global_score for {len(domains)} domains")

    # Step 1 -- composants + scope + bonus fondateur (business_computed_values)
    bcv_rows = fetch_in_batches(
        client,
        "business_computed_values",
        "domain",
        domains,
        select=(
            "domain, equity_score, maturity_fit_cg, maturity_fit_by, "
            "traction_score, serial_entrepreneur, scope"
        ),
    )
    bcv_by_domain = {r["domain"]: r for r in bcv_rows}
    logger.info(f"Fetched business_computed_values for {len(bcv_by_domain)} domains")

    # Step 2 -- scope_manual (companies), prioritaire sur bcv.scope (meme
    # resolution que l'alias maturity_fit documente sur Notion)
    company_rows = fetch_in_batches(
        client, "companies", "domain", domains, select="domain, scope_manual"
    )
    scope_manual_by_domain = {r["domain"]: r.get("scope_manual") for r in company_rows}
    logger.info(f"Fetched scope_manual for {len(scope_manual_by_domain)} domains")

    # Step 3 -- score chaque domaine
    records = []
    no_component_count = 0
    bonus_applied_count = 0
    scope_excluded_count = 0
    for domain in domains:
        bcv = bcv_by_domain.get(domain, {})
        raw_scope_manual = scope_manual_by_domain.get(domain)
        raw_scope = bcv.get("scope")
        effective_scope = _effective_scope(raw_scope_manual, raw_scope)
        if effective_scope is None and (raw_scope_manual or raw_scope):
            # Un scope existe mais n'est reconnu ni comme CG, BY ni BOTH --
            # composant maturity exclu, cf. docstring.
            scope_excluded_count += 1

        maturity_component = None
        if effective_scope == "CG":
            maturity_component = bcv.get("maturity_fit_cg")
        elif effective_scope == "BY":
            maturity_component = bcv.get("maturity_fit_by")

        serial_entrepreneur = bcv.get("serial_entrepreneur")
        score = _score_global(
            equity_score=bcv.get("equity_score"),
            maturity_component=maturity_component,
            traction_score=bcv.get("traction_score"),
            serial_entrepreneur=serial_entrepreneur,
        )

        if score is None:
            no_component_count += 1
        elif serial_entrepreneur is True:
            bonus_applied_count += 1

        records.append({"domain": domain, "global_fund_score": score})

    logger.info(
        f"{no_component_count} domains with no available component "
        "(global_fund_score=None)"
    )
    logger.info(f"{bonus_applied_count} domains got the serial_entrepreneur bonus")
    logger.info(
        f"{scope_excluded_count} domains had an unrecognized scope "
        "(maturity component excluded; BOTH is treated as CG, NULL scope has no component)"
    )

    # Step 4 -- upsert
    if records:
        upsert_in_batches(
            client,
            "business_computed_values",
            records,
            on_conflict="domain",
            logger=logger,
        )
        logger.info(f"Upserted {len(records)} global_fund_score records")
    else:
        logger.info("No records to upsert")

    logger.info("compute_global_score complete")
