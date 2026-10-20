import random
from datetime import date, timedelta
from typing import Optional

from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner
from pydantic import BaseModel, Field

from src.config.clients import get_supabase_client
from src.config.settings import get_settings
from src.tasks import (
    annotate_company_tags,
    compute_equity_score,
    compute_founders_values,
    compute_funding_metrics,
    compute_global_score,
    compute_growth_score,
    compute_maturity,
    compute_scores,
    embed_textual_dimensions,
    fuzzy_matching_metrics,
    pull_attio_status,
)
from src.utils.logger import get_logger

FRESHNESS_DAYS = 180  # 6 months
NB_DOMAINS_AUTO_MODE = 200

TASK_REPRESENTATIVE_COLUMNS: dict[str, str] = {
    "sync_attio_status": "in_attio",
    "compute_fuzzy_matching_metrics": "competitors_cg",
    "compute_funding_metrics": "vc_current_stage",
    "compute_founders_values": "founders_background",
    "annotate_company_tags": "scope",
    "compute_scores": "solution_fit_cg",
    # Volontairement ABSENTS ici : compute_equity_score (equity_score),
    # compute_maturity (maturity_fit_cg), compute_growth_score (traction_score)
    # et compute_global_score (global_fund_score). Ces colonnes peuvent rester
    # NULL legitimement (absence de signal) et sont vides au premier
    # deploiement : les ajouter ici ferait considerer quasiment toutes les
    # societes comme "incompletes" en mode auto, donc retraiter en boucle des
    # domaines deja traites (appels Gemini inclus) et retarder les nouveaux.
    # Les 4 scores sont calcules sur les domaines selectionnes pour les autres
    # raisons, et remplis sur l'existant par un run manuel recompute_all
    # (config limitee aux 4 scores).
}


def retrieve_all_domains_in_business_computed_values():
    client = get_supabase_client()
    page_size = 1000
    offset = 0
    domains = []
    while True:
        rows = (
            client.table("business_computed_values")
            .select("domain")
            .range(offset, offset + page_size - 1)
            .execute()
            .data
        )
        domains.extend(row["domain"] for row in rows)
        if len(rows) < page_size:
            break
        offset += page_size
    return domains


def _get_enabled_representative_columns(config: "BusinessProcessingConfig") -> list[str]:
    return [
        col
        for task_flag, col in TASK_REPRESENTATIVE_COLUMNS.items()
        if getattr(config, task_flag, False)
    ]


def _get_scraped_domains(client) -> set[str]:
    page_size = 1000
    offset = 0
    domains: set[str] = set()
    while True:
        rows = (
            client.table("web_scraping_enrichment")
            .select("domain")
            .eq("success", True)
            .not_.is_("description", "null")
            .range(offset, offset + page_size - 1)
            .execute()
            .data
        )
        domains.update(row["domain"] for row in rows)
        if len(rows) < page_size:
            break
        offset += page_size
    return domains


def _get_fresh_complete_domains(
    client, columns: list[str], cutoff_date_str: str
) -> set[str]:
    page_size = 1000
    offset = 0
    domains: set[str] = set()
    while True:
        query = (
            client.table("business_computed_values")
            .select("domain")
            .gte("updated_at", cutoff_date_str)
        )
        for col in columns:
            query = query.not_.is_(col, "null")
        rows = query.range(offset, offset + page_size - 1).execute().data
        domains.update(row["domain"] for row in rows)
        if len(rows) < page_size:
            break
        offset += page_size
    return domains



class BusinessProcessingConfig(BaseModel):
    sync_attio_status: bool = Field(default=True)
    compute_fuzzy_matching_metrics: bool = Field(default=True)
    compute_funding_metrics: bool = Field(default=True)
    compute_founders_values: bool = Field(default=True)
    annotate_company_tags: bool = Field(default=True)
    embed_textual_dimensions: bool = Field(default=True)
    compute_scores: bool = Field(default=True)
    compute_equity_score: bool = Field(default=True)
    compute_maturity: bool = Field(default=True)
    compute_growth_score: bool = Field(default=True)
    compute_global_score: bool = Field(default=True)


@task(name="retrieve_business_domains_automatically")
def retrieve_domains_automatically(
    nb_domains: int = NB_DOMAINS_AUTO_MODE,
    config: BusinessProcessingConfig = BusinessProcessingConfig(),
) -> list[str]:
    logger = get_logger()
    columns = _get_enabled_representative_columns(config)
    if not columns:
        logger.warning("No tasks enabled in config — no columns to check")
        return []

    cutoff_date_str = (date.today() - timedelta(days=FRESHNESS_DAYS)).isoformat()
    client = get_supabase_client()

    scraped = _get_scraped_domains(client)
    logger.info(f"Found {len(scraped)} successfully scraped domains")

    fresh_complete = _get_fresh_complete_domains(client, columns, cutoff_date_str)
    logger.info(f"Found {len(fresh_complete)} fresh and complete domains to exclude")

    candidates = list(scraped - fresh_complete)
    logger.info(f"Found {len(candidates)} candidate domains (new, incomplete, or stale)")

    random.shuffle(candidates)
    result = candidates[:nb_domains]
    logger.info(f"Auto mode selected {len(result)} domains")
    return result


@task(name="embed_and_compute_scores")
def embed_and_compute_scores(
    domains: list[str], embeddings_enabled: bool = True, scores_enabled: bool = True
):
    if embeddings_enabled:
        embed_textual_dimensions(domains)
    if scores_enabled:
        compute_scores(domains)


@flow(
    name="business-processing-flow",
    task_runner=ThreadPoolTaskRunner(max_workers=8),
    timeout_seconds=14400,
)  # 4 hours
def business_processing_flow(
    domains: list[str] = None,
    config: Optional[BusinessProcessingConfig] = BusinessProcessingConfig(),
    recompute_all: bool = False,
    auto: bool = False,
    nb_domains: int = NB_DOMAINS_AUTO_MODE,
):
    logger = get_logger()
    if auto:
        logger.info(f"Auto mode: retrieving up to {nb_domains} domains")
        domains = retrieve_domains_automatically(nb_domains, config)
        logger.info(f"Auto mode retrieved {len(domains)} domains")
    elif recompute_all:
        domains = retrieve_all_domains_in_business_computed_values()
    elif not domains:
        raise ValueError("Provide domains, set auto=True, or set recompute_all=True")
    domains = list(set(domains))
    logger.info(f"Processing {len(domains)} domains")
    settings = get_settings()
    batches = [
        domains[i : i + settings.compute_business_metric_batch_size]
        for i in range(0, len(domains), settings.compute_business_metric_batch_size)
    ]
    logger.info(f"Processing {len(batches)} batches")
    for i, batch in enumerate(batches):
        logger.info(f"Processing batch {i + 1}/{len(batches)}")
        parallel_tasks = []
        if config.sync_attio_status:
            parallel_tasks.append(pull_attio_status.submit(batch))
        fuzzy_matching_future = None
        if config.compute_fuzzy_matching_metrics:
            fuzzy_matching_future = fuzzy_matching_metrics.submit(batch)
            parallel_tasks.append(fuzzy_matching_future)
        funding_metrics_future = None
        if config.compute_funding_metrics:
            funding_metrics_future = compute_funding_metrics.submit(batch)
            parallel_tasks.append(funding_metrics_future)
        founders_future = None
        if config.compute_founders_values:
            founders_future = compute_founders_values.submit(batch)
            parallel_tasks.append(founders_future)
        annotate_future = None
        if config.annotate_company_tags:
            annotate_future = annotate_company_tags.submit(batch)
            parallel_tasks.append(annotate_future)
        if any([config.embed_textual_dimensions, config.compute_scores]):
            parallel_tasks.append(
                embed_and_compute_scores.submit(
                    batch, config.embed_textual_dimensions, config.compute_scores
                )
            )
        equity_future = None
        if config.compute_equity_score:
            equity_future = compute_equity_score.submit(batch)
            parallel_tasks.append(equity_future)
        maturity_future = None
        if config.compute_maturity:
            # compute_maturity lit business_computed_values.vc_current_stage,
            # ecrit par compute_funding_metrics, et business_computed_values.scope,
            # ecrit par annotate_company_tags (pour renseigner la colonne
            # unique maturity_fit) -- il faut attendre ces resultats avant
            # de soumettre compute_maturity dans le meme batch pour ne pas
            # lire une valeur perimee (cf. docstring de compute_maturity.py).
            if funding_metrics_future is not None:
                funding_metrics_future.result()
            if annotate_future is not None:
                annotate_future.result()
            maturity_future = compute_maturity.submit(batch)
            parallel_tasks.append(maturity_future)
        growth_future = None
        if config.compute_growth_score:
            # compute_growth_score lit business_computed_values.global_2000_clients
            # (Bloc A), ecrit par fuzzy_matching_metrics -- meme risque de
            # valeur perimee que compute_maturity/compute_funding_metrics
            # ci-dessus si les deux tournent en parallele dans le meme
            # batch : on attend explicitement le resultat avant de
            # soumettre compute_growth_score.
            if fuzzy_matching_future is not None:
                fuzzy_matching_future.result()
            growth_future = compute_growth_score.submit(batch)
            parallel_tasks.append(growth_future)
        if config.compute_global_score:
            # compute_global_score lit business_computed_values.equity_score /
            # maturity_fit_cg / maturity_fit_by / traction_score / scope /
            # serial_entrepreneur, ecrits respectivement par
            # compute_equity_score / compute_maturity / compute_growth_score /
            # annotate_company_tags / compute_founders_values -- meme risque
            # de valeur perimee que les dependances ci-dessus si ces taches
            # tournent en parallele dans le meme batch : on attend
            # explicitement chaque resultat avant de soumettre
            # compute_global_score (cf. docstring de compute_global_score.py).
            for dependency_future in (
                equity_future,
                maturity_future,
                growth_future,
                founders_future,
                annotate_future,
            ):
                if dependency_future is not None:
                    dependency_future.result()
            parallel_tasks.append(compute_global_score.submit(batch))
        for future in parallel_tasks:
            future.result()
