from datetime import date, timedelta

from prefect import flow, task

from src.config.clients import get_supabase_client
from src.config.settings import get_settings
from src.tasks import WebsiteEnrichmentQAInput, website_ai_parsing, website_crawling
from src.utils.logger import get_logger

FRESHNESS_DAYS = 90
NB_DOMAINS_AUTO_MODE = 200


@task(name="website_enrichment_task")
def website_enrichment_task(domains: list[str]):
    settings = get_settings()
    domains = list(set(domains))
    logger = get_logger()
    logger.info(f"Starting website enrichment for {len(domains)} domains")
    for i in range(0, len(domains), settings.website_enrichment_batch_size):
        logger.info(
            f"Processing batch {i // settings.website_enrichment_batch_size + 1}/{len(domains) // settings.website_enrichment_batch_size}"
        )
        batch = domains[i : i + settings.website_enrichment_batch_size]
        results = website_crawling(batch)
        inputs = [
            WebsiteEnrichmentQAInput(
                company_id=data.record_id, domain=domain, content=data.content
            )
            for domain, data in results.items()
        ]
        website_ai_parsing(inputs)
    logger.info("Website enrichment completed")


@task(name="retrieve_domains_automatically")
def retrieve_domains_automatically(number: int = NB_DOMAINS_AUTO_MODE):
    """Retrieve domains from companies tables that are not in web_scraping_enrichment_table witha succes scrapping within the last 3 months"""
    cutoff_date_str = (date.today() - timedelta(days=FRESHNESS_DAYS)).isoformat()
    client = get_supabase_client()
    result = client.rpc(
        "get_companies_that_should_be_scraped",
        {"ref_date": cutoff_date_str, "row_limit": number},
    ).execute()
    return [row["domain"] for row in result.data]


@flow(name="website-enrichment-flow", timeout_seconds=5400)  # 1.5 hours
def website_enrichment_flow(
    domains: list[str], auto: bool = False, nb_domains: int = NB_DOMAINS_AUTO_MODE
):
    logger = get_logger()
    settings = get_settings()
    if auto:
        logger.info(f"Retrieving {nb_domains} domains automatically")
        domains = retrieve_domains_automatically(nb_domains)
    else:
        domains = list(set(domains))
    logger.info(f"Starting website enrichment for {len(domains)} domains")
    for i in range(0, len(domains), settings.website_enrichment_batch_size):
        logger.info(
            f"Processing batch {i // settings.website_enrichment_batch_size + 1}/{len(domains) // settings.website_enrichment_batch_size}"
        )
        batch = domains[i : i + settings.website_enrichment_batch_size]
        results = website_crawling(batch)
        inputs = [
            WebsiteEnrichmentQAInput(
                company_id=data.record_id, domain=domain, content=data.content
            )
            for domain, data in results.items()
        ]
        website_ai_parsing(inputs)
    logger.info("Website enrichment completed")
