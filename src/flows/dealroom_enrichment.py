from datetime import date, timedelta

from prefect import flow, task

from src.config.clients import get_supabase_client
from src.tasks import dealroom_enrichment
from src.utils.logger import get_logger

NB_DOMAINS_AUTO_MODE = 50 * 25  # 25 calls with 50 domains each
FRESHNESS_DAYS = 180  # 6 months


@task(name="retrieve_domains_automatically")
def retrieve_domains_automatically(number: int = NB_DOMAINS_AUTO_MODE):
    """Retrieve domains from companies tables that are not in web_scraping_enrichment_table witha succes scrapping within the last 3 months"""
    cutoff_date_str = (date.today() - timedelta(days=FRESHNESS_DAYS)).isoformat()
    client = get_supabase_client()
    result = client.rpc(
        "get_companies_that_should_be_dealroom_enriched",
        {"ref_date": cutoff_date_str, "row_limit": number},
    ).execute()
    return [row["domain"] for row in result.data]


@flow(name="dealroom-enrichment-flow", timeout_seconds=1800)  # 30 minutes
def dealroom_enrichment_flow(
    domains: list[str], auto: bool = False, nb_domains: int = NB_DOMAINS_AUTO_MODE
):
    logger = get_logger()
    if auto:
        logger.info(f"Retrieving {nb_domains} domains automatically")
        domains = retrieve_domains_automatically(nb_domains)
    else:
        domains = list(set(domains))
    logger.info(f"Starting dealroom enrichment for {len(domains)} domains")
    dealroom_enrichment(domains)
    logger.info("Dealroom enrichment completed")
