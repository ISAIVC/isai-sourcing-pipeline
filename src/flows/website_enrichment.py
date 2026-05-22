from datetime import date, timedelta

from prefect import flow, task

from src.config.clients import get_supabase_client
from src.config.settings import get_settings
from src.tasks import WebsiteEnrichmentQAInput, website_ai_parsing, website_crawling
from src.utils.logger import get_logger

FRESHNESS_DAYS = 90
NB_DOMAINS_AUTO_MODE = 200


def _filter_stale_domains(domains: list[str]) -> list[str]:
    """Return only domains that should be scraped.

    Rules:
    - Skip if success=True within the last FRESHNESS_DAYS (already fresh).
    - Skip permanently if the domain has never succeeded (no success=True ever)
      but has at least one failure (success=False). These are sites blocked to
      scrapers — retrying burns Gemini credits with no chance of success.
    - Retry everything else: domains that succeeded before (but are now stale),
      and domains with only null records (infra crash, not a site block).
    """
    cutoff_date_str = (date.today() - timedelta(days=FRESHNESS_DAYS)).isoformat()
    fresh_domains: set[str] = set()
    permanently_blocked_domains: set[str] = set()
    client = get_supabase_client()

    for i in range(0, len(domains), 1000):
        chunk = domains[i : i + 1000]

        # Rule 1: recently successful → fresh, skip
        rows = (
            client.table("web_scraping_enrichment")
            .select("domain")
            .in_("domain", chunk)
            .gte("sourcing_date", cutoff_date_str)
            .eq("success", True)
            .execute()
        )
        fresh_domains.update(row["domain"] for row in rows.data)

        # Rule 2: permanently blocked — has failures but never succeeded
        ever_succeeded = {
            row["domain"]
            for row in client.table("web_scraping_enrichment")
            .select("domain")
            .in_("domain", chunk)
            .eq("success", True)
            .execute()
            .data
        }
        ever_failed = {
            row["domain"]
            for row in client.table("web_scraping_enrichment")
            .select("domain")
            .in_("domain", chunk)
            .eq("success", False)
            .execute()
            .data
        }
        permanently_blocked_domains.update(ever_failed - ever_succeeded)

    skip_domains = fresh_domains | permanently_blocked_domains
    stale = [d for d in domains if d not in skip_domains]
    logger = get_logger()
    logger.info(
        f"Skipping {len(fresh_domains)} fresh domains and "
        f"{len(permanently_blocked_domains)} permanently blocked domains; "
        f"{len(stale)} domains to process"
    )
    return stale


@task(name="website_enrichment_task")
async def website_enrichment_task(domains: list[str], force: bool = False):
    settings = get_settings()
    domains = list(set(domains))
    logger = get_logger()

    if not force:
        domains = _filter_stale_domains(domains)
        if not domains:
            logger.info("All domains are fresh — nothing to scrape.")
            return

    logger.info(f"Starting website enrichment for {len(domains)} domains")
    for i in range(0, len(domains), settings.website_enrichment_batch_size):
        logger.info(
            f"Processing batch {i // settings.website_enrichment_batch_size + 1}/{len(domains) // settings.website_enrichment_batch_size}"
        )
        batch = domains[i : i + settings.website_enrichment_batch_size]
        results = await website_crawling(batch)
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
async def website_enrichment_flow(
    domains: list[str],
    auto: bool = False,
    nb_domains: int = NB_DOMAINS_AUTO_MODE,
    force: bool = False,
):
    logger = get_logger()
    settings = get_settings()
    if auto:
        logger.info(f"Retrieving {nb_domains} domains automatically")
        domains = retrieve_domains_automatically(nb_domains)
    else:
        domains = list(set(domains))

    if not force:
        domains = _filter_stale_domains(domains)
        if not domains:
            logger.info("All domains are fresh — nothing to scrape.")
            return

    logger.info(f"Starting website enrichment for {len(domains)} domains")
    for i in range(0, len(domains), settings.website_enrichment_batch_size):
        logger.info(
            f"Processing batch {i // settings.website_enrichment_batch_size + 1}/{len(domains) // settings.website_enrichment_batch_size}"
        )
        batch = domains[i : i + settings.website_enrichment_batch_size]
        results = await website_crawling(batch)
        inputs = [
            WebsiteEnrichmentQAInput(
                company_id=data.record_id, domain=domain, content=data.content
            )
            for domain, data in results.items()
        ]
        website_ai_parsing(inputs)
    logger.info("Website enrichment completed")
