from prefect import flow

from src.tasks import (  # dealroom_enrichment,
    companies_reconciliation,
    founders_reconciliation,
    funding_rounds_reconciliation,
)

from .website_enrichment import website_enrichment_task


@flow(name="enrichment-flow", timeout_seconds=7200)  # 2 hours
def enrichment_flow(domains: list[str], force: bool = False):
    domains = list(set(domains))
    companies_reconciliation(domains)
    parallel_tasks = [
        founders_reconciliation.submit(domains),
        funding_rounds_reconciliation.submit(domains),
        # dealroom_enrichment.submit(domains),
        website_enrichment_task.submit(domains, force),
    ]
    for future in parallel_tasks:
        future.result()
