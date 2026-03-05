from prefect import flow, task

from src.config.settings import get_settings
from src.tasks.attio_push import attio_push
from src.tasks.compute_one_pager import compute_one_pager
from src.tasks.pull_attio_status import pull_attio_status
from src.utils.logger import get_logger


@task(name="attio-push-loop")
def attio_push_loop(domains: list[str], workspace: str = "cg") -> None:
    logger = get_logger()
    failed = []
    succeeded = []
    logger.info(f"Pushing {len(domains)} domain(s) to [{workspace.upper()}]")

    for domain in domains:
        try:
            compute_one_pager([domain], force=False)  # no-op if fresh (60-day TTL)
        except Exception as e:
            logger.warning(f"[{domain}] one-pager failed (push will proceed): {e}")

        try:
            attio_push(domain, workspace)
            succeeded.append(domain)
        except Exception as e:
            logger.error(f"[{domain}] attio push failed: {e}")
            failed.append(domain)

    if failed:
        logger.warning(f"{len(failed)} domain(s) failed: {failed}")

    return succeeded, failed


@flow(name="attio-push-flow", timeout_seconds=3600)
def attio_push_flow(domains: list[str], workspace: str = "cg") -> None:
    logger = get_logger()
    domains = list(dict.fromkeys(domains))  # deduplicate, preserve order
    succeeded, failed = attio_push_loop(domains, workspace)

    # Step 2: sync DB for successfully pushed domains
    if succeeded:
        batch_size = get_settings().compute_business_metric_batch_size
        batches = [
            succeeded[i : i + batch_size] for i in range(0, len(succeeded), batch_size)
        ]
        logger.info(
            f"Syncing Attio status for {len(succeeded)} domain(s) in {len(batches)} batch(es)"
        )
        for i, batch in enumerate(batches):
            logger.info(f"Sync batch {i + 1}/{len(batches)}")
            pull_attio_status(batch)

    logger.info("Flow complete.")
