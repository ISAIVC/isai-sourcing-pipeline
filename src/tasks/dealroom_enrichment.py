import base64
import json
import time
from datetime import date, timedelta
from uuid import uuid4

import requests
from prefect import task
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config.clients import get_supabase_client
from src.config.settings import get_settings
from src.utils.db import insert_in_batches
from src.utils.logger import get_logger

BULK_ENDPOINT = "https://api.dealroom.co/api/v1/companies/bulk"
DEALROOM_BATCH_SIZE = 50
PAGE_LIMIT = 100
REQUEST_DELAY = 1.0
SLEEPING_TIME = 20
TABLE = "dealroom_enrichment"
FRESHNESS_DAYS = 30
FIELDS = ",".join(
    [
        "type",
        "id",
        "name",
        "path",
        "tagline",
        "about",
        "url",
        "website_url",
        "images(32x32,74x74,100x100)",
        "growth_stage",
        "growth_stage_new",
        "hq_locations",
        "launch_year",
        "company_status",
        "last_updated",
        "last_updated_utc",
        "employees",
        "employees_latest",
        "employees_chart",
        "employee_3_months_growth_unique",
        "employee_3_months_growth_percentile",
        "employee_3_months_growth_relative",
        "employee_3_months_growth_delta",
        "employee_6_months_growth_unique",
        "employee_6_months_growth_percentile",
        "employee_6_months_growth_relative",
        "employee_6_months_growth_delta",
        "employee_12_months_growth_unique",
        "employee_12_months_growth_percentile",
        "employee_12_months_growth_relative",
        "employee_12_months_growth_delta",
        "website_traffic_estimates_chart",
        "website_traffic_3_months_growth_unique",
        "website_traffic_3_months_growth_percentile",
        "website_traffic_3_months_growth_relative",
        "website_traffic_3_months_growth_delta",
        "website_traffic_6_months_growth_unique",
        "website_traffic_6_months_growth_percentile",
        "website_traffic_6_months_growth_relative",
        "website_traffic_6_months_growth_delta",
        "website_traffic_12_months_growth_unique",
        "website_traffic_12_months_growth_percentile",
        "website_traffic_12_months_growth_relative",
        "website_traffic_12_months_growth_delta",
    ]
)


def _auth_header(api_key: str) -> dict:
    token = base64.b64encode(f"{api_key}:".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
    }


def _bulk_post(payload: dict, headers: dict) -> dict:
    response = requests.post(BULK_ENDPOINT, json=payload, headers=headers, timeout=30)
    if not response.ok:
        raise RuntimeError(
            f"Dealroom API error {response.status_code}: {response.text}"
        )
    return response.json()


def _fetch_batch(domains: list[str], headers: dict) -> tuple[list[dict], str]:
    all_items: list[dict] = []
    next_page_id = None

    while True:
        payload: dict = {
            "keyword": domains,
            "keyword_type": "website_domain",
            "keyword_match_type": "exact",
            "fields": FIELDS,
            "limit": PAGE_LIMIT,
        }
        if next_page_id is not None:
            payload["next_page_id"] = next_page_id

        data = _bulk_post(payload, headers)
        items = data.get("items", [])
        all_items.extend(items)

        next_page_id = data.get("next_page_id")
        if not next_page_id:
            break

        time.sleep(REQUEST_DELAY)

    raw_json = json.dumps({"domains": domains, "results": all_items})
    return all_items, raw_json


def _normalize_domain(url: str | None) -> str | None:
    if not url:
        return None
    normalized = url.strip()
    for prefix in ("https://www.", "http://www.", "https://", "http://"):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix) :]
            break
    return normalized.rstrip("/")


def _last_traffic(chart: list | None) -> int | None:
    if chart and isinstance(chart, list):
        return chart[-1].get("value")
    return None


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=16))
def _save_to_storage(client, bucket: str, path: str, content: str) -> None:
    client.storage.from_(bucket).upload(
        path,
        content.encode("utf-8"),
        file_options={"upsert": "true"},
    )


def _build_result_map(items: list[dict]) -> dict[str, dict]:
    result_map: dict[str, dict] = {}
    for item in items:
        normalized = _normalize_domain(item.get("website_url"))
        if normalized:
            result_map[normalized] = item
    return result_map


@task(name="dealroom_enrichment")
def dealroom_enrichment(domains: list[str]):
    logger = get_logger()
    settings = get_settings()
    client = get_supabase_client()
    api_key = settings.dealroom_api_key.get_secret_value()
    bucket = settings.dealroom_bucket_name
    headers = _auth_header(api_key)

    # 1. Deduplicate input
    domains = list(set(domains))
    logger.info(f"dealroom_enrichment: {len(domains)} unique domains received")

    # 2. Filter recently enriched domains (chunked to avoid URL length limits)
    cutoff_date_str = (date.today() - timedelta(days=FRESHNESS_DAYS)).isoformat()
    fresh_domains: set[str] = set()
    for i in range(0, len(domains), 1000):
        chunk = domains[i : i + 1000]
        rows = (
            client.table(TABLE)
            .select("domain")
            .in_("domain", chunk)
            .gte("sourcing_date", cutoff_date_str)
            .execute()
        )
        fresh_domains.update(row["domain"] for row in rows.data)
    domains_to_fetch = [d for d in domains if d not in fresh_domains]

    skipped = len(fresh_domains)
    logger.info(
        f"Skipping {skipped} domains enriched within the last {FRESHNESS_DAYS} days; "
        f"{len(domains_to_fetch)} domains to fetch"
    )

    # 3. Early exit
    if not domains_to_fetch:
        logger.info("All domains are fresh — nothing to fetch.")
        return

    total_processed = 0

    # 4. Chunk into batches of DEALROOM_BATCH_SIZE
    for batch_start in range(0, len(domains_to_fetch), DEALROOM_BATCH_SIZE):
        batch = domains_to_fetch[batch_start : batch_start + DEALROOM_BATCH_SIZE]

        # 5a. Fetch batch from API
        items, raw_json = _fetch_batch(batch, headers)

        # 5b. Persist raw JSON to storage
        storage_path = f"{date.today().isoformat()}/{uuid4()}.json"
        _save_to_storage(client, bucket, storage_path, raw_json)

        # 5c. Build result map
        result_map = _build_result_map(items)

        # 5d. Build one record per domain in the batch
        records = []
        matched_count = 0
        for domain in batch:
            matched = result_map.get(domain)
            if matched:
                matched_count += 1
            record = {
                "domain": domain,
                "storage_path": storage_path,
                "sourcing_date": date.today().isoformat(),
                "headcount": matched.get("employees_latest") if matched else None,
                "headcount_growth_l12m": (
                    matched.get("employee_12_months_growth_relative")
                    if matched
                    else None
                ),
                "web_traffic": (
                    _last_traffic(matched.get("website_traffic_estimates_chart"))
                    if matched
                    else None
                ),
                "web_traffic_growth_l12m": (
                    matched.get("website_traffic_12_months_growth_relative")
                    if matched
                    else None
                ),
            }
            records.append(record)

        # 5e. Upsert records
        insert_in_batches(client, TABLE, records, logger=logger)

        unmatched_count = len(batch) - matched_count
        logger.info(
            f"Batch {batch_start // DEALROOM_BATCH_SIZE + 1}: "
            f"{matched_count} matched, {unmatched_count} unmatched"
        )
        total_processed += len(batch)
        logger.info(f"Sleeping for {SLEEPING_TIME} seconds")
        time.sleep(SLEEPING_TIME)

    logger.info(
        f"dealroom_enrichment complete: {total_processed} processed, {skipped} skipped"
    )


if __name__ == "__main__":
    with open("data/6k.txt", "r") as f:
        domains = f.read().splitlines()

    dealroom_enrichment(domains=domains)
