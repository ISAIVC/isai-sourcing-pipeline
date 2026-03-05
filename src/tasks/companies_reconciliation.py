"""
Companies reconciliation task.

Takes a list of unique domains and reconciles data from crunchbase_companies
and traxcn_companies into a single companies table in Supabase.
"""

import json
import math
from pathlib import Path

import pandas as pd
from prefect import task
from prefect.tasks import exponential_backoff

from src.config.clients import get_supabase_client
from src.utils.db import fetch_as_dataframe, sanitize, upsert_in_batches
from src.utils.logger import get_logger

COUNTRY_CODES_PATH = (
    Path(__file__).resolve().parent.parent.parent / "assets" / "country_codes.json"
)

TARGET_COLUMNS = [
    "logo",
    "name",
    "domain",
    "hq_country",
    "hq_city",
    "inc_date",
    "description",
    "all_tags",
    "vc_current_stage",
    "total_amount_raised",
    "last_funding_amount",
    "last_funding_date",
    "all_investors",
    "source",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_country_codes() -> dict[str, str]:
    with open(COUNTRY_CODES_PATH) as f:
        return json.load(f)


def _val(value):
    """Return None if value is NaN/None/empty, else return the value."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


def _traxcn_or_cb_or_none(row, tx_col: str, cb_col: str):
    """Common pattern: take tracxn value if available, else crunchbase, else None."""
    tx = _val(row.get(tx_col))
    if tx is not None:
        return tx
    return _val(row.get(cb_col))


def _array_to_list(val) -> list[str]:
    """Convert a postgres TEXT[] (comes back as list) or comma-string to a flat list."""
    if val is None:
        return []
    if isinstance(val, float) and math.isnan(val):
        return []
    if isinstance(val, list):
        return [str(v).strip() for v in val if v is not None and str(v).strip()]
    if isinstance(val, str) and val.strip():
        return [v.strip() for v in val.split(",") if v.strip()]
    return []


# ---------------------------------------------------------------------------
# Reconciliation functions (one per target column)
# ---------------------------------------------------------------------------


def reconciliation_logo(row) -> str | None:
    return _val(row.get("cb_logo_url"))


def reconciliation_name(row) -> str | None:
    return _traxcn_or_cb_or_none(row, "tx_company_name", "cb_name")


def reconciliation_hq_country(row, country_codes: dict[str, str]) -> list[str] | None:
    countries: list[str] = []

    tx_country = _val(row.get("tx_country"))
    if tx_country is not None:
        countries.extend(c.strip() for c in tx_country.split(",") if c.strip())

    cb_code = _val(row.get("cb_country_code"))
    if cb_code is not None:
        cb_country = country_codes.get(cb_code)
        if cb_country:
            countries.append(cb_country)

    unique = list(dict.fromkeys(countries))  # deduplicate, preserve order
    return unique if unique else None


def reconciliation_hq_city(row) -> str | None:
    return _traxcn_or_cb_or_none(row, "tx_city", "cb_city")


def reconciliation_inc_date(row) -> int | None:
    tx_year = _val(row.get("tx_founded_year"))
    if tx_year is not None:
        return int(tx_year)
    cb_date = _val(row.get("cb_founded_on"))
    if cb_date is not None:
        try:
            return int(str(cb_date)[:4])
        except (ValueError, TypeError):
            return None
    return None


def reconciliation_description(row) -> str | None:
    return _traxcn_or_cb_or_none(row, "tx_description", "cb_short_description")


def reconciliation_vc_current_stage(row) -> str | None:
    return _val(row.get("tx_company_stage"))


def reconciliation_total_amount_raised(row) -> float | None:
    tx_val = _val(row.get("tx_total_funding_in_usd"))
    cb_val = _val(row.get("cb_total_funding_usd"))
    tx_num = float(tx_val) if tx_val is not None else None
    cb_num = float(cb_val) if cb_val is not None else None
    if tx_num is not None and cb_num is not None:
        return max(tx_num, cb_num)
    return tx_num if tx_num is not None else cb_num


def reconciliation_last_funding_amount(row) -> float | None:
    val = _val(row.get("tx_latest_funded_amount_in_usd"))
    if val is not None:
        return float(val)
    return None


def reconciliation_last_funding_date(row) -> str | None:
    """Return the latest funding date in YYYY-MM-DD format as it should be in the traxcn_companies table"""
    return _val(row.get("tx_latest_funded_date"))


def reconciliation_all_investors(row) -> list[str]:
    return _val(row.get("tx_institutional_investors"))


def reconciliation_source(row) -> str:
    has_cb = _val(row.get("cb_name")) is not None
    has_tx = _val(row.get("tx_company_name")) is not None
    if has_cb and has_tx:
        return "both"
    if has_cb:
        return "crunchbase"
    return "traxcn"


def reconciliation_all_tags(row) -> list[str]:
    tags: list[str] = []
    for col in [
        "tx_sector",
        "tx_business_models",
        "tx_waves",
        "tx_trending_themes",
        "tx_special_flags",
    ]:
        tags.extend(_array_to_list(row.get(col)))
    for col in ["cb_category_list", "cb_category_groups_list"]:
        tags.extend(_array_to_list(row.get(col)))
    return tags


# ---------------------------------------------------------------------------
# Main task
# ---------------------------------------------------------------------------


@task(
    name="companies_reconciliation",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=4),
)
def companies_reconciliation(domains: list[str]):
    logger = get_logger()
    client = get_supabase_client()
    country_codes = _load_country_codes()

    logger.info(f"Starting companies reconciliation for {len(domains)} domains")

    # Step 1 – pull crunchbase data
    cb_df = fetch_as_dataframe(client, "crunchbase_companies", "domain", domains)
    logger.info(f"Fetched {len(cb_df)} rows from crunchbase_companies")

    # Step 2 – pull tracxn data
    tx_df = fetch_as_dataframe(client, "traxcn_companies", "domain_name", domains)
    logger.info(f"Fetched {len(tx_df)} rows from traxcn_companies")

    # Step 3 – log overlap stats
    cb_domains = set(cb_df["domain"]) if not cb_df.empty else set()
    tx_domains = set(tx_df["domain_name"]) if not tx_df.empty else set()
    overlap = cb_domains & tx_domains
    only_cb = cb_domains - tx_domains
    only_tx = tx_domains - cb_domains
    logger.info(
        f"Overlap: {len(overlap)} | Only in CB: {len(only_cb)} | "
        f"Only in Tracxn: {len(only_tx)}"
    )

    # Step 4 – merge with prefixes to avoid column name conflicts
    if not cb_df.empty:
        cb_df = cb_df.add_prefix("cb_")
        cb_df = cb_df.rename(columns={"cb_domain": "domain"})
    else:
        cb_df = pd.DataFrame(columns=["domain"])

    if not tx_df.empty:
        tx_df = tx_df.add_prefix("tx_")
        tx_df = tx_df.rename(columns={"tx_domain_name": "domain"})
    else:
        tx_df = pd.DataFrame(columns=["domain"])

    merged = pd.merge(cb_df, tx_df, on="domain", how="outer")
    logger.info(f"Merged dataframe: {len(merged)} rows")

    # Steps 5 & 6 – apply reconciliation functions
    merged["logo"] = merged.apply(reconciliation_logo, axis=1)
    merged["name"] = merged.apply(reconciliation_name, axis=1)
    merged["hq_country"] = merged.apply(
        lambda row: reconciliation_hq_country(row, country_codes), axis=1
    )
    merged["hq_city"] = merged.apply(reconciliation_hq_city, axis=1)
    merged["inc_date"] = merged.apply(reconciliation_inc_date, axis=1)
    merged["description"] = merged.apply(reconciliation_description, axis=1)
    merged["vc_current_stage"] = merged.apply(reconciliation_vc_current_stage, axis=1)
    merged["total_amount_raised"] = merged.apply(
        reconciliation_total_amount_raised, axis=1
    )
    merged["last_funding_amount"] = merged.apply(
        reconciliation_last_funding_amount, axis=1
    )
    merged["last_funding_date"] = merged.apply(reconciliation_last_funding_date, axis=1)
    merged["all_investors"] = merged.apply(reconciliation_all_investors, axis=1)
    merged["source"] = merged.apply(reconciliation_source, axis=1)
    merged["all_tags"] = merged.apply(reconciliation_all_tags, axis=1)

    # Step 7 – keep only the target columns
    result = merged[TARGET_COLUMNS].copy()

    # Step 8 – push to supabase
    records = result.to_dict(orient="records")

    cleaned: list[dict] = []
    for rec in records:
        clean_rec = {k: sanitize(v) for k, v in rec.items()}
        if clean_rec.get("inc_date") is not None:
            clean_rec["inc_date"] = int(clean_rec["inc_date"])
        cleaned.append(clean_rec)

    logger.info(f"Pushing {len(cleaned)} records to companies table")
    upsert_in_batches(client, "companies", cleaned, on_conflict="domain", logger=logger)

    logger.info("Companies reconciliation complete")
