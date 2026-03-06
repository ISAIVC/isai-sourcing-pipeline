import requests
from prefect import task

from src.config.clients import get_supabase_client
from src.config.settings import get_settings
from src.utils.logger import get_logger

ATTIO_BASE = "https://api.attio.com/v2"

_BASE_FIELD_MAP = [
    ("detailed_solution", "detailed_solution", "text"),
    ("tech_tags", "tags", "multiselect"),
    ("business_model", "bm", "select"),
    ("headcount", "headcount", "number"),
    ("business_mapping", "business_mapping", "select"),
    ("clients_served", "clients", "multiselect"),
    ("tech_tags", "tags_process_metiers", "multiselect"),
    ("vc_current_stage", "vc_current_stage", "select"),
    ("first_vc_round_date", "first_vc_round_date", "date"),
    ("first_vc_round_amount", "first_vc_round_amount", "number"),
    ("total_amount_raised", "total_amount_raised", "number"),
    ("last_funding_amount", "last_funding_amount", "number"),
    ("last_funding_date", "last_funding_date", "date"),
    ("all_investors", "all_investors", "multiselect"),
    ("headcount_growth_l12m", "headcount_growth_l12m", "number"),
    ("all_industries_served", "all_industries_served", "multiselect"),
    ("overview", "one_pager_overview", "text"),
    ("management_and_team", "one_pager_management_and_team", "text"),
    ("solution", "one_pager_solution", "text"),
    ("market_and_competition", "one_pager_market_and_competition", "text"),
]

WORKSPACE_CONFIG = {
    "cg": {
        "token_attr": "attio_cg_token",
        "extra_fields": [
            ("solution_fit_cg", "scoring_isai", "number"),
            ("primary_sector_served_cg", "sector", "select"),
        ],
        "dealflow_values": {
            "status": [{"status": "To watch"}],
            "dd_stage_1": [{"status": "I-CG Discovery"}],
            "channel": [{"option": "Data Driven Sourcing"}],
        },
    },
    "by": {
        "token_attr": "attio_by_token",
        "extra_fields": [
            ("solution_fit_by", "scoring_isai", "number"),
            ("primary_sector_served_by", "sector", "select"),
        ],
        "dealflow_values": {
            "status_3": [{"status": "To watch"}],
            "dd_stage": [{"option": "New"}],
            "channel_6": [{"option": "Data Driven Sourcing"}],
        },
    },
}

ATTIO_ATTR_TYPE_MAP = {
    "text": {"type": "text", "is_multiselect": False},
    "number": {"type": "number", "is_multiselect": False},
    "select": {"type": "select", "is_multiselect": False},
    "multiselect": {"type": "select", "is_multiselect": True},
    "date": {"type": "date", "is_multiselect": False},
}


def _fmt_text(v):
    return [{"value": v}]


def _fmt_number(v):
    return [{"value": float(v)}]


def _fmt_select(v):
    return [{"option": v}]


def _fmt_status(v):
    return [{"status": v}]


def _fmt_date(v):
    return [{"value": str(v)}]


def _fmt_multiselect(v):
    return [{"option": item} for item in v]


FORMATTERS = {
    "text": _fmt_text,
    "number": _fmt_number,
    "select": _fmt_select,
    "status": _fmt_status,
    "date": _fmt_date,
    "multiselect": _fmt_multiselect,
}


def _ensure_attributes(headers: dict, field_map: list, logger) -> None:
    logger.info("Ensuring company attributes exist in Attio...")

    resp = requests.get(f"{ATTIO_BASE}/objects/companies/attributes", headers=headers)
    resp.raise_for_status()
    existing_slugs = {a["api_slug"] for a in resp.json().get("data", [])}

    needed = {slug: fmt_type for _, slug, fmt_type in field_map}

    for slug, fmt_type in needed.items():
        if slug in existing_slugs:
            logger.info(f"[exists]  {slug}")
            continue

        attr_cfg = ATTIO_ATTR_TYPE_MAP[fmt_type]
        title = slug.replace("_", " ").title()
        payload = {
            "data": {
                "title": title,
                "description": "",
                "api_slug": slug,
                "type": attr_cfg["type"],
                "is_multiselect": attr_cfg["is_multiselect"],
                "is_required": False,
                "is_unique": False,
                "config": {},
            }
        }
        cr = requests.post(
            f"{ATTIO_BASE}/objects/companies/attributes", headers=headers, json=payload
        )
        if cr.ok:
            logger.info(
                f"[created] {slug}  "
                f"(type={attr_cfg['type']}, multiselect={attr_cfg['is_multiselect']})"
            )
        elif cr.status_code == 409:
            logger.info(f"[exists]  {slug}  (slug conflict — already created)")
        else:
            logger.error(f"[error]   {slug}: {cr.status_code} {cr.text}")


def _ensure_select_options(
    headers: dict, values: dict, field_map: list, logger
) -> None:
    slug_to_type = {slug: fmt_type for _, slug, fmt_type in field_map}

    for slug, formatted_value in values.items():
        fmt_type = slug_to_type.get(slug)
        if fmt_type not in ("select", "multiselect"):
            continue

        for item in formatted_value:
            option_title = item.get("option")
            if not option_title or not isinstance(option_title, str):
                continue

            url = f"{ATTIO_BASE}/objects/companies/attributes/{slug}/options"
            resp = requests.post(
                url, headers=headers, json={"data": {"title": option_title}}
            )
            if resp.ok:
                logger.info(f"[option created] {slug}: '{option_title}'")
            elif resp.status_code == 409:
                pass  # already exists — fine
            else:
                logger.warning(
                    f"[warn] option '{option_title}' for '{slug}': "
                    f"{resp.status_code} {resp.text}"
                )


def _fetch_one_pager_row(domain: str, logger) -> dict:
    client = get_supabase_client()
    result = (
        client.from_("one_pager")
        .select("overview, management_and_team, solution, market_and_competition")
        .eq("domain", domain)
        .maybe_single()
        .execute()
    )
    if result is None or not result.data:
        logger.warning(f"No one_pager record for domain={domain}")
        return {}
    return result.data


def _fetch_sourcing_row(domain: str, logger) -> dict:
    logger.info(f"Fetching sourcing_mv record for domain: {domain}")
    client = get_supabase_client()
    result = (
        client.from_("sourcing_mv")
        .select("*")
        .eq("website", domain)
        .single()
        .execute()
    )
    if not result.data:
        raise ValueError(f"No record found in sourcing_mv for website='{domain}'")
    logger.info(f"Found record: {result.data.get('name', '(no name)')}")
    return result.data


def _build_company_values(row: dict, domain: str, field_map: list, logger) -> dict:
    values = {"domains": [{"domain": domain}]}

    for col, slug, fmt_type in field_map:
        raw = row.get(col)
        if raw is None:
            continue
        if isinstance(raw, list) and len(raw) == 0:
            continue
        if isinstance(raw, str) and raw.strip() == "":
            continue

        formatter = FORMATTERS[fmt_type]
        try:
            values[slug] = formatter(raw)
        except Exception as e:
            logger.warning(f"Could not format field '{col}' -> '{slug}': {e}")

    return values


def _assert_company_record(headers: dict, values: dict, logger) -> str:
    logger.info("Upserting company record (matching_attribute=domains)...")
    url = f"{ATTIO_BASE}/objects/companies/records?matching_attribute=domains"
    payload = {"data": {"values": values}}
    resp = requests.put(url, headers=headers, json=payload)
    if not resp.ok:
        logger.error(f"ERROR {resp.status_code}: {resp.text}")
    resp.raise_for_status()
    data = resp.json().get("data", {})
    record_id = data.get("id", {}).get("record_id")
    if not record_id:
        raise ValueError(f"No record_id in response: {resp.text}")
    logger.info(f"record_id: {record_id}  (HTTP {resp.status_code})")
    return record_id


def _find_dealflow_entry(headers: dict, domain: str, logger) -> str | None:
    logger.info(f"Checking for existing dealflow entry for domain: {domain}")
    url = f"{ATTIO_BASE}/lists/dealflow/entries/query"
    payload = {
        "filter": {
            "$or": [
                {
                    "path": [
                        ["dealflow", "parent_record"],
                        ["companies", "domains"],
                    ],
                    "constraints": {"root_domain": domain},
                }
            ]
        }
    }
    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    entries = resp.json().get("data", [])
    if entries:
        entry_id = entries[0]["id"]["entry_id"]
        logger.info(f"Found existing entry_id: {entry_id}")
        return entry_id
    logger.info("No existing entry found.")
    return None


def _create_dealflow_entry(
    headers: dict, record_id: str, dealflow_values: dict, logger
) -> str:
    logger.info("Creating new dealflow entry...")
    url = f"{ATTIO_BASE}/lists/dealflow/entries"
    payload = {
        "data": {
            "parent_record_id": record_id,
            "parent_object": "companies",
            "entry_values": dealflow_values,
        }
    }
    resp = requests.post(url, headers=headers, json=payload)
    if not resp.ok:
        logger.error(f"ERROR {resp.status_code}: {resp.text}")
    resp.raise_for_status()
    entry_id = resp.json().get("data", {}).get("id", {}).get("entry_id")
    logger.info(f"Created entry_id: {entry_id}  (HTTP {resp.status_code})")
    return entry_id


@task(name="attio_push")
def attio_push(domain: str, workspace: str = "cg") -> None:
    logger = get_logger()
    if workspace not in WORKSPACE_CONFIG:
        raise ValueError(f"Unknown workspace '{workspace}'. Choose 'cg' or 'by'.")

    config = WORKSPACE_CONFIG[workspace]
    field_map = _BASE_FIELD_MAP + config["extra_fields"]
    token = getattr(get_settings(), config["token_attr"]).get_secret_value()
    dealflow_values = config["dealflow_values"]
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    logger.info(f"[{workspace.upper()}] Pushing: {domain}")
    _ensure_attributes(headers, field_map, logger)
    row = _fetch_sourcing_row(domain, logger)
    one_pager = _fetch_one_pager_row(domain, logger)
    row.update(one_pager)
    values = _build_company_values(row, domain, field_map, logger)
    _ensure_select_options(headers, values, field_map, logger)
    record_id = _assert_company_record(headers, values, logger)
    entry_id = _find_dealflow_entry(headers, domain, logger)
    if entry_id is None:
        _create_dealflow_entry(headers, record_id, dealflow_values, logger)
    else:
        logger.info(f"Dealflow entry already exists ({entry_id}), skipping.")
    logger.info(f"[{workspace.upper()}] Done: {domain}")
