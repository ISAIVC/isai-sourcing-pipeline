import io
import math
import re
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from prefect import task

from src.config.clients import get_supabase_client
from src.config.settings import get_settings
from src.utils.logger import get_logger


def load_traxcn_export(supabase_file_path: str) -> bytes:
    client = get_supabase_client()
    file = client.storage.from_(get_settings().traxcn_exports_bucket_name).download(
        supabase_file_path
    )
    if file is None:
        raise FileNotFoundError(f"The file '{supabase_file_path}' was not found.")
    return file


def _find_header_row(raw_df: pd.DataFrame, sheet_name: str) -> int:
    """Detect the header row index of a single sheet read with header=None.

    Tracxn prepends a variable number of metadata rows (title, description,
    'Prepared on ...', copyright, occasional footnotes such as
    'Asterisk (*) denotes calculated estimates.'), and that number is NOT the
    same across sheets: on the July 2026 export the Companies sheet carries one
    extra line, so its header sits at index 6 while Funding and People sit at
    index 5. Each sheet must therefore be probed independently — reusing the
    Companies index shifts the other sheets by one row, their real header gets
    consumed as data and the first data row becomes the header.
    """
    for i, row in raw_df.iterrows():
        if "Domain Name" in row.values:
            return i
    raise ValueError(
        f"Could not find a header row containing 'Domain Name' in sheet '{sheet_name}'"
    )


def load_and_clean_excel(file: bytes):
    """
    Loads an Excel file, filters sheets starting with 'Companies', 'Funding', or 'People',
    auto-detects the header row, and returns them as dataframes.

    Args:
        file (bytes): The Excel file.

    Returns:
        dict: Keys are output names (companies, funding, people), values are dataframes.
    """
    logger = get_logger()
    raw_sheets = pd.read_excel(io.BytesIO(file), sheet_name=None, header=None)
    sheet_prefixes = {
        "Companies": "companies",
        "Funding": "funding",
        "People": "people",
    }
    filtered_sheets = {}

    for sheet_name, raw_df in raw_sheets.items():
        # Check if sheet starts with any of the target prefixes
        matching_prefix = None
        for prefix, output_name in sheet_prefixes.items():
            if sheet_name.startswith(prefix):
                matching_prefix = output_name
                break

        # Skip sheets that don't match
        if matching_prefix is None:
            logger.info(f"Skipping sheet: '{sheet_name}'")
            continue

        # Detect this sheet's own header row, then re-read it with that header
        header_row = _find_header_row(raw_df, sheet_name)
        logger.info(
            f"Processing sheet: '{sheet_name}' (header row at index {header_row})"
        )
        df = pd.read_excel(io.BytesIO(file), sheet_name=sheet_name, header=header_row)
        df = df.dropna(how="all", axis=0)
        df = df.dropna(how="all", axis=1)
        df = df.map(
            lambda x: (
                x.replace("\n", " ").replace("\r", "") if isinstance(x, str) else x
            )
        )
        filtered_sheets[matching_prefix] = df
    return filtered_sheets


def comma2list(value: Any) -> Optional[list[str]]:
    """
    Turns string containing commas into a list.
    Returns None if value is empty/nan.
    """
    if pd.isna(value) or value == "" or value is None:
        return None
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",") if item.strip()]
        return items if items else None
    return None


def parse_amount(value):
    if pd.isna(value) or value == "" or value is None:
        return None
    try:
        value_str = str(value).split(".")[0].replace(",", "")
        return int(value_str)
    except (ValueError, AttributeError):
        return None


def parsedate(value: Any) -> Optional[str]:
    """
    Parse dates in various formats and return YYYY-MM-DD (ISO) format for Postgres.
    Handles:
    - Oct 16, 2019 -> 2019-10-16
    - 2015 -> 2015-01-01
    - Jan 2023 -> 2023-01-01
    - 2019-10-16 -> 2019-10-16
    """
    if pd.isna(value) or value == "" or value is None:
        return None

    value_str = str(value).strip()

    # Handle year only (e.g., "2015")
    if re.match(r"^\d{4}$", value_str):
        return f"{value_str}-01-01"

    # Handle "Month Year" format (e.g., "Jan 2023")
    month_year_match = re.match(r"^([A-Za-z]{3})\s+(\d{4})$", value_str)
    if month_year_match:
        month_str, year = month_year_match.groups()
        try:
            date_obj = datetime.strptime(f"{month_str} {year}", "%b %Y")
            return f"{year}-{date_obj.month:02d}-01"
        except ValueError:
            return None

    # Handle "Month Day, Year" format (e.g., "Oct 16, 2019")
    try:
        date_obj = datetime.strptime(value_str, "%b %d, %Y")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        pass

    # Handle ISO format (e.g., "2019-10-16")
    try:
        date_obj = datetime.strptime(value_str, "%Y-%m-%d")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        pass

    return None


def parse_column_names(columns: list[str], csv_type: str) -> list[str]:
    """
    Parse column names according to rules:
    - To lower
    - Replace spaces by _
    - Remove things between () except for (USD) that becomes 'in_usd'
    - Remove all '
    - For companies: remove ': TRUE' in Special Flags col
    """
    parsed_columns = []

    for col in columns:
        parsed_col = col.lower()
        parsed_col = parsed_col.replace("'", "")
        parsed_col = parsed_col.replace("-", "_")

        if "(usd)" in parsed_col:
            parsed_col = parsed_col.replace("(usd)", "in_usd")
        parsed_col = re.sub(r"\([^)]*\)", "", parsed_col)

        if csv_type == "companies" and ": true" in parsed_col:
            parsed_col = parsed_col.replace(": true", "")

        parsed_col = parsed_col.replace(" ", "_")
        parsed_col = re.sub(r"_+", "_", parsed_col)
        parsed_col = parsed_col.strip("_")

        parsed_columns.append(parsed_col)

    return parsed_columns


def parse_people(df: pd.DataFrame) -> pd.DataFrame:
    """Parse people dataframe according to specified rules."""

    df.columns = parse_column_names(df.columns.tolist(), "people")
    cols_to_keep = [
        "founder_name",
        "title",
        "company_name",
        "domain_name",
        "people_location",
        "profile_links",
        "emails",
        "description",
        "photo_url",
    ]
    cols_to_keep = [col for col in cols_to_keep if col in df.columns]
    df = df[cols_to_keep]
    df = df.dropna(subset=["founder_name", "title", "domain_name"])
    df = df.dropna(subset=["company_name"])
    df = df.drop_duplicates(subset=["founder_name", "title", "domain_name"])

    if "emails" in df.columns:
        df["emails"] = df["emails"].apply(
            lambda x: (
                [e.strip() for e in str(x).split()] if pd.notna(x) and x != "" else None
            )
        )

    return df


def parse_companies(df: pd.DataFrame) -> pd.DataFrame:
    """Parse companies dataframe according to specified rules."""
    df.columns = parse_column_names(df.columns.tolist(), "companies")

    cols_to_keep = [
        "company_name",
        "domain_name",
        "overview",
        "founded_year",
        "country",
        "state",
        "city",
        "description",
        "sector",
        "business_models",
        "team_background",
        "waves",
        "trending_themes",
        "special_flags",
        "company_stage",
        "all_associated_legal_entities",
        "is_funded",
        "total_funding_in_usd",
        "latest_funded_amount_in_usd",
        "latest_funded_date",
        "latest_valuation_in_usd",
        "institutional_investors",
        "angel_investors",
        "annual_revenue_in_usd",
        "annual_net_profit_in_usd",
        "annual_ebitda_in_usd",
        "key_people_info",
        "key_people_email_ids",
        "links_to_key_people_profiles",
        "total_employee_count",
        "acquisition_list",
        "is_acquired",
        "is_ipo",
        "editors_rating",
        "editors_rated_date",
        "tracxn_score",
        "company_emails",
        "company_phone_numbers",
        "website",
        "website_status",
        "website_status_last_updated",
        "linkedin",
        "twitter",
        "facebook",
        "blog_url",
        "tracxn_url",
        "date_added",
        "is_deadpooled",
        "part_of",
    ]
    cols_to_keep = [col for col in cols_to_keep if col in df.columns]
    df = df[cols_to_keep]

    # drop duplicates on (domain_name)
    df = df.dropna(subset=["domain_name"])
    df = df.drop_duplicates(subset=["domain_name"])

    # drop null in name
    df = df.dropna(subset=["company_name"])

    if "founded_year" in df.columns:
        df["founded_year"] = pd.to_numeric(df["founded_year"], errors="coerce").astype(
            "Int64"
        )

    comma_list_cols = [
        "sector",
        "business_models",
        "waves",
        "trending_themes",
        "special_flags",
        "institutional_investors",
        "angel_investors",
        "key_people_email_ids",
        "links_to_key_people_profiles",
        "acquisition_list",
        "company_emails",
    ]

    for col in comma_list_cols:
        if col in df.columns:
            df[col] = df[col].apply(comma2list)

    bool_cols = ["is_funded", "is_deadpooled", "is_acquired", "is_ipo"]
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: (
                    True
                    if str(x).lower() in ["yes", "true", "1"]
                    else False
                    if str(x).lower() in ["no", "false", "0"]
                    else None
                )
            )

    date_cols = [
        "latest_funded_date",
        "editors_rated_date",
        "website_status_last_updated",
        "date_added",
    ]

    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].apply(parsedate)

    return df


def parse_funding(df: pd.DataFrame) -> pd.DataFrame:
    """Parse funding dataframe according to specified rules."""
    df.columns = parse_column_names(df.columns.tolist(), "funding")

    cols_to_keep = [
        "round_date",
        "company_name",
        "domain_name",
        "round_name",
        "round_amount_in_usd",
        "round_pre_money_valuation_in_usd",
        "round_post_money_valuation_in_usd",
        "round_trailing_12m_revenue_in_usd",
        "institutional_investors",
        "angel_investors",
        "lead_investor",
        "facilitators",
        "total_funding_in_usd",
        "round_revenue_multiple",
        "overview",
        "founded_year",
        "country",
        "state",
        "city",
        "practice_areas",
        "feed_name",
        "business_models",
    ]
    cols_to_keep = [col for col in cols_to_keep if col in df.columns]
    df = df[cols_to_keep]

    # drop null in name
    df = df.dropna(subset=["company_name"])

    if "round_date" in df.columns:
        df["round_date"] = df["round_date"].apply(parsedate)

    for col in [
        "round_amount_in_usd",
        "round_pre_money_valuation_in_usd",
        "round_post_money_valuation_in_usd",
        "round_trailing_12m_revenue_in_usd",
        "total_funding_in_usd",
        "round_revenue_multiple",
    ]:
        if col in df.columns:
            df[col] = df[col].apply(parse_amount)

    if "founded_year" in df.columns:
        df["founded_year"] = pd.to_numeric(df["founded_year"], errors="coerce").astype(
            "Int64"
        )

    comma_list_cols = [
        "institutional_investors",
        "angel_investors",
        "lead_investor",
        "facilitators",
        "practice_areas",
        "feed_name",
        "business_models",
    ]

    for col in comma_list_cols:
        if col in df.columns:
            df[col] = df[col].apply(comma2list)

    # drop duplicates after all parsing/normalization to avoid conflicts from
    # different raw formats resolving to the same values (e.g. date formats)
    df = df.drop_duplicates(subset=["round_date", "domain_name", "round_name"])

    return df


def clean_row(row: dict) -> dict:
    """Clean a row dict for Supabase: convert NaN/NaT/pd.NA to None."""
    cleaned = {}
    for key, value in row.items():
        if isinstance(value, float) and math.isnan(value):
            cleaned[key] = None
        elif isinstance(value, list):
            cleaned[key] = [item for item in value if item is not None]
        elif pd.isna(value):
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned


def upsert_in_batches(
    table_name: str,
    df: pd.DataFrame,
    upsert_on_conflict: str,
    batch_size: int = 1000,
) -> None:
    """Upsert DataFrame records into a Supabase table in batches."""
    logger = get_logger()
    client = get_supabase_client()
    records = [clean_row(row) for row in df.to_dict(orient="records")]
    total = len(records)
    total_batches = math.ceil(total / batch_size)

    logger.info(
        f"  Pushing {total} records to '{table_name}' (batch size: {batch_size})..."
    )

    for i in range(0, total, batch_size):
        batch = records[i : i + batch_size]
        batch_num = i // batch_size + 1

        client.table(table_name).upsert(batch, on_conflict=upsert_on_conflict).execute()

        logger.info(f"    Batch {batch_num}/{total_batches} ({len(batch)} records)")

    logger.info(f"  Done: {total} records pushed to '{table_name}'")


@task(name="ingest_traxcn_export")
def ingest_traxcn_export(supabase_file_path: str) -> None:
    """Parse all TraxCN CSV files and push to Supabase."""
    file = load_traxcn_export(supabase_file_path)
    filtered_sheets = load_and_clean_excel(file)
    companies_df = parse_companies(filtered_sheets["companies"])
    upsert_in_batches(
        "traxcn_companies", companies_df, upsert_on_conflict="domain_name"
    )
    funding_df = parse_funding(filtered_sheets["funding"])
    upsert_in_batches(
        "traxcn_funding_rounds",
        funding_df,
        upsert_on_conflict="round_date,domain_name,round_name",
    )
    people_df = parse_people(filtered_sheets["people"])
    upsert_in_batches(
        "traxcn_founders",
        people_df,
        upsert_on_conflict="founder_name,title,domain_name",
    )
    # Return the company domains
    return companies_df["domain_name"].tolist()
