from datetime import datetime, timedelta, timezone

from prefect import task
from pydantic import BaseModel, Field

from src.config.clients import get_qa_model, get_supabase_client
from src.utils.db import fetch_in_batches, upsert_in_batches
from src.utils.logger import get_logger
from src.utils.qa_model import ModelName, Question


def call_qa_by_batches(
    questions: list[Question], model_name: ModelName, batch_size: int = 50
):
    answers = []
    qa_model = get_qa_model()
    logger = get_logger()
    logger.info(f"Calling QA model for {len(questions)} domains")
    nb_batches = (len(questions) // batch_size) + 1 * (len(questions) % batch_size != 0)
    for i in range(0, len(questions), batch_size):
        logger.info(f"Processing batch {i // batch_size + 1}/{nb_batches}")
        batch = questions[i : i + batch_size]
        answers.extend(qa_model(batch, model_name=model_name))
    qa_model.log_cost(logger)

    return answers


class OnePager(BaseModel):
    overview: str = Field(
        ...,
        description=(
            "High-level summary: what the company does, who it serves, core value proposition, "
            "notable IP. 600–700 characters."
        ),
    )
    management_and_team: str = Field(
        ...,
        description=(
            "Background on founding team and key leadership: names, roles, credentials, "
            "prior experience. 500–600 characters."
        ),
    )
    solution: str = Field(
        ...,
        description=(
            "GTM strategy, target buyer, product-workflow fit, pricing model, "
            "commercial traction. 600–700 characters."
        ),
    )
    market_and_competition: str = Field(
        ...,
        description=(
            "Competitive landscape: key named competitors, differentiation, "
            "market maturity, category leader status. 900–1,100 characters."
        ),
    )


SELECT_FIELDS = """
name, website, hq_country, hq_city, inc_date,
description, detailed_solution, use_cases, clients_served, number_of_clients_identified,
global_2000_clients, cg_key_platforms, by_key_platforms,
vc_current_stage, first_vc_round_date, total_amount_raised,
last_funding_amount, last_funding_date, all_investors,
last_round_lead_investors, total_nber_of_rounds,
business_model, founders_background, serial_entrepreneur,
primary_sector_served_cg, all_industries_served, tech_tags, business_mapping,
gtm_target_cg, gtm_target_by,
headcount, headcount_growth_l12m, web_traffic, web_traffic_growth_l12m
""".strip()

SYSTEM_PROMPT = """You are a Senior Investment Analyst at ISAI, a leading venture capital firm.
Your task: write a concise, structured one-pager for investment committee review
based on the company data provided.

## One-pager sections

### 1. OVERVIEW [600–700 characters]
A high-level summary: what the company does, who it serves, its core value proposition,
and any notable IP or differentiating technology. Open with a category descriptor
(e.g. "AI-powered X platform for Y…"). Investor-analytical tone.

### 2. MANAGEMENT / TEAM [500–600 characters]
Background on the founding team: names, roles, academic credentials, prior company
experience, domain expertise. Be specific — name founders, quantify tenure, call out
PhDs or prior exits.

### 3. SOLUTION [600–700 characters]
GTM motion (top-down / bottom-up), target buyer, how the product fits the workflow,
pricing model (e.g. SaaS, per-seat, per-usage), land-and-expand strategy, and key
named clients or verticals with traction signals.

### 4. MARKET & COMPETITION [900–1,100 characters]
Assessment of the competitive landscape: name key competitors, describe differentiation,
evaluate market maturity and fragmentation, and note whether a category leader has emerged.

## Critical rules
1. Sections 1–3: use ONLY the provided input context. Do not invent facts.
2. Section 4 (Market & Competition) ONLY: you MAY use Google Search to identify
   up-to-date competitors and market data. Ground competitor names with real evidence.
3. Respect character count guidelines. Tight, analytical writing — no marketing fluff.
4. Professional VC-memo tone. Use "The company…" / "The solution enables…" framing.
5. Return only the JSON object — no extra commentary.
6. Write normal text, no markdown or html!
"""


def _fmt(label: str, value) -> str:
    if value is None or value == "" or value == []:
        return ""
    return f"- {label}: {value}"


def build_context(row: dict) -> str:
    lines = []

    # Company basics
    lines.append("## Company Basics")
    for item in [
        _fmt("Name", row.get("name")),
        _fmt("Website", row.get("website")),
        _fmt(
            "HQ",
            f"{row.get('hq_city', '')} {row.get('hq_country', '')}".strip() or None,
        ),
        _fmt("Founded (incorporation date)", row.get("inc_date")),
    ]:
        if item:
            lines.append(item)

    # Description & Solution
    lines.append("\n## Description & Solution")
    for item in [
        _fmt("Description", row.get("description")),
        _fmt("Detailed solution", row.get("detailed_solution")),
        _fmt("Use cases", row.get("use_cases")),
    ]:
        if item:
            lines.append(item)

    # Clients & Traction
    lines.append("\n## Clients & Traction")
    for item in [
        _fmt("Clients served", row.get("clients_served")),
        _fmt("Number of clients identified", row.get("number_of_clients_identified")),
        _fmt("Global 2000 clients", row.get("global_2000_clients")),
        _fmt("Headcount", row.get("headcount")),
        _fmt("Headcount growth (last 12 months)", row.get("headcount_growth_l12m")),
        _fmt("Web traffic", row.get("web_traffic")),
        _fmt("Web traffic growth (last 12 months)", row.get("web_traffic_growth_l12m")),
    ]:
        if item:
            lines.append(item)

    # Partnerships
    lines.append("\n## Partnerships")
    for item in [
        _fmt("Key platforms (Capgemini fund perspective)", row.get("cg_key_platforms")),
        _fmt("Key platforms (Bpifrance fund perspective)", row.get("by_key_platforms")),
    ]:
        if item:
            lines.append(item)

    # Funding
    lines.append("\n## Funding")
    for item in [
        _fmt("Current VC stage", row.get("vc_current_stage")),
        _fmt("First VC round date", row.get("first_vc_round_date")),
        _fmt("Total amount raised", row.get("total_amount_raised")),
        _fmt("Last funding amount", row.get("last_funding_amount")),
        _fmt("Last funding date", row.get("last_funding_date")),
        _fmt("All investors", row.get("all_investors")),
        _fmt("Last round lead investors", row.get("last_round_lead_investors")),
        _fmt("Total number of rounds", row.get("total_nber_of_rounds")),
    ]:
        if item:
            lines.append(item)

    # Team
    lines.append("\n## Team")
    for item in [
        _fmt("Founders background", row.get("founders_background")),
        _fmt("Serial entrepreneur", row.get("serial_entrepreneur")),
    ]:
        if item:
            lines.append(item)

    # Market positioning
    lines.append("\n## Market Positioning")
    for item in [
        _fmt("All industries served", row.get("all_industries_served")),
        _fmt("Business model", row.get("business_model")),
        _fmt("Business mapping", row.get("business_mapping")),
        _fmt("Tech tags", row.get("tech_tags")),
        _fmt("GTM target (Capgemini fund perspective)", row.get("gtm_target_cg")),
        _fmt("GTM target (Bpifrance fund perspective)", row.get("gtm_target_by")),
    ]:
        if item:
            lines.append(item)

    return "\n".join(lines)


@task(name="compute_one_pager")
def compute_one_pager(domains: list[str], force: bool = False):
    logger = get_logger()
    client = get_supabase_client()

    logger.info(
        f"Starting compute_one_pager for {len(domains)} domains | force={force}"
    )

    # Step 1 – Freshness check
    domains_to_process = domains
    if not force:
        existing_rows = fetch_in_batches(
            client, "one_pager", "domain", domains, select="domain, updated_at"
        )
        cutoff = datetime.now(timezone.utc) - timedelta(days=60)
        fresh_domains = set()
        for row in existing_rows:
            updated_at = row.get("updated_at")
            if updated_at:
                if isinstance(updated_at, str):
                    updated_at = datetime.fromisoformat(updated_at)
                if updated_at.tzinfo is None:
                    updated_at = updated_at.replace(tzinfo=timezone.utc)
                if updated_at > cutoff:
                    fresh_domains.add(row["domain"])

        if fresh_domains:
            logger.info(
                f"Skipping {len(fresh_domains)} fresh one-pagers (updated within 60 days)"
            )
        domains_to_process = [d for d in domains if d not in fresh_domains]

    if not domains_to_process:
        logger.info("All one-pagers are fresh, skipping.")
        return {}

    logger.info(f"Processing {len(domains_to_process)} domains")

    # Step 2 – Fetch sourcing_view rows
    sourcing_rows = fetch_in_batches(
        client, "sourcing_view", "website", domains_to_process, select=SELECT_FIELDS
    )
    domain_to_row = {row["website"]: row for row in sourcing_rows}
    logger.info(f"Fetched {len(domain_to_row)} sourcing_view rows")

    # Step 3 – Build questions
    questions: list[Question] = []
    question_domains: list[str] = []

    for domain in domains_to_process:
        row = domain_to_row.get(domain)
        if not row:
            logger.warning(f"No sourcing_view row found for domain: {domain}")
            continue

        context = build_context(row)
        questions.append(
            Question(
                text_content=context,
                question="Generate the one-pager for this company.",
                system_prompt=SYSTEM_PROMPT,
                pydantic_model=OnePager,
                use_grounding=True,
            )
        )
        question_domains.append(domain)

    if not questions:
        logger.info("No questions to process.")
        return {}

    # Step 4 – Call QA model
    answers = call_qa_by_batches(
        questions, ModelName.GEMINI_3_PRO_PREVIEW, batch_size=2
    )

    # Step 5 – Build upsert records
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for domain, answer in zip(question_domains, answers):
        if answer is None:
            logger.warning(f"No answer returned for domain: {domain}")
            continue
        one_pager: OnePager = answer.structured_response
        records.append(
            {
                "domain": domain,
                "overview": one_pager.overview,
                "management_and_team": one_pager.management_and_team,
                "solution": one_pager.solution,
                "market_and_competition": one_pager.market_and_competition,
                "updated_at": now,
            }
        )

    # Step 6 – Upsert
    if records:
        upsert_in_batches(
            client,
            "one_pager",
            records,
            on_conflict="domain",
            logger=logger,
        )
        logger.info(f"Upserted {len(records)} one-pager records")
    else:
        logger.info("No records to upsert")

    logger.info("compute_one_pager complete")
    return {record["domain"]: record for record in records}
