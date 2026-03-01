from prefect import task
from pydantic import BaseModel, Field

from src.config.clients import get_qa_model, get_supabase_client
from src.utils.db import fetch_in_batches, upsert_in_batches
from src.utils.logger import get_logger
from src.utils.qa_model import ModelName, Question


class SerialEntrepreneurAnalysis(BaseModel):
    serial_entrepreneur: bool = Field(
        ...,
        description="Whether any founder has founded a company before the current one",
    )
    reason: str = Field(
        ...,
        description="Concise reason: e.g. 'Yes, X founded Y before Z' or 'No evidence of prior founding'",
    )


SYSTEM_PROMPT = """You are an analyst determining whether any founder of a company is a serial entrepreneur.
A serial entrepreneur is someone who has founded at least one other company BEFORE the current one.

Rules:
- Only use the information provided. Do not hallucinate.
- If the information is unclear or insufficient, default to serial_entrepreneur = false.
- Being an employee, advisor, or investor at another company does NOT count — only founding counts.
"""


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


@task(name="compute_founders_values")
def compute_founders_values(domains: list[str]):
    logger = get_logger()
    client = get_supabase_client()

    logger.info(f"Starting compute_founders_values for {len(domains)} domains")

    # Step 1 – Fetch companies
    company_rows = fetch_in_batches(
        client, "companies", "domain", domains, select="id, domain, name"
    )
    id_to_domain: dict[str, str] = {}
    domain_to_company: dict[str, dict] = {}
    for row in company_rows:
        id_to_domain[row["id"]] = row["domain"]
        domain_to_company[row["domain"]] = row

    logger.info(f"Fetched {len(domain_to_company)} companies")

    # Step 2 – Fetch founders
    company_ids = list(id_to_domain.keys())
    founder_rows = fetch_in_batches(
        client,
        "founders",
        "company_id",
        company_ids,
        select="company_id, name, role, description",
    )
    logger.info(f"Fetched {len(founder_rows)} founders")

    # Step 3 – Group founders by domain
    founders_by_domain: dict[str, list[dict]] = {}
    for f in founder_rows:
        domain = id_to_domain.get(f["company_id"])
        if domain:
            founders_by_domain.setdefault(domain, []).append(f)

    # Step 4 – Build founders_background and QA questions
    backgrounds: dict[str, str] = {}
    questions: list[Question] = []
    question_domains: list[str] = []

    for domain in domains:
        founders = founders_by_domain.get(domain, [])
        if not founders:
            continue

        parts = []
        for f in founders:
            name = f.get("name") or "Unknown"
            role = f.get("role") or "Unknown"
            description = f.get("description")
            if description:
                parts.append(f"{name} [{role}]: {description}")
            else:
                parts.append(f"{name} [{role}]")

        background_text = "\n".join(parts)
        backgrounds[domain] = background_text

        company_name = domain_to_company.get(domain, {}).get("name", domain)
        questions.append(
            Question(
                text_content=background_text,
                question=f"Has any of these founders founded a company before {company_name}?",
                system_prompt=SYSTEM_PROMPT,
                pydantic_model=SerialEntrepreneurAnalysis,
            )
        )
        question_domains.append(domain)

    # Step 5 – Call QA model
    answers = []
    if questions:
        answers = call_qa_by_batches(questions, ModelName.GEMINI_3_FLASH_PREVIEW)

    # Step 6 – Build answer map
    answer_map: dict[str, SerialEntrepreneurAnalysis | None] = {}
    for domain, answer in zip(question_domains, answers):
        answer_map[domain] = answer

    # Step 7 – Build upsert records
    records = []
    for domain in domains:
        background_text = backgrounds.get(domain)
        answer = answer_map.get(domain)
        if answer is None:
            continue
        records.append(
            {
                "domain": domain,
                "founders_background": background_text,
                "serial_entrepreneur": answer.serial_entrepreneur,
            }
        )

    # Step 8 – Upsert
    if records:
        upsert_in_batches(
            client,
            "business_computed_values",
            records,
            on_conflict="domain",
            logger=logger,
        )
        logger.info(f"Upserted {len(records)} founders values records")
    else:
        logger.info("No records to upsert")

    logger.info("compute_founders_values complete")
