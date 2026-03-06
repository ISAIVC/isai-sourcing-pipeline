"""
This task annotates the companies with AI for columns  "gtm_target" "gtm_target_by" "business_model" "all_industries_served_sorted" "tech_tags_dynamic"
"""

from typing import Literal, Optional

from prefect import task
from pydantic import BaseModel, Field, create_model

from src.config.clients import get_qa_model, get_supabase_client
from src.utils.db import fetch_in_batches, keep_latest_per_domain, upsert_in_batches
from src.utils.logger import get_logger
from src.utils.qa_model import ModelName, Question

SYSTEM_PROMPT = """
**Role:**
You are a Senior Investment Analyst at ISAI, a leading entrepreneur-focused venture capital firm. Your expertise lies in deep-dive company analysis for two specific strategic funds:
1. **ISAI Cap Venture:** Focused on B2B software and services that align with Capgemini’s ecosystem (Enterprise AI, Cloud, Digital Transformation, Sustainability).
2. **ISAI Build Venture:** Focused on "ConTech" and "PropTech" in partnership with VINCI (Sustainable construction, smart cities, energy efficiency, infrastructure).

**Objective:**
Your task is to define tags for a company based on the provided information

**Input Context:**
You will be provided with an detailed description of a company.

**Analysis Guidelines:**
1. **Industries served:** The most important part of this analysis are the industries served by the provided company. Inspire from the uses cases and the solution description to identify this industries
2. **The GTM Target:** The ambition is to identify the nature of the targetted clients. We want a first generic labellisation (gtm_target) but also a more refined labelisation for Build Venture (gtm_target_by). It's possible that this refined labellisation is not relevant, in that case leave it empty.
3. **Business Model:** Identify the business model of the company among the provided list
4. **Business Map:** Associate a business from the list if relevant, else set to None
5. **Technology tags:** For the tech tags there is no specific list, you are free to put tags but keep it concise, accurate and relevant for VC sourcing!
6. **No Hallucinations:** Do not hallucinate any information, only use the information provided in the input context

**Output Format:**
Return only a valid JSON object following the `CompanyTags` schema provided.

### Fields and Tags Descriptions:
"""


class CompanyTags(BaseModel):
    sorted_industries_served: list[Literal["fake_industry", "fake_industry_2"]] = Field(
        ...,
        description="All the industries served by the company among the list. They must be sorted by the most relevant to the least relevant",
        max_length=4,
        min_length=1,
    )
    small_explanation_of_industries_sorting: str = Field(
        ...,
        description="A small explanation of why you sorted the industries the way you did",
    )
    gtm_target: Literal["fake_gtm_target", "fake_gtm_target_2"] = Field(
        ..., description="The go to market target (the type of clients) of the company"
    )
    # Optional pydantic field
    gtm_target_by: Optional[Literal["fake_gtm_target", "fake_gtm_target_2"]] = Field(
        None,
        description="The go to market target (the type of clients) of the company but among a really specific client typology, if not relevant set to None",
    )
    business_model: Literal["fake_bm", "fake_bm_2"] = Field(
        ..., description="The business model of the company"
    )
    business_map: Optional[Literal["fake_business", "fake_business_2"]] = Field(
        None,
        description="Associate a business from the list if relevant, else set to None",
    )
    tech_tags: list[str] = Field(..., description="Technology tags for this company")


def build_model_from_schema(schema: dict, model_name: str = None):
    model_name = model_name or schema.get("title", "DynamicModel")
    properties = schema.get("properties", {})
    required_fields = set(schema.get("required", []))
    fields = {}

    for field_name, field_schema in properties.items():
        field_type, field_default = _resolve_field(
            field_schema, field_name, field_name in required_fields
        )
        description = field_schema.get("description", "")
        fields[field_name] = (
            field_type,
            Field(default=field_default, description=description),
        )

    return create_model(model_name, **fields)


def _resolve_field(field_schema: dict, field_name: str, is_required: bool):
    # Handle anyOf (Optional types)
    if "anyOf" in field_schema:
        types = field_schema["anyOf"]
        non_null = [t for t in types if t.get("type") != "null"]
        has_null = any(t.get("type") == "null" for t in types)

        if non_null:
            inner_type, _ = _resolve_field(non_null[0], field_name, True)
            if has_null:
                final_type = Optional[inner_type]
                default = field_schema.get("default", None)
                return final_type, default
            return inner_type, ... if is_required else None

    # Handle enum at top level (overrides anyOf enum if present)
    top_enum = field_schema.get("enum")

    # Handle string with enum -> Literal
    if field_schema.get("type") == "string" and (top_enum or field_schema.get("enum")):
        enum_values = top_enum or field_schema.get("enum")
        literal_type = Literal[tuple(enum_values)]
        return literal_type, ... if is_required else None

    # Handle array
    if field_schema.get("type") == "array":
        items = field_schema.get("items", {})
        item_type, _ = _resolve_field(items, field_name, True)
        return list[item_type], ... if is_required else None

    # Handle basic types
    type_map = {
        "string": str,
        "integer": int,
        "number": float,
        "boolean": bool,
        "object": dict,
    }

    base_type = type_map.get(field_schema.get("type"), str)

    # String with enum
    if base_type is str and top_enum:
        return Literal[tuple(top_enum)], ... if is_required else None

    return base_type, ... if is_required else None


def build_response_model_dynamically() -> tuple[BaseModel, dict, dict, str]:
    client = get_supabase_client()

    # Industries parsing
    industries_data = client.table("industries").select("*").execute().data
    industry_tags_description = "### Industries:\n"
    for industry in industries_data:
        industry_tags_description += (
            f"{industry['industry']}: {industry['description']}\n"
        )
    industry_tags_description = industry_tags_description.strip()
    industry_to_scope_mapping = {r["industry"]: r["scope"] for r in industries_data}
    industry_to_sector_mapping = {r["industry"]: r["sector"] for r in industries_data}

    # GTM parsing
    gtm_data = (
        client.table("gtm_target").select("*").in_("scope", ["ALL"]).execute().data
    )
    gtm_tags_description = "### GTM Target:\n"
    for gtm in gtm_data:
        gtm_tags_description += f"{gtm['target']}: {gtm['description']}\n"
    gtm_tags_description = gtm_tags_description.strip()

    # GTM BY parsing
    gtm_by_data = (
        client.table("gtm_target").select("*").in_("scope", ["BY"]).execute().data
    )
    gtm_by_tags_description = "### GTM Target (Build Venture specific):\n"
    for gtm_by in gtm_by_data:
        gtm_by_tags_description += f"{gtm_by['target']}: {gtm_by['description']}\n"
    gtm_by_tags_description = gtm_by_tags_description.strip()

    # Business model parsing
    business_model_data = client.table("business_models").select("*").execute().data
    business_model_tags_description = "### Business Models:\n"
    for business_model in business_model_data:
        business_model_tags_description += (
            f"{business_model['name']}: {business_model['description']}\n"
        )
    business_model_tags_description = business_model_tags_description.strip()

    # Business map parsing
    business_map_data = client.table("business_mapping").select("*").execute().data
    business_map_tags_description = "### Business Maps:\n"
    for business_map in business_map_data:
        business_map_tags_description += (
            f"{business_map['name']}: {business_map['description']}\n"
        )
    business_map_tags_description = business_map_tags_description.strip()

    final_description = " ".join(
        [
            industry_tags_description,
            gtm_tags_description,
            gtm_by_tags_description,
            business_model_tags_description,
            business_map_tags_description,
        ]
    )

    pydantic_model = CompanyTags.model_json_schema()
    pydantic_model["properties"]["sorted_industries_served"]["items"]["enum"] = [
        industry["industry"] for industry in industries_data
    ]
    pydantic_model["properties"]["gtm_target"]["enum"] = [
        gtm["target"] for gtm in gtm_data
    ]
    pydantic_model["properties"]["gtm_target_by"]["anyOf"][0]["enum"] = [
        gtm_by["target"] for gtm_by in gtm_by_data
    ]
    pydantic_model["properties"]["business_model"]["enum"] = [
        business_model["name"] for business_model in business_model_data
    ]
    pydantic_model["properties"]["business_map"]["anyOf"][0]["enum"] = [
        business_map["name"] for business_map in business_map_data
    ]

    return (
        build_model_from_schema(pydantic_model, "CompanyTagsResponse"),
        industry_to_scope_mapping,
        industry_to_sector_mapping,
        final_description,
    )


def build_company_description_from_web_enrichment(record: dict) -> str:
    description = ""
    for dim, content in [
        ("Description", record["description"]),
        ("Detailed Solution", record["detailed_solution"]),
        ("Key Features", record["key_features"]),
        ("Use Cases", record["use_cases"]),
        ("Tech Description", record["tech_description"]),
        ("Industries Served", record["industries_served_description"]),
        ("Key Clients", record["key_clients"]),
        ("Key Partners", record["key_partners"]),
    ]:
        if content is not None:
            description += f"###{dim}\n{str(content)}\n\n"
    return description


def retrieve_companies_web_enrichement(domains: list[str]) -> list[dict]:
    records = fetch_in_batches(
        get_supabase_client(), "web_scraping_enrichment", "domain", domains
    )
    records = keep_latest_per_domain(records)
    # Drop all the record with empty description
    records = [record for record in records if record["description"] is not None]
    return records


def deduced_industry_tags(
    listed_industries: list[str],
    industry_to_scope_mapping: dict,
    industry_to_sector_mapping: dict,
) -> dict:
    """
    Function that determines the scope and the sectors of the company based on the industries served
    scope : if only BY then BY, if only CG then CG, else BOTH
    primary_sector_served_cg : take the first index where CG and get the sector, if no CG then None
    primary_industry_served_cg : take the first index where CG and get the industry, if no CG then None
    primary_sector_served_by : take the first index where BY and get the sector, if no BY then None
    primary_industry_served_by : take the first index where BY and get the industry, if no BY then None
    """
    scopes = [industry_to_scope_mapping[industry] for industry in listed_industries[:2]]

    unique_scopes = set(scopes)
    if unique_scopes == {"BY"}:
        scope = "BY"
    elif unique_scopes == {"CG"}:
        scope = "CG"
    else:
        scope = "BOTH"

    first_cg_industry = None
    first_by_industry = None

    for industry in listed_industries:
        if industry_to_scope_mapping[industry] == "CG" and first_cg_industry is None:
            first_cg_industry = industry
        if industry_to_scope_mapping[industry] == "BY" and first_by_industry is None:
            first_by_industry = industry
        if first_cg_industry and first_by_industry:
            break

    all_cg_industries = [
        industry
        for industry in listed_industries
        if industry_to_scope_mapping[industry] == "CG"
    ]
    all_by_industries = [
        industry
        for industry in listed_industries
        if industry_to_scope_mapping[industry] == "BY"
    ]

    all_cg_sectors = [
        industry_to_sector_mapping[industry] for industry in all_cg_industries
    ]
    all_by_sectors = [
        industry_to_sector_mapping[industry] for industry in all_by_industries
    ]

    unique_cg_sectors = set(all_cg_sectors)
    unique_by_sectors = set(all_by_sectors)

    if len(unique_cg_sectors) == 1:
        primary_sector_served_cg = list(unique_cg_sectors)[0]
    else:
        primary_sector_served_cg = "cross_sector"
    if len(unique_by_sectors) == 1:
        primary_sector_served_by = list(unique_by_sectors)[0]
    else:
        primary_sector_served_by = "cross_sector"

    return {
        "scope": scope,
        "primary_sector_served_cg": primary_sector_served_cg,
        "primary_industry_served_cg": first_cg_industry,
        "primary_sector_served_by": primary_sector_served_by,
        "primary_industry_served_by": first_by_industry,
    }


def build_upsert_record(
    source_records: list[dict],
    answers: list[CompanyTags],
    industry_to_scope_mapping: dict,
    industry_to_sector_mapping: dict,
) -> list[dict]:
    upsert_records = []
    for source_record, answer in zip(source_records, answers):
        if answer is None:
            continue
        industry_tags = deduced_industry_tags(
            answer.sorted_industries_served,
            industry_to_scope_mapping,
            industry_to_sector_mapping,
        )
        upsert_records.append(
            {
                "domain": source_record["domain"],
                "all_industries_served_sorted": answer.sorted_industries_served,
                "gtm_target": answer.gtm_target,
                "gtm_target_by": answer.gtm_target_by,
                "business_model": answer.business_model,
                "tech_tags_dynamic": answer.tech_tags,
                "scope": industry_tags["scope"],
                "primary_sector_served_cg": industry_tags["primary_sector_served_cg"],
                "primary_industry_served_cg": industry_tags[
                    "primary_industry_served_cg"
                ],
                "primary_sector_served_by": industry_tags["primary_sector_served_by"],
                "primary_industry_served_by": industry_tags[
                    "primary_industry_served_by"
                ],
                "business_mapping": answer.business_map,
            }
        )

    return upsert_records


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


@task(name="annotate_company_tags")
def annotate_company_tags(domains: list[str]):
    logger = get_logger()
    logger.info(f"Starting annotate company tags for {len(domains)} domains")
    (
        pydantic_model,
        industry_to_scope_mapping,
        industry_to_sector_mapping,
        final_description,
    ) = build_response_model_dynamically()
    records = retrieve_companies_web_enrichement(domains)
    logger.info(f"Retrieved {len(records)} records from web enrichment")
    system_prompt = SYSTEM_PROMPT + final_description
    requests = []
    for record in records:
        description = build_company_description_from_web_enrichment(record)

        question = Question(
            text_content=description,
            pydantic_model=pydantic_model,
            system_prompt=system_prompt,
            question="Please provide the tags for this company",
        )
        requests.append(question)
    logger.info("Calling model for tagging")
    responses = call_qa_by_batches(requests, ModelName.GEMINI_3_FLASH_PREVIEW)
    logger.info(f"Annotated {len(responses)} companies")
    records_to_upsert = build_upsert_record(
        records, responses, industry_to_scope_mapping, industry_to_sector_mapping
    )

    upsert_in_batches(
        get_supabase_client(),
        "business_computed_values",
        records_to_upsert,
        on_conflict="domain",
        logger=logger,
    )
