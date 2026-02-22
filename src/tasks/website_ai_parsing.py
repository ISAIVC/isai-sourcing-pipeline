from prefect import task
from pydantic import BaseModel, Field

from src.config.clients import get_qa_model, get_supabase_client
from src.utils.db import upsert_in_batches
from src.utils.logger import get_logger
from src.utils.qa_model import ModelName, Question


class CompanyAnalysis(BaseModel):
    description: str = Field(
        ...,
        description="A general description of the company, as if you are pitching them to an investor",
    )
    detailed_solution: str = Field(
        ...,
        description="A detailed description of the solution they are offering. What is their product?",
    )
    key_features: str = Field(
        ...,
        description="The key features of the solution. What are the main features of the solution?",
    )
    use_cases: str = Field(
        ...,
        description="The use cases they are solving. What problems are they solving? Provide examples if possible.",
    )
    tech_description: str = Field(
        ...,
        description="A detailed description of the tech stack they are using. What technologies are they using? Provide examples if possible.",
    )
    industries_served_description: str = Field(
        ...,
        description="A detailed description of the industries they are serving. What industries are they serving? Provide examples if possible.",
    )
    key_clients: list[str] = Field(
        ...,
        description="The key clients they have. Focus on Global 2000 companies, we are looking for big logos.",
    )
    key_partners: list[str] = Field(
        ...,
        description="The key partners they have. We are looking for all software or hardware partners!",
    )
    nb_of_clients_identified: int = Field(
        ..., description="The number of clients they have."
    )


SYSTEM_PROMPT = """
**Role:**
You are a Senior Investment Analyst at ISAI, a leading entrepreneur-focused venture capital firm. Your expertise lies in deep-dive company analysis for two specific strategic funds:
1. **ISAI Cap Venture:** Focused on B2B software and services that align with Capgemini’s ecosystem (Enterprise AI, Cloud, Digital Transformation, Sustainability).
2. **ISAI Build Venture:** Focused on "ConTech" and "PropTech" in partnership with VINCI (Sustainable construction, smart cities, energy efficiency, infrastructure).

**Objective:**
Your task is to ingest raw markdown data crawled from a company’s website and transform it into a high-quality, structured investment memo. You must filter out marketing "fluff" and extract hard facts, product capabilities, and evidence of market traction.

**Input Context:**
You will be provided with a collection of markdown strings representing different pages of a company's website (Home, Product, Solutions, Case Studies, About Us, etc.).

**Analysis Guidelines:**
1. **Investor Tone:** Write in a professional, objective, and analytical tone. Avoid using superlative marketing language (e.g., "world-changing," "incredible"). Use "The company provides..." or "The solution enables..."
2. **Global 2000 Focus:** For `key_clients` and `key_partners`, prioritize "Big Logos." Look for Fortune 500 or Global 2000 entities. If a client is a small local shop, omit it in favor of larger strategic names.
3. **Tech Inference:** If the tech stack isn't explicitly listed, look for clues in "Integrations," "Security/Compliance" pages (e.g., AWS, Kubernetes, SOC2, Python APIs), or "Careers" pages.
4. **No Hallucinations:** If information for a specific field (like `nb_of_clients_identified`) is not present in the text, return `0` or a conservative estimate based on listed logos. Do not invent client names.
5. **Use Case Specificity:** Do not just say "improves efficiency." Say "Reduces concrete drying time by 20% through IoT sensor monitoring."

**Output Format:**
Return only a valid JSON object following the `CompanyAnalysis` schema provided.

### Field-Specific Instructions:

*   **description:** Synthesize a 3-4 sentence "Elevator Pitch." Start with the category (e.g., "A SaaS platform for...") and end with the primary value proposition.
*   **detailed_solution:** Explain the "How." If it's a software, describe the workflow. If it's hardware, describe the physical component.
*   **tech_description:** Identify the core architecture. Mention if it’s On-prem/Cloud, API-first, Mobile-native, or uses specific AI models/LLMs.
*   **industries_served_description:** Be specific. Instead of "Construction," use "Heavy Infrastructure and Civil Engineering."
*   **key_clients / key_partners:** Extract only proper nouns. Ensure they are recognizable corporate entities.
*   **nb_of_clients_identified:** Count the unique logos or client names mentioned across all provided pages.
"""


class WebsiteEnrichmentQAInput(BaseModel):
    company_id: str
    domain: str
    content: str


def build_website_ai_parsing_question(content: str):
    logger = get_logger()
    if len(content.split()) > 25_000:
        logger.warning(
            f"Content is too long for the website AI parsing. Cropping to 25_000 words. Original length: {len(content.split())}"
        )
        content = " ".join(content.split()[:25_000])
    question = Question(
        text_content=content,
        question="Please generate the CompanyAnalysis JSON for this company",
        system_prompt=SYSTEM_PROMPT,
        pydantic_model=CompanyAnalysis,
    )
    return question


def update_database_with_website_ai_parsing_results(
    company_ids: list[str], domains: list[str], answers: list[CompanyAnalysis]
):
    client = get_supabase_client()
    logger = get_logger()
    records = []
    for company_id, domain, answer in zip(company_ids, domains, answers):
        if answer is not None:
            record = {
                "id": company_id,
                "domain": domain,
                "description": answer.description,
                "detailed_solution": answer.detailed_solution,
                "tech_description": answer.tech_description,
                "key_features": answer.key_features,
                "use_cases": answer.use_cases,
                "industries_served_description": answer.industries_served_description,
                "key_clients": answer.key_clients,
                "key_partners": answer.key_partners,
                "nb_of_clients_identified": answer.nb_of_clients_identified,
                "success": True,
            }
        else:
            record = {
                "id": company_id,
                "domain": domain,
                "success": False,
            }
        records.append(record)

    upsert_in_batches(
        client, "web_scraping_enrichment", records, on_conflict="id", logger=logger
    )


@task(name="website_ai_parsing")
def website_ai_parsing(list_of_inputs: list[WebsiteEnrichmentQAInput]):
    logger = get_logger()
    logger.info(f"Starting website parsing for {len(list_of_inputs)} companies")
    qa_requests = []
    company_ids = []
    domains = []
    for input in list_of_inputs:
        question = build_website_ai_parsing_question(input.content)
        qa_requests.append(question)
        company_ids.append(input.company_id)
        domains.append(input.domain)
    qa_model = get_qa_model()
    qa_model.log_cost(logger)
    answers = qa_model(qa_requests, model_name=ModelName.GEMINI_3_FLASH_PREVIEW)
    logger.info(f"Website AI parsing completed for {len(answers)} companies")
    update_database_with_website_ai_parsing_results(company_ids, domains, answers)
    logger.info(f"Database updated with {len(company_ids)} companies")
    qa_model.log_cost(logger)
