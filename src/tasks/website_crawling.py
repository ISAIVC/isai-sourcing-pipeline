import uuid
from datetime import datetime

from prefect import task
from prefect.cache_policies import NO_CACHE
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config.clients import get_qa_model, get_supabase_client
from src.config.settings import get_settings
from src.utils.crawler import Crawler
from src.utils.logger import get_logger
from src.utils.qa_model import Question

WEBSITE_BUCKET = get_settings().websites_bucket_name
RETRIEVE_MORE_PAGES_SYSTEM_PROMPT = """
We are checking the website of a startup to understand better the solution they are offering, the use cases they are solving, the clients they have, their tech stack, the key features and their partners.
You are an assistant that must validate if the provided homepage contains enough information to answer the following questions:
- What is the solution they are offering? What are the key features of the solution? Can you create a detailed description of the solution?
- How are their clients? Specific logos?
- What are the use cases they are solving?
- What is their tech stack?
- What are the partners of the startup? Software partners or hardware partners?

Answer in JSON format with the following fields:
- should_retrieve_more_pages: bool (True if you think there are more pages to retrieve in order to asses the previous questions, else False)
- concise_reason: str (A concise reason for why you think there are more pages to retrieve)
- pages_to_retrieve: list[str] (The urls of the pages to retrieve, MAXIMUM 3 urls so chose them wisely if needed)

Example of answer:
should_retrieve_more_pages: True
concise_reason: "The homepage does not contain enough information to answer the previous questions, we need to retrieve more pages"
pages_to_retrieve: ["https://example.com/solution", "https://example.com/success-stories", "https://example.com/partners"]

Example of answer:
should_retrieve_more_pages: False
concise_reason: "The homepage contains enough information to answer the previous questions"
pages_to_retrieve: []

Focus on pages about the solution, the use cases, the clients, the succes stories.
RULE : provide full urls with 'https://' ! All results must be valid urls!
"""


class WebsiteCawlingObject(BaseModel):
    record_id: str
    path: str
    content: str
    additional_pages_to_retrieve: list[str] = Field(default_factory=list)


class RetrieveMorePagesAnswer(BaseModel):
    should_retrieve_more_pages: bool
    concise_reason: str
    # Max 3 pages to retrieve
    pages_to_retrieve: list[str] = Field(..., max_length=3)


def build_asses_more_pages_question(content: str):
    content_parsed = f"## Homepage\n{content}"
    question = Question(
        text_content=content_parsed,
        question="Assess if there are more pages to retrieve for this website?",
        system_prompt=RETRIEVE_MORE_PAGES_SYSTEM_PROMPT,
        pydantic_model=RetrieveMorePagesAnswer,
        temperature=0.2,
    )
    return question


# Implement one retry with tenacity for the upload to the bucket
@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=16))
def save_content_to_bucket(path: str, content: str):
    """Save the content to the websites bucket and overwrite if the file already exists"""
    client = get_supabase_client()
    bytes_content = content.encode("utf-8")
    client.storage.from_(WEBSITE_BUCKET).upload(
        path,
        bytes_content,
        file_options={"upsert": "true"},
    )


def push_first_iteration_to_supabase(domains_dict: dict[str, str], error: bool):
    client = get_supabase_client()
    records = []
    domain_to_record_id_path_map = {}
    for domain, content in domains_dict.items():
        path = f"{domain}/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.md"
        save_content_to_bucket(path, content)
        unique_id = str(uuid.uuid4())
        record = {
            "id": unique_id,
            "domain": domain,
            "storage_path": path,
            "sourcing_date": datetime.now().strftime("%Y-%m-%d"),
        }
        if error:
            record["success"] = False
        domain_to_record_id_path_map[domain] = {"id": unique_id, "path": path}
        records.append(record)
    client.table("web_scraping_enrichment").insert(records).execute()
    # Return map of domain to record_id
    return domain_to_record_id_path_map


# No cache policy and no caching results
@task(
    name="website_crawling",
    cache_policy=NO_CACHE,
    cache_result_in_memory=False,
    timeout_seconds=420,
    retries=2,
    retry_delay_seconds=30,
)
async def website_crawling(domains: list[str]):
    logger = get_logger()
    crawler = Crawler(rate_limit=5, max_retries=3, page_timeout=45000)
    qa_model = get_qa_model()
    qa_model.log_cost(logger)
    logger.info(f"Starting website enrichment for {len(domains)} domains")

    # 1. Crawl the websites
    urls = [f"https://{domain}" for domain in domains]
    crawl_output = await crawler.crawl(urls)
    logger.info(f"Crawled {len(crawl_output.success)} websites")
    if len(crawl_output.errors) > 0:
        logger.error(
            f"There were a total of {len(crawl_output.errors)} errors crawling the websites"
        )

    # 2. Save the website/errors to the websites bucket and create the records in the web_scrapping_enrichment table
    succes_dict = {
        url.replace("https://", ""): content
        for url, content in crawl_output.success.items()
    }
    domain_to_record_id_path_map = push_first_iteration_to_supabase(
        succes_dict, error=False
    )
    if len(crawl_output.errors) > 0:
        errors_dict = {
            url.replace("https://", ""): crawl_error.browser_error
            for url, crawl_error in crawl_output.errors.items()
        }
        push_first_iteration_to_supabase(errors_dict, error=True)
    website_cawlings = {
        domain: WebsiteCawlingObject(
            record_id=domain_to_record_id_path_map[domain]["id"],
            path=domain_to_record_id_path_map[domain]["path"],
            content=content,
        )
        for domain, content in succes_dict.items()
    }
    logger.info(
        "Homepages successfully saved to the websites bucket and records created in the web_scrapping_enrichment table"
    )
    try:
        # 3. First iteration to check if we need to crawl other pages using the qa_model
        logger.info("Using the qa_model to check if we need to crawl other pages...")
        qa_requests = []
        domain_list = []
        for domain, data in website_cawlings.items():
            question = build_asses_more_pages_question(data.content)
            qa_requests.append(question)
            domain_list.append(domain)

        answers = qa_model(qa_requests)
        new_pages_to_crawl = []
        for domain, answer in zip(domain_list, answers):
            if answer is not None and answer.should_retrieve_more_pages:
                website_cawlings[
                    domain
                ].additional_pages_to_retrieve = answer.pages_to_retrieve
                new_pages_to_crawl.extend(answer.pages_to_retrieve)

        # 4. Crawl other pages if needed
        logger.info(f"Crawling {len(new_pages_to_crawl)} new pages...")
        crawl_output = await crawler.crawl(new_pages_to_crawl)
        logger.info(f"Crawled {len(crawl_output.success)} new pages")
        if len(crawl_output.errors) > 0:
            logger.error(
                f"There were a total of {len(crawl_output.errors)} errors crawling the new pages"
            )

        # 5. Save the new pages to the websites bucket
        for domain, data in website_cawlings.items():
            for page in data.additional_pages_to_retrieve:
                if page in crawl_output.success:
                    website_cawlings[
                        domain
                    ].content += f"\n\n## {page}\n{crawl_output.success[page]}"
            save_content_to_bucket(data.path, data.content)

        qa_model.log_cost(logger)
        return website_cawlings
    except Exception as e:
        logger.error(
            f"Error adding additional pages to the website enrichment object: {e}"
        )
        qa_model.log_cost(logger)
        return website_cawlings
