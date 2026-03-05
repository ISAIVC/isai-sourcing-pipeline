from collections import defaultdict

from prefect import task

from src.config.clients import get_embedding_model, get_supabase_client
from src.utils.db import fetch_in_batches, keep_latest_per_domain, upsert_in_batches
from src.utils.feature_extractor import EmbeddingTaskType
from src.utils.logger import get_logger

EMBED_BATCH_SIZE = 100

# (source_field, embedding_field, task_type)
DIMENSIONS = [
    (
        "solution_and_use_cases",
        "solution_and_use_cases_embedding",
        EmbeddingTaskType.CLASSIFICATION,
    ),
    ("full", "full_embedding", EmbeddingTaskType.RETRIEVAL_DOCUMENT),
]


def _build_text(record: dict, dimension: str) -> str | None:
    """Build the text input for a given dimension, returning None if empty."""
    if dimension == "solution_and_use_cases":
        solution = record.get("detailed_solution")
        use_cases = record.get("use_cases")
        parts = [p for p in (solution, use_cases) if p]
        return "\n".join(parts) if parts else None
    if dimension == "full":
        description = record.get("description")
        solution = record.get("detailed_solution")
        use_cases = record.get("use_cases")
        parts = [p for p in (description, solution, use_cases) if p]
        return "\n".join(parts) if parts else None
    value = record.get(dimension)
    return value if value else None


@task(name="embed_textual_dimensions")
def embed_textual_dimensions(domains: list[str]):
    logger = get_logger()
    client = get_supabase_client()
    embedder = get_embedding_model()

    # 1. Fetch and deduplicate
    records = fetch_in_batches(client, "web_scraping_enrichment", "domain", domains)
    records = keep_latest_per_domain(records)
    records = [r for r in records if r.get("description") is not None]
    logger.info(f"Found {len(records)} records with descriptions to embed")

    if not records:
        return

    # 2. Group texts by task_type (each dimension may have a different task type)
    task_texts: dict[EmbeddingTaskType, list[str]] = defaultdict(list)
    task_index_map: dict[EmbeddingTaskType, list[tuple[int, str]]] = defaultdict(list)

    for rec_idx, record in enumerate(records):
        for src_field, emb_field, task_type in DIMENSIONS:
            text = _build_text(record, src_field)
            if text:
                task_texts[task_type].append(text)
                task_index_map[task_type].append((rec_idx, emb_field))

    total_texts = sum(len(t) for t in task_texts.values())
    logger.info(f"Embedding {total_texts} texts across {len(records)} records")

    # 3. Embed each task-type group in chunks
    upsert_records: dict[int, dict] = {}
    for task_type, texts in task_texts.items():
        index_map = task_index_map[task_type]
        nb_batches = (len(texts) // EMBED_BATCH_SIZE) + (
            1 if len(texts) % EMBED_BATCH_SIZE != 0 else 0
        )
        for i in range(0, len(texts), EMBED_BATCH_SIZE):
            chunk = texts[i : i + EMBED_BATCH_SIZE]
            vectors = embedder(chunk, task_type=task_type)
            for vec, (rec_idx, emb_field) in zip(
                vectors, index_map[i : i + EMBED_BATCH_SIZE]
            ):
                if rec_idx not in upsert_records:
                    upsert_records[rec_idx] = {"domain": records[rec_idx]["domain"]}
                upsert_records[rec_idx][emb_field] = vec
            logger.info(
                f"[{task_type.value}] Embedded chunk {i // EMBED_BATCH_SIZE + 1}/{nb_batches}"
            )

    rows = list(upsert_records.values())
    logger.info(f"Upserting {len(rows)} embedding records")

    # 5. Upsert
    upsert_in_batches(
        client,
        "company_embeddings",
        rows,
        on_conflict="domain",
        logger=logger,
        batch_size=25,
    )
