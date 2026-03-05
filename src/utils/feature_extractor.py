"""
Generic text embedding processor using Google Gemini embedding models.

This module provides a reusable EmbeddingModel class that converts text strings
into normalized embedding vectors. It has no project-specific logic and can be
used for any text embedding task.
"""

from enum import Enum
from typing import List

import numpy as np
from google import genai
from google.genai import types
from google.genai.errors import ClientError, ServerError
from google.oauth2.service_account import Credentials
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from src.utils.logger import get_logger


class EmbeddingTaskType(str, Enum):
    RETRIEVAL_QUERY = "RETRIEVAL_QUERY"
    RETRIEVAL_DOCUMENT = "RETRIEVAL_DOCUMENT"
    SEMANTIC_SIMILARITY = "SEMANTIC_SIMILARITY"
    CLASSIFICATION = "CLASSIFICATION"
    CLUSTERING = "CLUSTERING"


class EmbeddingModel:
    """
    Generic text embedding model using Google Gemini.

    This class provides a simple interface to convert text strings into
    normalized embedding vectors. It supports batch processing and handles
    authentication, retries, and normalization automatically.

    Example:
        >>> embedder = EmbeddingModel()
        >>> texts = ["Hello world", "How are you?"]
        >>> embeddings = embedder(texts)
        >>> len(embeddings)
        2
        >>> len(embeddings[0])
        768
    """

    def __init__(
        self,
        credentials: dict,
        project: str,
        model_name: str = "gemini-embedding-001",
        output_dimensionality: int = 768,
        task_type: EmbeddingTaskType = EmbeddingTaskType.SEMANTIC_SIMILARITY,
    ):
        """
        Initialize the embedding model.

        Args:
            model_name: Gemini embedding model to use (default: "gemini-embedding-001")
            api_key: Google GenAI API key (defaults to GOOGLE_GENAI_API_KEY env var)
            credentials: Service account credentials dict for Vertex AI
            project: GCP project ID for Vertex AI
            output_dimensionality: Dimension of output embeddings (default: 768)
            task_type: Task type for optimizing embeddings (default: "SEMANTIC_SIMILARITY")
                      Options: "RETRIEVAL_QUERY", "RETRIEVAL_DOCUMENT",
                              "SEMANTIC_SIMILARITY", "CLASSIFICATION", "CLUSTERING"
        """
        self.model_name = model_name
        self.output_dimensionality = output_dimensionality
        self.task_type = task_type

        self.logger = get_logger()
        self.logger.info(f"Initializing EmbeddingModel with {model_name}")
        parsed_credentials = Credentials.from_service_account_info(
            credentials, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self.client = genai.Client(
            credentials=parsed_credentials,
            project=project,
        )
        self.logger.info(f"Initialized EmbeddingModel with {model_name}")

    @staticmethod
    def _is_retryable_error(exception: Exception) -> bool:
        """Check if an exception is retryable (rate limits, server errors)"""
        if isinstance(exception, ServerError):
            return exception.code in {408, 429, 500, 503, 504}
        if isinstance(exception, ClientError):
            return exception.code in {408, 429, 500, 503, 504}
        if isinstance(exception, (ConnectionError, TimeoutError, OSError)):
            return True
        return False

    @retry(
        retry=retry_if_exception(_is_retryable_error.__func__),
        wait=wait_exponential(multiplier=2, min=4, max=60),
        stop=stop_after_attempt(6),
        reraise=True,
    )
    def _embed_with_retry(
        self, texts: List[str], task_type: EmbeddingTaskType
    ) -> types.EmbedContentResponse:
        """
        Call the embedding API with retry logic.

        Args:
            texts: List of strings to embed
            task_type: Task type to use for this embedding call

        Returns:
            EmbedContentResponse from the API
        """
        return self.client.models.embed_content(
            model=self.model_name,
            contents=texts,
            config=types.EmbedContentConfig(
                task_type=task_type.value,
                output_dimensionality=self.output_dimensionality,
            ),
        )

    @staticmethod
    def _normalize_embedding(embedding: List[float]) -> List[float]:
        """
        Normalize an embedding vector to unit length.

        For dimensions other than 3072, Gemini embeddings are not automatically
        normalized. This function normalizes vectors using L2 normalization.

        Args:
            embedding: Raw embedding vector

        Returns:
            Normalized embedding vector with unit length
        """
        embedding_array = np.array(embedding)
        norm = np.linalg.norm(embedding_array)

        if norm == 0:
            logger = get_logger()
            logger.warning("Zero-norm embedding detected, returning as-is")
            return embedding

        normalized = embedding_array / norm
        return normalized.tolist()

    def __call__(
        self, texts: List[str], task_type: EmbeddingTaskType | None = None
    ) -> List[List[float]]:
        """
        Embed a list of text strings into normalized vectors.

        This method processes all texts in a single batch API call for efficiency,
        then normalizes each embedding to unit length.

        Args:
            texts: List of strings to embed

        Returns:
            List of normalized embedding vectors, one per input text.
            Each vector has length equal to output_dimensionality (default: 768).

        Raises:
            ClientError: If the API call fails after retries
            ValueError: If texts is empty or contains invalid data

        Example:
            >>> embedder = EmbeddingModel()
            >>> embeddings = embedder(["Hello", "World"])
            >>> len(embeddings)
            2
            >>> len(embeddings[0])
            768
        """
        if not texts:
            raise ValueError("texts cannot be empty")

        if not all(isinstance(t, str) for t in texts):
            raise ValueError("All items in texts must be strings")

        resolved_task_type = task_type if task_type is not None else self.task_type
        self.logger.debug(f"Embedding {len(texts)} texts with {self.model_name}")

        # Call API with batch embedding
        response = self._embed_with_retry(texts, resolved_task_type)

        # Extract embeddings, normalizing when dimension < 3072 (Gemini only
        # auto-normalizes at 3072 dimensions)
        normalized_embeddings = []
        for embedding in response.embeddings:
            if self.output_dimensionality < 3072:
                normalized_embeddings.append(
                    self._normalize_embedding(embedding.values)
                )
            else:
                normalized_embeddings.append(list(embedding.values))

        self.logger.debug(
            f"Successfully embedded {len(normalized_embeddings)} texts "
            f"(dimension: {self.output_dimensionality})"
        )

        return normalized_embeddings
