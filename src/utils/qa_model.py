import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import Enum
from typing import Generic, List, Optional, TypeVar, Union

from google import genai
from google.genai import types
from google.genai.errors import ClientError, ServerError
from pydantic import BaseModel
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from src.utils.logger import get_logger

T = TypeVar("T")


class ModelName(Enum):
    GEMINI_2_5_FLASH = "gemini-2.5-flash"
    GEMINI_2_5_FLASH_LITE = "gemini-2.5-flash-lite"
    GEMINI_2_5_FLASH_LITE_PREVIEW_09_2025 = "gemini-2.5-flash-lite-preview-09-25"
    GEMINI_2_5_FLASH_PREVIEW_09_2025 = "gemini-2.5-flash-preview-09-2025"
    GEMINI_2_5_FLASH_PREVIEW_TTS = "gemini-2.5-flash-preview-tts"
    GEMINI_2_5_PRO = "gemini-2.5-pro"
    GEMINI_2_5_PRO_PREVIEW_TTS = "gemini-2.5-pro-preview-tts"
    GEMINI_3_FLASH_PREVIEW = "gemini-3-flash-preview"
    GEMINI_3_PRO_PREVIEW = "gemini-3-pro-preview"

    @staticmethod
    def get_cost(model_name: "ModelName") -> dict[str, float]:
        costs = {
            ModelName.GEMINI_3_PRO_PREVIEW: {
                "input": 2.0,
                "output": 12.0,
            },
            ModelName.GEMINI_3_FLASH_PREVIEW: {
                "input": 0.5,
                "output": 3.0,
            },
            ModelName.GEMINI_2_5_FLASH: {
                "input": 0.3,
                "output": 2.5,
            },
            ModelName.GEMINI_2_5_FLASH_PREVIEW_09_2025: {
                "input": 0.3,
                "output": 2.5,
            },
            ModelName.GEMINI_2_5_FLASH_LITE_PREVIEW_09_2025: {
                "input": 0.1,
                "output": 0.4,
            },
        }
        return costs[model_name]


@dataclass
class SimpleAnswer:
    """Simple response with just an answer field"""

    answer: str


class Answer(Generic[T]):
    """Generic response type that can be either a simple response or a structured Pydantic model"""

    def __init__(self, response: Union[str, T]):
        if isinstance(response, str):
            self._response = SimpleAnswer(answer=response)
        else:
            self._response = response

    @property
    def answer(self) -> str:
        """Get the answer, works for both simple and structured responses"""
        if isinstance(self._response, SimpleAnswer):
            return self._response.answer
        elif hasattr(self._response, "answer"):
            return self._response.answer
        else:
            # For structured responses without an 'answer' field, convert to string
            return str(self._response)

    @property
    def is_structured(self) -> bool:
        """Check if this is a structured response"""
        return not isinstance(self._response, SimpleAnswer)

    def __getattr__(self, name):
        """Delegate attribute access to the underlying response"""
        if name == "structured_response":
            # Handle structured_response attribute specially
            if isinstance(self._response, SimpleAnswer):
                raise AttributeError("This response is not structured")
            return self._response
        return getattr(self._response, name)

    def __str__(self) -> str:
        return str(self._response)

    def __repr__(self) -> str:
        return repr(self._response)


@dataclass
class Question:
    text_content: str
    question: str
    system_prompt: str
    pydantic_model: Optional[BaseModel] = None
    temperature: float = 1.0


class QAModel:
    def __init__(
        self,
        api_key: str,
        max_workers: int = 10,
        timeout: int = 120_000,
    ):
        self.client = genai.Client(api_key=api_key)
        self.logger = get_logger()
        self.max_workers = max_workers
        self.timeout = timeout
        self.logger.info(
            f"QAModel initialized | max_workers={max_workers} | timeout={timeout}ms | default model={ModelName.GEMINI_2_5_FLASH_PREVIEW_09_2025}"
        )
        self.input_cost = 0
        self.output_cost = 0
        self.cost_lock = threading.Lock()

    def get_cost(self) -> dict[str, float]:
        total_cost = (self.input_cost + self.output_cost) / 1_000_000
        return {
            "input": self.input_cost / 1_000_000,
            "output": self.output_cost / 1_000_000,
            "total": total_cost,
        }

    def log_cost(self, logger: logging.Logger):
        cost = self.get_cost()
        logger.info(
            f"Current cost is at {cost['total']:.2f} USD [{cost['input']:.2f} USD of input and {cost['output']:.2f} USD of output]"
        )

    def __call__(
        self,
        VQARequest: Question | List[Question],
        model_name: ModelName = ModelName.GEMINI_2_5_FLASH,
    ) -> Answer[BaseModel] | List[Optional[Answer[BaseModel]]]:
        if isinstance(VQARequest, Question):
            return self._process_single_request(VQARequest, model_name)
        elif isinstance(VQARequest, list):
            return self._process_multiple_requests(VQARequest, model_name)
        else:
            raise ValueError(f"Invalid request type: {type(VQARequest)}")

    @staticmethod
    def _is_retryable_error(exception: Exception) -> bool:
        """Check if an exception is retryable"""
        logger = get_logger()
        logger.warning(f"Retryable check for: {type(exception).__name__}: {exception}")
        if isinstance(exception, ServerError):
            return exception.code in {408, 429, 500, 503, 504}
        if isinstance(exception, ClientError):
            return exception.code == 429
        if isinstance(exception, (ConnectionError, TimeoutError, OSError)):
            return True
        return False

    @retry(
        retry=retry_if_exception(_is_retryable_error.__func__),
        wait=wait_exponential(multiplier=2, min=4, max=60),
        stop=stop_after_attempt(6),
        reraise=True,
    )
    def _generate_content_with_retry(
        self,
        contents: List[types.Part],
        config: types.GenerateContentConfig,
        model_name: ModelName,
    ) -> types.GenerateContentResponse:
        """Call the model with retry logic"""
        return self.client.models.generate_content(
            model=model_name,
            contents=contents,
            config=config,
        )

    def _process_single_request(
        self, VQARequest: Question, model_name: ModelName
    ) -> Answer[BaseModel]:

        message = f"# Content Provided\n{VQARequest.text_content}\n\n# Question\n{VQARequest.question}"
        contents = [types.Part.from_text(text=message)]

        config = types.GenerateContentConfig(
            temperature=VQARequest.temperature,
            system_instruction=[types.Part.from_text(text=VQARequest.system_prompt)],
            http_options=types.HttpOptions(timeout=self.timeout),
        )

        # Add JSON schema if structured output is requested
        if VQARequest.pydantic_model is not None:
            # Request JSON output
            config.response_mime_type = "application/json"
            # Add schema from pydantic model
            schema = VQARequest.pydantic_model.model_json_schema()
            config.response_schema = schema

        # Call the model with retry
        response = self._generate_content_with_retry(contents, config, model_name)

        # Lock to update the cost
        with self.cost_lock:
            self.input_cost += (
                response.usage_metadata.prompt_token_count
                * ModelName.get_cost(model_name)["input"]
            )
            self.output_cost += (
                response.usage_metadata.candidates_token_count
                * ModelName.get_cost(model_name)["output"]
            )

        # Parse response
        if VQARequest.pydantic_model is not None:
            # Parse JSON and validate with Pydantic
            response_text = response.text
            try:
                response_json = json.loads(response_text)
                validated_response = VQARequest.pydantic_model(**response_json)
                return Answer(validated_response)
            except (json.JSONDecodeError, ValueError) as e:
                self.logger.error(f"Failed to parse structured response: {e}")
                self.logger.error(f"Response text: {response_text}")
                raise ValueError(f"Failed to parse structured response: {e}")
        else:
            return Answer(response.text)

    def _process_multiple_requests(
        self, VQARequest: List[Question], model_name: ModelName
    ) -> List[Optional[Answer[BaseModel]]]:
        """Process multiple VQA requests in parallel using ThreadPoolExecutor"""
        results = {}
        total = len(VQARequest)
        failed = 0
        resource_exhausted_count = 0
        RESOURCE_EXHAUSTED_LIMIT = 30

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_idx = {
                executor.submit(self._process_single_request, request, model_name): i
                for i, request in enumerate(VQARequest)
            }

            for future in as_completed(future_to_idx):
                i = future_to_idx[future]
                try:
                    results[i] = future.result()
                except Exception as e:
                    self.logger.error(
                        f"Request {i} failed after retries: {type(e).__name__}: {e}"
                    )
                    results[i] = None
                    failed += 1
                    if isinstance(e, ClientError) and e.code == 429:
                        resource_exhausted_count += 1
                        if resource_exhausted_count >= RESOURCE_EXHAUSTED_LIMIT:
                            for f in future_to_idx:
                                f.cancel()
                            raise RuntimeError(
                                f"Aborting: {resource_exhausted_count} RESOURCE_EXHAUSTED (429) errors "
                                f"reached limit of {RESOURCE_EXHAUSTED_LIMIT}. Quota exhausted."
                            )

        if failed:
            self.logger.warning(f"{failed}/{total} requests failed and returned None")

        # Sort results by index to maintain order
        return [results[i] for i in sorted(results.keys())]
