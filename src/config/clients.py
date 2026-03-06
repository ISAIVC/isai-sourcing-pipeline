import threading

from supabase import Client, create_client

from src.utils.feature_extractor import EmbeddingModel
from src.utils.qa_model import QAModel

from .settings import get_settings

def get_supabase_client() -> Client:
    settings = get_settings()
    return create_client(
        settings.supabase_url,
        settings.supabase_service_role_key.get_secret_value(),
    )


_QA_MODEL = None
_qa_model_lock = threading.Lock()


def get_qa_model(max_workers: int = 10) -> QAModel:
    global _QA_MODEL
    if _QA_MODEL is None:
        with _qa_model_lock:
            _QA_MODEL = QAModel(
                credentials=get_settings().google_credentials_parsed,
                project=get_settings().google_cloud_project,
                max_workers=max_workers,
            )
    return _QA_MODEL


_EMBEDDING_MODEL = None
_embedding_model_lock = threading.Lock()


def get_embedding_model() -> EmbeddingModel:
    global _EMBEDDING_MODEL
    if _EMBEDDING_MODEL is None:
        with _embedding_model_lock:
            _EMBEDDING_MODEL = EmbeddingModel(
                output_dimensionality=768,
                credentials=get_settings().google_credentials_parsed,
                project=get_settings().google_cloud_project,
            )
    return _EMBEDDING_MODEL
