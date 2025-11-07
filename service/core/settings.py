# app/core/settings.py
from pydantic import BaseSettings, AnyHttpUrl, Field
from typing import Optional

class Settings(BaseSettings):
    # App
    APP_NAME: str = "astro-reason"
    APP_ENV: str = "dev"  # dev|staging|prod
    LOG_LEVEL: str = "INFO"

    # Postgres
    PG_DSN: str = Field(..., description="postgresql://user:pass@host:5432/db")

    # Redis (if/when you add a queue)
    REDIS_URL: Optional[str] = None

    # MinIO / S3 for raw blobs
    S3_ENDPOINT: Optional[str] = None
    S3_ACCESS_KEY: Optional[str] = None
    S3_SECRET_KEY: Optional[str] = None
    S3_BUCKET: Optional[str] = "astro-reason"
    S3_USE_SSL: bool = False

    # LLM / Embeddings
    OLLAMA_URL: Optional[AnyHttpUrl] = None
    LLM_MODEL: str = "qwen2.5:7b-instruct-q4_K_M"
    EMBEDDINGS_MODEL: str = "BAAI/bge-large-en-v1.5"

    # Wikipedia
    WIKI_LANG_DEFAULT: str = "en"

    class Config:
        env_file = ".env"
        case_sensitive = False

settings = Settings()
