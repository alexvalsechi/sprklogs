"""
Configuration — Singleton via functools.lru_cache.
Reads from environment variables or .env file.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # LLM
    llm_provider: Optional[str] = None          # "openai" | "anthropic"
    llm_api_key: Optional[str] = None
    openai_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None

    # OAuth2 — OpenAI
    openai_oauth_client_id: Optional[str] = None
    openai_oauth_client_secret: Optional[str] = None

    # OAuth2 — Anthropic
    anthropic_oauth_client_id: Optional[str] = None
    anthropic_oauth_client_secret: Optional[str] = None

    # OAuth2 — Google Gemini
    google_oauth_client_id: Optional[str] = None
    google_oauth_client_secret: Optional[str] = None

    # CORS
    cors_origins: list[str] = ["*"]

    # Storage
    upload_tmp_dir: str = "/tmp/sparkui_uploads"

    # Feature flags
    max_zip_mb: int = 500
    max_uncompressed_mb: int = 1000  # Max uncompressed size (2x compressed)
    max_files_in_zip: int = 1000     # Max number of files in ZIP
    compression_ratio_limit: int = 100  # Max compression ratio

    # Celery
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/0"

    # Auth & Security
    secret_key: str = "your-secret-key-change-in-production"
    frontend_url: str = "http://localhost:8000"

    def model_post_init(self, __context):
        # Convenience: promote OPENAI_API_KEY / ANTHROPIC_API_KEY to unified fields
        if not self.llm_api_key:
            if self.openai_api_key:
                object.__setattr__(self, "llm_provider", self.llm_provider or "openai")
                object.__setattr__(self, "llm_api_key", self.openai_api_key)
            elif self.anthropic_api_key:
                object.__setattr__(self, "llm_provider", self.llm_provider or "anthropic")
                object.__setattr__(self, "llm_api_key", self.anthropic_api_key)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
