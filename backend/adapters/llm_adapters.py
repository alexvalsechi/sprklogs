"""
LLM Adapters
============
Adapter Pattern: each provider exposes the same `complete(prompt) -> str` interface.
Singleton Pattern: LLMClientFactory caches provider instances per key.
"""
from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from typing import Optional

logger = logging.getLogger(__name__)

# ─── Abstract Adapter ────────────────────────────────────────────────────────

class BaseLLMAdapter(ABC):
    MAX_RETRIES = 3
    RETRY_DELAY = 2.0

    def complete(self, prompt: str) -> str:
        for attempt in range(self.MAX_RETRIES):
            try:
                return self._complete(prompt)
            except Exception as exc:
                if attempt == self.MAX_RETRIES - 1:
                    raise
                logger.warning("LLM attempt %d failed: %s — retrying…", attempt + 1, exc)
                time.sleep(self.RETRY_DELAY * (attempt + 1))
        return ""  # unreachable

    @abstractmethod
    def _complete(self, prompt: str) -> str:
        ...


# ─── Concrete Adapters ───────────────────────────────────────────────────────

class OpenAIAdapter(BaseLLMAdapter):
    MODEL = "gpt-4o-mini"

    def __init__(self, api_key: str):
        try:
            from openai import OpenAI  # type: ignore
        except ImportError as e:
            raise ImportError("openai package not installed. Run: pip install openai") from e
        self._client = OpenAI(api_key=api_key)

    def _complete(self, prompt: str) -> str:
        response = self._client.chat.completions.create(
            model=self.MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=8192,
            temperature=0,
            seed=42,
        )
        return response.choices[0].message.content or ""


class AnthropicAdapter(BaseLLMAdapter):
    MODEL = "claude-haiku-4-5-20251001"

    def __init__(self, api_key: str):
        try:
            import anthropic  # type: ignore
        except ImportError as e:
            raise ImportError("anthropic package not installed. Run: pip install anthropic") from e
        self._client = anthropic.Anthropic(api_key=api_key)

    def _complete(self, prompt: str) -> str:
        message = self._client.messages.create(
            model=self.MODEL,
            max_tokens=8192,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text


class GeminiAdapter(BaseLLMAdapter):
    MODEL = "gemini-3-flash-preview"

    def __init__(self, api_key: str):
        try:
            import google.generativeai as genai  # type: ignore
        except ImportError as e:
            raise ImportError("google-generativeai package not installed. Run: pip install google-generativeai") from e
        genai.configure(api_key=api_key)
        self._model = genai.GenerativeModel(self.MODEL)

    def _complete(self, prompt: str) -> str:
        from google.generativeai.types import GenerationConfig  # type: ignore
        response = self._model.generate_content(
            prompt,
            generation_config=GenerationConfig(temperature=0),
        )
        return response.text


class NoOpAdapter(BaseLLMAdapter):
    """Used when no LLM provider is configured — returns a polite notice."""

    def _complete(self, prompt: str) -> str:
        return (
            "⚠️ **LLM analysis not configured.**\n\n"
            "Set `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, or `GEMINI_API_KEY` in your environment, "
            "or supply an API key via the web form."
        )


# ─── Factory / Singleton cache ───────────────────────────────────────────────

class LLMClientFactory:
    """
    Factory + Singleton: creates and caches adapter instances.
    Same (provider, key) pair returns the same instance.
    """

    _instances: dict[tuple[str, str], BaseLLMAdapter] = {}

    @classmethod
    def get(
        cls,
        provider: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> BaseLLMAdapter:
        if not provider or not api_key:
            return NoOpAdapter()

        cache_key = (provider.lower(), api_key)
        if cache_key not in cls._instances:
            cls._instances[cache_key] = cls._build(provider, api_key)
        return cls._instances[cache_key]

    @classmethod
    def _build(cls, provider: str, api_key: str) -> BaseLLMAdapter:
        match provider.lower():
            case "openai":
                return OpenAIAdapter(api_key)
            case "anthropic":
                return AnthropicAdapter(api_key)
            case "gemini":
                return GeminiAdapter(api_key)
            case _:
                logger.warning("Unknown LLM provider '%s', using no-op.", provider)
                return NoOpAdapter()
