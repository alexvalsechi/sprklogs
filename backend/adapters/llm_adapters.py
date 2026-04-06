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
from collections import OrderedDict
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
            max_tokens=16384,
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
            max_tokens=16384,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text


class GeminiAdapter(BaseLLMAdapter):
    MODEL = "gemini-3-flash-preview"

    def __init__(self, api_key: str):
        try:
            from google import genai  # type: ignore
        except ImportError as e:
            raise ImportError("google-genai package not installed. Run: pip install google-genai") from e
        self._client = genai.Client(api_key=api_key)

    def _complete(self, prompt: str) -> str:
        from google.genai import types  # type: ignore
        response = self._client.models.generate_content(
            model=self.MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0,
                max_output_tokens=16384,
            ),
        )
        return response.text or ""


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
    Uses LRU eviction to prevent unbounded memory growth.
    """

    _instances: OrderedDict[tuple[str, str], BaseLLMAdapter] = OrderedDict()
    _max_instances = 20  # Limit cache size to prevent memory leaks

    @classmethod
    def get(
        cls,
        provider: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> BaseLLMAdapter:
        if not provider or not api_key:
            return NoOpAdapter()

        cache_key = (provider.lower(), api_key)
        if cache_key in cls._instances:
            # Move to end (most recently used)
            cls._instances.move_to_end(cache_key)
            return cls._instances[cache_key]

        # Evict oldest if at capacity
        while len(cls._instances) >= cls._max_instances:
            cls._instances.popitem(last=False)

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
