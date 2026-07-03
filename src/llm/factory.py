import asyncio
from src.llm.openrouter import OpenRouterChat
from src.config.settings import get_settings
from typing import Dict, Any
import hashlib
import json


class LLMClientPool:
    """
    Pool for reusing LLM client instances.

    This pool caches LLM clients based on their configuration (provider, model, etc.)
    to avoid creating new connections for each request, improving performance and
    reducing API rate limit issues.
    """

    def __init__(self):
        """Initialize the LLM client pool."""
        self._pool: Dict[str, Any] = {}
        self._lock = asyncio.Lock()  # Protect pool access

    def _get_cache_key(self, provider: str, model: str, **kwargs) -> str:
        """
        Generate a cache key for the LLM configuration.

        Args:
            provider: The LLM provider
            model: The model name
            **kwargs: Additional configuration

        Returns:
            A unique cache key string
        """
        # Create a deterministic key from the configuration
        config = {"provider": provider, "model": model, **kwargs}
        # Remove API key from cache key for security
        config.pop("openrouter_api_key", None)

        base_url = kwargs.get("base_url", "https://openrouter.ai/api/v1")
        config["base_url"] = base_url

        # Hash the configuration to create a unique key
        config_str = json.dumps(config, sort_keys=True)
        return hashlib.md5(config_str.encode()).hexdigest()

    async def get_client(self, provider: str, model: str, **kwargs) -> Any:
        """
        Get or create an LLM client from the pool with thread-safe access.

        Args:
            provider: The LLM provider
            model: The model name
            **kwargs: Additional configuration

        Returns:
            An LLM instance
        """
        base_url = kwargs.get("base_url", None) or "https://openrouter.ai/api/v1"

        # Add base_url to kwargs for cache key generation
        cache_kwargs = kwargs.copy()
        cache_kwargs["base_url"] = base_url

        cache_key = self._get_cache_key(provider, model, **cache_kwargs)

        # Thread-safe check-and-create pattern
        async with self._lock:
            # Check if client exists in pool
            if cache_key in self._pool:
                return self._pool[cache_key]

            # Create new client
            client = await self._create_client(provider, model, **kwargs)

            # Cache the client
            self._pool[cache_key] = client

            return client

    async def _create_client(self, provider: str, model: str, **kwargs) -> Any:
        """
        Create a new LLM client.

        Args:
            provider: The LLM provider
            model: The model name
            **kwargs: Additional configuration

        Returns:
            An LLM instance
        """
        # Enforce OpenRouter provider
        if provider != "openrouter":
            # Log warning or just override? The user said "strictly enforces OpenRouter".
            # So if they pass "openai", we should probably treat it as "openrouter" or fail.
            # Given the instruction "defaults to and strictly enforces OpenRouter",
            # I will assume we should treat everything as OpenRouter or fail if it's not.
            # But to be safe and helpful, if they pass something else, we might want to fail to let them know.
            # However, the instruction says "defaults to", so maybe we should just use OpenRouter regardless?
            # "Update ... to eliminate dynamic provider selection, ensuring the system defaults to and strictly enforces OpenRouter"
            # This suggests we should just use OpenRouter.
            pass

        settings = get_settings()
        openrouter_api_key = (
            kwargs.pop("openrouter_api_key", None)
            or settings.llms.openrouter_api_key
        )
        base_url = kwargs.pop("base_url", None) or "https://openrouter.ai/api/v1"

        if not openrouter_api_key:
            raise ValueError("OpenRouter API key is required")

        return OpenRouterChat(
            model=model,
            openrouter_api_key=openrouter_api_key,
            base_url=base_url,
            **kwargs,
        )

    def clear(self):
        """Clear all cached clients."""
        self._pool.clear()

    def size(self) -> int:
        """Get the number of cached clients."""
        return len(self._pool)


# Global LLM client pool instance
_llm_pool = LLMClientPool()


async def create_llm_client(provider: str, model: str, **kwargs) -> Any:
    """Create or get a cached LLM instance based on the provider.

    This function uses a pool to reuse LLM clients with the same configuration,
    improving performance and reducing API rate limit issues.

    Args:
        provider: The LLM provider (e.g., 'openrouter')
        model: The model name
        **kwargs: Additional arguments for the LLM (e.g., response_format, temperature, max_tokens)

    Returns:
        An LLM instance
    """
    return await _llm_pool.get_client(provider, model, **kwargs)


def get_llm_pool() -> LLMClientPool:
    """Get the global LLM client pool instance.

    Returns:
        The LLMClientPool instance
    """
    return _llm_pool
