from langchain_openai import ChatOpenAI
from typing import Optional, Any


class OpenRouterChat(ChatOpenAI):
    """OpenRouter chat model implementation using ChatOpenAI."""

    def __init__(
        self,
        model: str = "openai/gpt-4o-mini",
        openrouter_api_key: Optional[str] = None,
        base_url: str = "https://openrouter.ai/api/v1",
        temperature: float = 0.7,
        max_tokens: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize OpenRouterChat.

        Args:
            model: The model to use (e.g., "openai/gpt-4o-mini")
            openrouter_api_key: OpenRouter API key
            base_url: Base URL for the API
            temperature: Temperature setting
            max_tokens: Maximum tokens to generate
            **kwargs: Additional arguments
        """
        super().__init__(
            model=model,
            api_key=openrouter_api_key,  # type: ignore[arg-type]
            base_url=base_url,
            temperature=temperature,
            max_tokens=max_tokens,  # type: ignore[call-arg]
            **kwargs,
        )
