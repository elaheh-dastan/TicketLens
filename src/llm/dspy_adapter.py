"""
DSPy LLM utilities for LangChain compatibility.

This module provides utilities for using LangChain LLMs with DSPy.
"""

import logging
from typing import Any, Optional

from src.config.settings import get_settings

logger = logging.getLogger(__name__)


import dspy


def create_dspy_lm(
    provider: str, model: str, api_key: Optional[str] = None, **kwargs
) -> Any:
    """
    Create a DSPy LM instance.

    Args:
        provider: LLM provider (e.g., 'openai', 'anthropic', 'openrouter')
        model: Model name
        api_key: Optional API key
        **kwargs: Additional LM parameters

    Returns:
        DSPy LM instance
    """

    settings = get_settings()

    # Build model identifier
    if provider and not model.startswith(f"{provider}/"):
        model_id = f"{provider}/{model}"
    else:
        model_id = model

    # Determine API configuration
    resolved_api_key = api_key
    resolved_api_base = kwargs.pop("api_base", None)

    if provider.lower() == "openrouter":
        resolved_api_base = resolved_api_base or "https://openrouter.ai/api/v1"
        resolved_api_key = resolved_api_key or settings.llms.openrouter_api_key

        if not resolved_api_key:
            raise ValueError(
                "OpenRouter API key is required. Set LLMS__OPENROUTER_API_KEY or provide api_key."
            )

    # Create LM with resolved configuration
    lm_kwargs = {"model": model_id}

    if resolved_api_key:
        lm_kwargs["api_key"] = resolved_api_key

    if resolved_api_base:
        lm_kwargs["api_base"] = resolved_api_base

    # Add any additional kwargs
    lm_kwargs.update(kwargs)

    lm = dspy.LM(**lm_kwargs)
    logger.info(f"Created DSPy LM: {model_id}")

    return lm


def configure_dspy_lm(lm: Any):
    """
    Configure DSPy with an LM instance.

    Args:
        lm: DSPy LM instance
    """
    dspy.settings.configure(lm=lm)
    logger.info("DSPy configured with LM")


# Export
__all__ = [
    "create_dspy_lm",
    "configure_dspy_lm",
]
