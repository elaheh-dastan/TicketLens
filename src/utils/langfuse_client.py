"""
Langfuse client wrapper for observability integration.

This module provides a singleton Langfuse client that reads configuration
from settings and provides helper functions for tracing node execution.

Updated for Langfuse SDK v3 API which uses:
- start_span() instead of trace().span()
- start_generation() for LLM calls
- Nested spans via span.start_span()
- Sessions for grouping related traces
"""

import logging
import contextvars
from typing import Optional, Dict, Any, Generator, TYPE_CHECKING
from contextlib import contextmanager

from langfuse import Langfuse

from src.config.settings import get_settings

if TYPE_CHECKING:
    from langfuse._client.span import LangfuseSpan, LangfuseGeneration
    from langfuse._client.trace import LangfuseTrace

logger = logging.getLogger(__name__)

# Global Langfuse client instance
_langfuse_client: Optional[Langfuse] = None

# Key for storing trace context in state dictionary
LANGFUSE_TRACE_KEY = "_langfuse_trace"

# ContextVar for propagating the current Langfuse span across async calls
# within a single graph.ainvoke() execution. This is more reliable than
# passing spans through LangGraph state (which may drop undeclared keys).
_current_langfuse_span: contextvars.ContextVar[Optional[Any]] = contextvars.ContextVar(
    "langfuse_span", default=None
)


def get_current_langfuse_span() -> Optional[Any]:
    """Get the current Langfuse span from the async context."""
    return _current_langfuse_span.get()


def set_current_langfuse_span(span: Optional[Any]) -> None:
    """Set the current Langfuse span in the async context."""
    _current_langfuse_span.set(span)


def get_langfuse_client() -> Optional[Langfuse]:
    """
    Get the global Langfuse client instance.

    Returns:
        Langfuse client if enabled, None otherwise
    """
    global _langfuse_client

    settings = get_settings()

    # Check if Langfuse is enabled
    if not settings.langfuse.enabled:
        logger.debug("Langfuse is disabled")
        return None

    # Check if credentials are configured
    if not settings.langfuse.secret_key or not settings.langfuse.public_key:
        logger.warning("Langfuse is enabled but credentials are not configured")
        return None

    # Create client if not already created
    if _langfuse_client is None:
        try:
            # Langfuse v3 API - sampling is handled by the SDK via sample_rate
            _langfuse_client = Langfuse(
                secret_key=settings.langfuse.secret_key,
                public_key=settings.langfuse.public_key,
                host=settings.langfuse.host,
                debug=settings.langfuse.debug,
                environment=settings.langfuse.environment or settings.environment,
                release=settings.langfuse.release,
                timeout=settings.langfuse.timeout,
                sample_rate=settings.langfuse.sample_rate,
            )
            logger.info(f"Langfuse client initialized (host: {settings.langfuse.host})")
        except Exception as e:
            logger.error(f"Failed to initialize Langfuse client: {e}")
            _langfuse_client = None

    return _langfuse_client


def start_trace(
    name: str,
    trace_id: Optional[str] = None,
    session_id: Optional[str] = None,
    user_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional["LangfuseSpan"]:
    """
    Create a top-level span for hierarchical tracing.

    In Langfuse v3, traces are created implicitly. This creates a root span
    that serves as the parent for all subsequent node spans.

    Args:
        name: Name of the trace (e.g., "task:agent_name")
        trace_id: Optional trace ID for unifying distributed spans (e.g., correlation_id).
                  If provided, all spans with the same trace_id appear under one trace.
        session_id: Optional session ID for grouping related traces
        user_id: Optional user ID for filtering
        metadata: Optional metadata dictionary

    Returns:
        LangfuseSpan object if tracing is enabled, None otherwise
    """
    client = get_langfuse_client()

    if client is None:
        return None

    try:
        # In v3, we create a root span that will implicitly create the trace
        # Pass session_id and user_id as first-class fields for indexing/filtering
        span = client.start_span(
            name=name,
            trace_id=trace_id,
            session_id=session_id,
            user_id=user_id,
            metadata=metadata,
        )
        return span
    except Exception as e:
        logger.warning(f"Failed to start Langfuse trace '{name}': {e}")
        return None


def start_span(
    name: str,
    trace_id: Optional[str] = None,
    input_data: Optional[Any] = None,
    metadata: Optional[Dict[str, Any]] = None,
    level: Optional[str] = None,
) -> Optional["LangfuseSpan"]:
    """
    Start a new Langfuse span for tracing.

    This is the primary way to create spans in Langfuse v3.

    Args:
        name: Name of the span
        trace_id: Optional trace ID for unifying distributed spans (e.g., correlation_id)
        input_data: Optional input data to log
        metadata: Optional metadata dictionary
        level: Optional level ('DEBUG', 'DEFAULT', 'WARNING', 'ERROR')

    Returns:
        LangfuseSpan object if tracing is enabled, None otherwise
    """
    client = get_langfuse_client()

    if client is None:
        return None

    try:
        span = client.start_span(
            name=name,
            trace_id=trace_id,
            input=input_data,
            metadata=metadata,
            level=level,  # type: ignore[arg-type]
        )
        return span
    except Exception as e:
        logger.warning(f"Failed to start Langfuse span '{name}': {e}")
        return None


def start_generation(
    name: str,
    trace_id: Optional[str] = None,
    model: Optional[str] = None,
    input_data: Optional[Any] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional["LangfuseGeneration"]:
    """
    Start a new Langfuse generation span for LLM calls.

    Args:
        name: Name of the generation
        trace_id: Optional trace ID for unifying distributed spans (e.g., correlation_id)
        model: Model name/identifier
        input_data: Optional input (prompt) data
        metadata: Optional metadata dictionary

    Returns:
        LangfuseGeneration object if tracing is enabled, None otherwise
    """
    client = get_langfuse_client()

    if client is None:
        return None

    try:
        generation = client.start_generation(
            name=name,
            trace_id=trace_id,
            model=model,
            input=input_data,
            metadata=metadata,
        )
        return generation
    except Exception as e:
        logger.warning(f"Failed to start Langfuse generation '{name}': {e}")
        return None


@contextmanager
def trace_node_execution(
    node_name: str,
    node_type: str,
    agent_name: Optional[str] = None,
    input_state: Optional[Dict[str, Any]] = None,
    trace_id: Optional[str] = None,
) -> Generator[Optional["LangfuseSpan"], None, None]:
    """
    Context manager for tracing node execution with Langfuse.

    This creates a span for the node execution and automatically:
    - Records start/end time
    - Captures input/output state
    - Logs any errors

    Updated for Langfuse v3 API.

    Args:
        node_name: Name of the node being executed
        node_type: Type of the node (e.g., "generator", "dspy", "router")
        agent_name: Optional name of the agent
        input_state: Optional input state dictionary
        trace_id: Optional trace ID for unifying distributed spans (e.g., correlation_id)

    Yields:
        LangfuseSpan object if tracing is enabled, None otherwise

    Example:
        with trace_node_execution("analyze", "generator", "qc-agent", state, trace_id="corr-123") as span:
            result = await node.execute(state)
            if span:
                span.update(output={"result": result})
    """
    client = get_langfuse_client()

    # Skip if Langfuse is disabled
    if client is None:
        yield None
        return

    # Create a span for this node execution using Langfuse v3 API
    span_name = (
        f"{agent_name or 'agent'}:{node_type}:{node_name}"
        if agent_name
        else f"{node_type}:{node_name}"
    )

    # Truncate input state for logging
    truncated_input = None
    if input_state:
        truncated_input = _truncate_dict(input_state, max_length=1000)

    try:
        span = client.start_span(
            name=span_name,
            trace_id=trace_id,
            input=truncated_input,
            metadata={
                "node_name": node_name,
                "node_type": node_type,
                "agent_name": agent_name,
            },
        )
    except Exception as e:
        logger.warning(f"Failed to create Langfuse span for node '{node_name}': {e}")
        yield None
        return

    try:
        yield span
    except Exception as e:
        # Log error to span - Langfuse v3 uses 'ERROR' level
        try:
            span.update(
                level="ERROR",
                status_message=str(e),
                metadata={"error_type": type(e).__name__},
            )
        except Exception as update_error:
            logger.warning(f"Failed to update Langfuse span with error: {update_error}")
        logger.error(f"Error in node '{node_name}': {e}", exc_info=True)
        raise
    finally:
        # End the span
        try:
            span.end()
        except Exception as end_error:
            logger.warning(
                f"Failed to end Langfuse span for node '{node_name}': {end_error}"
            )


def _truncate_dict(data: Dict[str, Any], max_length: int = 1000) -> Dict[str, Any]:
    """
    Truncate dictionary values to avoid sending too much data to Langfuse.

    Args:
        data: Dictionary to truncate
        max_length: Maximum length for string values

    Returns:
        Truncated dictionary
    """
    truncated = {}
    for key, value in data.items():
        if isinstance(value, str):
            if len(value) > max_length:
                truncated[key] = value[:max_length] + "... (truncated)"
            else:
                truncated[key] = value
        elif isinstance(value, (dict, list)):
            # For nested structures, just keep the structure but truncate strings
            truncated[key] = str(type(value).__name__)
        else:
            truncated[key] = str(value)
    return truncated


def observe_llm_call(
    node_name: str,
    model: str,
    prompt: str,
    response: str,
    tokens_used: Optional[int] = None,
    latency_ms: Optional[float] = None,
    parent_span: Optional["LangfuseSpan"] = None,
) -> Optional["LangfuseGeneration"]:
    """
    Observe an LLM call and send it to Langfuse.

    Updated for Langfuse v3 API using start_generation().

    Args:
        node_name: Name of the node making the LLM call
        model: Model name/identifier
        prompt: The prompt sent to the LLM
        response: The response from the LLM
        tokens_used: Optional number of tokens used
        latency_ms: Optional latency in milliseconds (not directly supported in v3)
        parent_span: Optional parent span to nest the generation under

    Returns:
        LangfuseGeneration object if created, None otherwise
    """
    context = parent_span or get_langfuse_client()
    if not context:
        return None

    try:
        # Build usage details if tokens provided
        usage_details = None
        if tokens_used:
            usage_details = {
                "input": tokens_used // 2,
                "output": tokens_used // 2,
                "total": tokens_used,
            }

        generation = context.start_generation(
            name=f"{node_name}:llm_call",
            model=model,
            input=prompt[:5000],
            output=response[:5000],
            usage_details=usage_details,
            metadata={"latency_ms": latency_ms},
        )

        generation.end()
        return generation

    except Exception as e:
        logger.error(f"Failed to observe LLM call: {e}")
        return None


def flush_langfuse() -> None:
    """
    Flush any pending Langfuse data.

    This should be called before application shutdown to ensure
    all traces are sent to Langfuse.
    """
    client = get_langfuse_client()

    if client is None:
        return

    try:
        client.flush()
        logger.info("Langfuse data flushed")
    except Exception as e:
        logger.error(f"Failed to flush Langfuse data: {e}")


def shutdown_langfuse() -> None:
    """
    Shutdown the Langfuse client gracefully.

    This flushes pending data and releases resources.
    Should be called on application shutdown.
    """
    global _langfuse_client

    if _langfuse_client is None:
        return

    try:
        _langfuse_client.shutdown()
        logger.info("Langfuse client shutdown complete")
    except Exception as e:
        logger.error(f"Failed to shutdown Langfuse client: {e}")
    finally:
        _langfuse_client = None
