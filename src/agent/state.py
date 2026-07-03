"""
Agent state definitions.

Defines the TypedDict schemas used for agent state in LangGraph.
These are the contracts for what state looks like and how it's updated.

Extending State
---------------
Users can extend these states by creating their own TypedDict that inherits
from AgentState or ExtendedAgentState:

    from src.agent.state import AgentState
    from typing_extensions import TypedDict

    class MyCustomState(AgentState):
        custom_field: str
        another_field: int

Or for more complex agents:

    from src.agent.state import ExtendedAgentState

    class RAGState(ExtendedAgentState):
        retrieved_context: list[str]
        relevance_scores: list[float]
"""

from typing import Any, Optional, Annotated, Dict
from typing_extensions import TypedDict
from operator import add

from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages

# Key for storing Langfuse trace/span in state for hierarchical tracing
LANGFUSE_TRACE_KEY = "_langfuse_trace"


class AgentState(TypedDict, total=False):
    """
    Base agent state for simple agents.

    This is the minimal state schema. Agents can extend this with additional keys.

    Keys:
        messages: Conversation history (auto-appends via add_messages reducer)
        output: Final agent output
        metadata: Additional metadata about execution
        _langfuse_trace: Langfuse trace/span object for hierarchical tracing
    """

    messages: Annotated[list[AnyMessage], add_messages]
    output: Optional[str]
    metadata: Optional[dict[str, Any]]
    _langfuse_trace: Optional[
        Any
    ]  # Langfuse trace/span object for hierarchical tracing


class ExtendedAgentState(AgentState):
    """
    Extended state with additional fields for more complex agents.

    Extends the base AgentState with:
    - documents: Retrieved documents for RAG
    - tool_calls: Tool invocations
    - errors: Error tracking
    - checkpoint: For debugging/replay

    Use this as a base for agents that need these common extended fields,
    or inherit from AgentState directly for custom state structures.
    """

    documents: Annotated[list[str], add]  # Appends documents
    tool_calls: Annotated[list[dict], add]  # Appends tool calls
    errors: Annotated[list[str], add]  # Appends errors
    checkpoint: Optional[dict[str, Any]]  # Latest checkpoint


def create_agent_state(state_fields: Dict[str, Any], base_class=None) -> type:
    """
    Create a custom AgentState class dynamically from a dictionary of field definitions.

    This allows agents to define their own state fields in YAML config without
    needing to create Python classes manually.

    Args:
        state_fields: Dictionary mapping field names to their types/annotations
                     Example: {"url": Optional[str], "quiz_output": Optional[Any]}
        base_class: Base class to inherit from (default: AgentState)

    Returns:
        A new TypedDict class with the specified fields

    Example:
        >>> fields = {
        ...     "url": Optional[str],
        ...     "scraped_content": Optional[Any],
        ...     "quiz_output": Optional[Any]
        ... }
        >>> QuizState = create_agent_state(fields)
    """
    if base_class is None:
        base_class = AgentState

    # Merge annotations from base class with new state_fields
    # This ensures fields like _langfuse_trace are inherited
    merged_annotations = {}
    if hasattr(base_class, "__annotations__"):
        merged_annotations.update(base_class.__annotations__)
    merged_annotations.update(state_fields)

    # Create a new TypedDict class that inherits from the base class
    # TypedDict requires all fields to be defined at class creation time
    return type(
        "CustomAgentState",
        (base_class,),
        {
            "__annotations__": merged_annotations,
            "total": False,  # Make all fields optional
        },
    )


__all__ = ["AgentState", "ExtendedAgentState", "create_agent_state"]
