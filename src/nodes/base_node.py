"""
Abstract base class for all nodes in the agent framework.

Following LangGraph v1 patterns, nodes are functions that:
- Accept state as input
- Optionally accept config (RunnableConfig) for thread_id, metadata, etc.
- Optionally accept runtime for context
- Return a dict update to the state

This module provides the BaseNode abstract class that all custom nodes inherit from.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import logging
import time

from src.metrics.registry import get_metrics


logger = logging.getLogger(__name__)


class BaseNode(ABC):
    """
    Abstract base class for all nodes in the agent framework.

    A node is a function that processes the current state and returns updates.
    All nodes must implement the execute() method.

    Nodes can be async or sync (they'll be converted to RunnableLambda by LangGraph).

    Example:
        class MyNode(BaseNode):
            async def execute(self, state: dict) -> dict:
                result = await some_operation(state)
                return {"key": result}
    """

    def __init__(self, name: Optional[str] = None):
        """
        Initialize a node.

        Args:
            name: Optional name for the node. If not provided, the class name is used.
        """
        self.name = name or self.__class__.__name__
        logger.debug(f"Initialized node: {self.name}")

    @abstractmethod
    async def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the node logic.

        This is the main method that nodes implement. It receives the current state
        and returns updates to the state.

        Args:
            state: The current graph state (TypedDict or dict)

        Returns:
            A dictionary with state updates. Only includes keys that changed.

        Raises:
            Exception: Any exception will be logged and propagated to LangGraph

        Example:
            async def execute(self, state: dict) -> dict:
                # Do something with state
                result = await expensive_operation(state["input"])
                # Return only what changed
                return {"output": result}
        """
        pass

    async def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make the node callable. This is how LangGraph calls the node.

        Args:
            state: The current graph state

        Returns:
            State updates
        """
        logger.info(f"Starting execution of node: {self.name}")

        # Allow callers to pass simple values (e.g. a string) directly
        # by coercing non-dict inputs into a dict under the key 'input'.
        # This makes the node call-site more ergonomic for simple tests
        # and mirrors common patterns where nodes expect an 'input' key.
        if not isinstance(state, dict):
            state = {"input": state}

        # Langfuse tracing - using v3 API with start_span()
        # Uses contextvars for reliable inter-node propagation (LangGraph may
        # drop undeclared keys from state between nodes).
        langfuse_span = None
        try:
            from src.utils.langfuse_client import (
                get_langfuse_client,
                LANGFUSE_TRACE_KEY,
                get_current_langfuse_span,
                set_current_langfuse_span,
            )

            client = get_langfuse_client()
            if client:
                truncated_input = self._truncate_dict(state, max_length=1000)

                # Check for parent span: prefer contextvars, fall back to state
                parent_trace = get_current_langfuse_span() or state.get(
                    LANGFUSE_TRACE_KEY
                )

                if parent_trace:
                    langfuse_span = parent_trace.start_span(
                        name=f"node:{self.__class__.__name__}:{self.name}",
                        input=truncated_input,
                        metadata={
                            "node_name": self.name,
                            "node_type": self.__class__.__name__,
                            "state_keys": list(state.keys()),
                        },
                    )
                else:
                    # First node — create root span
                    langfuse_span = client.start_span(
                        name=f"node:{self.__class__.__name__}:{self.name}",
                        input=truncated_input,
                        metadata={
                            "node_name": self.name,
                            "node_type": self.__class__.__name__,
                            "state_keys": list(state.keys()),
                        },
                    )

                # Always propagate via both mechanisms
                set_current_langfuse_span(langfuse_span)
                state[LANGFUSE_TRACE_KEY] = langfuse_span
        except Exception as e:
            logger.warning(
                f"Failed to initialize Langfuse tracing for node '{self.name}': {e}"
            )
            langfuse_span = None

        # Execute the node with timing
        start_time = time.time()
        try:
            result = await self.execute(state)
            execution_time = time.time() - start_time

            logger.info(
                f"Completed execution of node: {self.name} (took {execution_time:.2f}s)"
            )

            # Prometheus metrics
            try:
                m = get_metrics()
                if m:
                    m.node_executions_total.labels(
                        node_name=self.name, node_type=self.__class__.__name__, status="success",
                    ).inc()
                    m.node_execution_duration_seconds.labels(
                        node_name=self.name, node_type=self.__class__.__name__,
                    ).observe(execution_time)
            except Exception:
                pass

            # Update Langfuse span with result
            if langfuse_span:
                try:
                    truncated_output = self._truncate_dict(result, max_length=1000)
                    langfuse_span.update(
                        output=truncated_output,
                        metadata={
                            "execution_time_seconds": execution_time,
                            "output_keys": list(result.keys()),
                        },
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to update Langfuse span for node '{self.name}': {e}"
                    )

            # Propagate the current span in the result so subsequent nodes nest under it
            if langfuse_span:
                from src.utils.langfuse_client import LANGFUSE_TRACE_KEY

                result[LANGFUSE_TRACE_KEY] = langfuse_span

            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(
                f"Error executing node '{self.name}' after {execution_time:.2f}s: {e}",
                exc_info=True,
            )

            # Prometheus metrics
            try:
                m = get_metrics()
                if m:
                    m.node_executions_total.labels(
                        node_name=self.name, node_type=self.__class__.__name__, status="error",
                    ).inc()
                    m.node_execution_duration_seconds.labels(
                        node_name=self.name, node_type=self.__class__.__name__,
                    ).observe(execution_time)
            except Exception:
                pass

            # Update Langfuse span with error - Langfuse v3 uses 'ERROR' level
            if langfuse_span:
                try:
                    langfuse_span.update(
                        level="ERROR",
                        status_message=str(e),
                        metadata={
                            "error_type": type(e).__name__,
                            "execution_time_seconds": execution_time,
                        },
                    )
                except Exception as langfuse_error:
                    logger.warning(
                        f"Failed to update Langfuse span with error: {langfuse_error}"
                    )

            raise
        finally:
            # End the Langfuse span
            if langfuse_span:
                try:
                    langfuse_span.end()
                except Exception as e:
                    logger.warning(
                        f"Failed to end Langfuse span for node '{self.name}': {e}"
                    )

    def _truncate_dict(
        self, data: Dict[str, Any], max_length: int = 1000
    ) -> Dict[str, Any]:
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

    def __repr__(self) -> str:
        """Return string representation of the node."""
        return f"{self.__class__.__name__}(name='{self.name}')"
