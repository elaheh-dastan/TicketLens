"""
Tests for the AgentFactory.

Verifies that the factory can load a simple YAML config and build a compiled graph.
uv run python -m pytest tests/test_factory.py -q
"""

import pytest
import asyncio
from src.agent.factory import AgentFactory


@pytest.mark.asyncio
async def test_factory_builds_graph(tmp_path):
    cfg_path = tmp_path / "echo.yml"
    cfg_path.write_text(
        """
agent:
  agent_name: test_agent
  models:
    generator: gpt-3.5-turbo
    provider: openrouter
    embedding: text-embedding-3-small
  graph:
    entry_point: start
    nodes:
      start:
        type: passthrough
        next: __end__
"""
    )

    factory = AgentFactory(cfg_path)
    graph = await factory.create()
    assert graph is not None

    # Invoke the compiled graph with a simple input state
    # AgentState has messages, output, metadata fields
    initial_state = {"messages": [{"role": "user", "content": "hello"}]}
    result = await graph.ainvoke(initial_state)
    # For passthrough node nothing changes; ensure result is a dict or similar
    # The result should be the state after execution
    assert result is not None, "Graph invocation returned None"
    assert isinstance(result, dict) or isinstance(result, list), f"Expected dict or list, got {type(result)}"
