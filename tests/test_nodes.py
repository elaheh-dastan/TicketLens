"""
Tests for the node system.

Tests that:
1. BaseNode ABC works correctly
2. Core nodes execute as expected
3. Nodes can be called async
4. State updates work properly
uv run python -m pytest tests/test_nodes.py -q  
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

from src.nodes.base_node import BaseNode
from src.nodes.core_nodes import (
    GeneratorNode,
    RouterNode,
    EndNode,
    PassthroughNode,
)


class TestBaseNode:
    """Test the BaseNode abstract base class."""
    
    def test_base_node_is_abstract(self):
        """BaseNode cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseNode()
    
    def test_base_node_subclass_requires_execute(self):
        """Subclass must implement execute()."""
        with pytest.raises(TypeError):
            class IncompleteNode(BaseNode):
                pass
            IncompleteNode()
    
    @pytest.mark.asyncio
    async def test_concrete_node_execution(self):
        """Concrete node subclass can execute."""
        class SimpleNode(BaseNode):
            async def execute(self, state: dict) -> dict:
                return {"result": "ok"}
        
        node = SimpleNode("test_node")
        result = await node.execute({"input": "test"})
        assert result == {"result": "ok"}
    
    @pytest.mark.asyncio
    async def test_node_call_interface(self):
        """Nodes are callable via __call__."""
        class SimpleNode(BaseNode):
            async def execute(self, state: dict) -> dict:
                return {"output": state.get("input", "default")}
        
        node = SimpleNode()
        result = await node("test")
        # When calling directly, it tries to call as state, but node expects dict
        # So we need to pass a dict
        state = {"input": "hello"}
        result = await node(state)
        assert result == {"output": "hello"}
    
    def test_node_repr(self):
        """Node has useful string representation."""
        class TestNode(BaseNode):
            async def execute(self, state: dict) -> dict:
                return {}
        
        node = TestNode("my_node")
        assert "TestNode" in repr(node)
        assert "my_node" in repr(node)


class TestGeneratorNode:
    """Test the GeneratorNode."""
    
    @pytest.mark.asyncio
    async def test_generator_node_requires_llm(self):
        """GeneratorNode requires LLM to be set."""
        node = GeneratorNode()
        
        with pytest.raises(RuntimeError, match="no LLM configured"):
            await node.execute({"messages": []})
    
    @pytest.mark.asyncio
    async def test_generator_node_requires_prompt_key(self):
        """GeneratorNode requires prompt key in state."""
        node = GeneratorNode(prompt_key="messages")
        node.llm = AsyncMock()
        
        with pytest.raises(ValueError, match="State missing key"):
            await node.execute({})  # No 'messages' key
    
    @pytest.mark.asyncio
    async def test_generator_node_generates(self):
        """GeneratorNode calls LLM and returns response."""
        # Mock LLM
        mock_response = MagicMock()
        mock_response.content = "Generated response"
        
        mock_llm = AsyncMock()
        mock_llm.ainvoke = AsyncMock(return_value=mock_response)
        
        node = GeneratorNode(prompt_key="input", output_key="response")
        node.llm = mock_llm
        
        state = {"input": "Hello"}
        result = await node.execute(state)
        
        assert "response" in result
        mock_llm.ainvoke.assert_called_once_with("Hello")
    
    def test_generator_node_config(self):
        """GeneratorNode can be configured."""
        node = GeneratorNode(
            name="my_gen",
            prompt_key="prompt",
            output_key="answer",
            temperature=0.5,
            max_tokens=100,
        )
        
        assert node.name == "my_gen"
        assert node.prompt_key == "prompt"
        assert node.output_key == "answer"
        assert node.temperature == 0.5
        assert node.max_tokens == 100


class TestRouterNode:
    """Test the RouterNode."""
    
    @pytest.mark.asyncio
    async def test_router_node_requires_route_func(self):
        """RouterNode requires a route function."""
        node = RouterNode()
        
        with pytest.raises(RuntimeError, match="no route function"):
            await node.execute({})
    
    @pytest.mark.asyncio
    async def test_router_node_routes(self):
        """RouterNode calls route function and returns result."""
        route_func = lambda state: "next_node"
        node = RouterNode(route_func=route_func)
        
        state = {"input": "test"}
        result = await node.execute(state)
        
        assert result == {"route": "next_node"}
    
    @pytest.mark.asyncio
    async def test_router_node_conditional_routing(self):
        """RouterNode can implement complex routing logic."""
        def complex_route(state):
            if state.get("error"):
                return "error_handler"
            elif state.get("approved"):
                return "process"
            else:
                return "review"
        
        node = RouterNode(route_func=complex_route)
        
        # Test different branches
        assert (await node.execute({"error": True}))["route"] == "error_handler"
        assert (await node.execute({"approved": True}))["route"] == "process"
        assert (await node.execute({}))["route"] == "review"


class TestEndNode:
    """Test the EndNode."""
    
    @pytest.mark.asyncio
    async def test_end_node_execution(self):
        """EndNode executes without error."""
        node = EndNode()
        result = await node.execute({"output": "final"})
        
        assert result == {}  # No updates


class TestPassthroughNode:
    """Test the PassthroughNode."""
    
    @pytest.mark.asyncio
    async def test_passthrough_execution(self):
        """PassthroughNode passes state through."""
        node = PassthroughNode()
        state = {"input": "test", "other": "data"}
        result = await node.execute(state)
        
        assert result == {}  # No changes


class TestNodeIntegration:
    """Integration tests with multiple nodes."""
    
    @pytest.mark.asyncio
    async def test_node_chain(self):
        """Nodes can be chained together."""
        # Create chain: input -> generator -> router -> end
        
        # Mock LLM
        mock_response = MagicMock()
        mock_response.content = "response"
        mock_llm = AsyncMock(ainvoke=AsyncMock(return_value=mock_response))
        
        gen_node = GeneratorNode(prompt_key="input", output_key="generated")
        gen_node.llm = mock_llm
        
        route_func = lambda state: "end" if "generated" in state else "error"
        router_node = RouterNode(route_func=route_func)
        
        # Simulate chain
        state1 = {"input": "hello"}
        state1.update(await gen_node.execute(state1))
        assert "generated" in state1
        
        state2 = state1.copy()
        state2.update(await router_node.execute(state2))
        assert state2["route"] == "end"


@pytest.mark.asyncio
async def test_async_support():
    """All nodes support async execution."""
    class AsyncNode(BaseNode):
        async def execute(self, state: dict) -> dict:
            await asyncio.sleep(0.01)  # Simulate async work
            return {"processed": True}
    
    node = AsyncNode()
    result = await node.execute({})
    assert result["processed"] is True
