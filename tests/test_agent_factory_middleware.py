import yaml
import tempfile
import asyncio
import pytest
from src.agent.factory import AgentFactory
from unittest.mock import patch


@pytest.mark.asyncio
async def test_factory_injects_llm_and_retries(monkeypatch, tmp_path):
    # Create a minimal agent config file
    cfg = {
        "agent": {
            "agent_name": "test_agent",
            "models": {"generator": "gpt-3.5-turbo", "provider": "openrouter", "api_key": "testkey"},
            "graph": {"entry_point": "gen", "nodes": {"gen": {"type": "generator", "next": "__end__"}}},
            "features": {"middleware": {"model_retry": True, "model_retry_retries": 2}}
        }
    }
    cfg_file = tmp_path / "agent.yml"
    cfg_file.write_text(yaml.safe_dump(cfg))

    # Create a flaky LLM that fails once then succeeds
    class FlakyLLM:
        def __init__(self):
            self.calls = 0
        async def ainvoke(self, prompt):
            self.calls += 1
            if self.calls < 2:
                raise RuntimeError("transient")
            return {"content": "ok"}

    async def fake_create_llm_client(provider=None, model=None, api_key=None, api_base=None, **kwargs):
        return FlakyLLM()

    # Patch the async factory function directly so async callers (GeneratorNode)
    # will await the fake implementation.
    monkeypatch.setattr("src.llm.factory.create_llm_client", fake_create_llm_client)

    factory = AgentFactory(str(cfg_file))
    # load config and build graph (async builder)
    await factory.load_config()
    graph = await factory.build_graph()
    # invoke graph asynchronously (nodes are async and factory registers async callables)
    res = await graph.ainvoke({"messages": ["hello"]})
    assert res is not None