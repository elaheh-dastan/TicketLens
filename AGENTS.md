# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project Overview

AI Support QC is a configuration-driven framework for automated quality control of customer support interactions. Users define QC workflows in YAML files, which the framework orchestrates through a LangGraph StateGraph architecture. Built on Python 3.13+ with FastAPI, it evaluates support conversations and publishes results via Kafka.

## Common Commands

```bash
# Install dependencies
uv sync

# Run all tests
uv run pytest tests/ -v

# Run unit tests only (no external dependencies)
uv run pytest -m "not integration" tests/

# Run specific test file
uv run pytest tests/test_qc_agent.py -v

# Integration tests (requires Kafka on 127.0.0.1:9092)
docker compose up -d kafka zookeeper
uv run pytest -m integration tests/

# Code formatting and linting
uv run black src/ tests/
uv run ruff check src/ tests/ --fix
uv run mypy src/

# Start API server (development)
uv run uvicorn src.api.server:app --reload --host 0.0.0.0 --port 8000

# Start infrastructure services
docker compose up -d
```

## Architecture

### Data Flow
```
REST API Request → TaskManager (background task) → AgentFactory (loads YAML, builds LangGraph)
    → StateGraph Execution (Validator → Analyzer → Scorer) → Kafka Producer (publish results)
```

### Key Components

| Component | Location | Purpose |
|-----------|----------|---------|
| AgentFactory | `src/agent/factory.py` | Loads YAML configs, builds LangGraph StateGraph |
| APIFactory | `src/api/api_factory.py` | Creates FastAPI routers from API configs |
| TaskManager | `src/api/task_manager.py` | Background task execution with timeouts |
| Node System | `src/nodes/` | BaseNode, GeneratorNode, DSPyNode, ValidatorNode, RouterNode |
| LLM Factory | `src/llm/factory.py` | Creates LLM clients with provider abstraction and pooling |
| Kafka Producer | `src/kafka/producer.py` | Async singleton for publishing results |

### Configuration-Driven Design

- **Agent configs**: `agent_config/*.yml` - Define agent workflows, nodes, and state
- **API configs**: `agent_config/apis/*.yml` - Define REST endpoints (auto-discovered at startup)
- **Prompt templates**: `prompts/` - Jinja2 templates referenced by generator nodes
- **Training data**: `data/training/` - JSON files for DSPy optimization

### Node Types

- `generator`: LLM text generation with Jinja2 templates
- `dspy`: Programmatic prompt optimization using training data
- `validator`: Input schema validation
- `router`: Conditional branching
- `tool_executor`: Execute registered tools
- `passthrough`: Pass state unchanged

### Adding a New QC Agent

1. Create `agent_config/my_agent.yml` with graph definition
2. Create prompt templates in `prompts/my_agent/`
3. Create API config in `agent_config/apis/my_api.yml`
4. Restart server - API auto-discovered

## Environment Configuration

Required: `LLMS__OPENROUTER_API_KEY` for LLM calls.

Optional services configured via `POSTGRES__*`, `REDIS__*`, `KAFKA__*` prefixes. See `.env.example`.
