# AI Support QC

A **configuration-driven framework** for automated quality control of customer support interactions. Define QC workflows in YAML, and the framework orchestrates them through a LangGraph StateGraph architecture. Built on Python 3.13+ with FastAPI, it evaluates support conversations and publishes results via Kafka.

## Quick Start

### 1. Install Dependencies

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync
```

### 2. Start Services

```bash
docker compose up -d
```

### 3. Configure Environment

```bash
cp .env.example .env
# Edit .env and set: LLMS__OPENROUTER_API_KEY=your-key-here
```

### 4. Run the API Server

```bash
# Development mode
uv run uvicorn src.api.server:app --reload --host 0.0.0.0 --port 8000

# Production
uvicorn src.api.server:app --workers 4 --host 0.0.0.0 --port 8000
```

### 5. Call the QC API

```bash
# Evaluate a support ticket (returns task_id for background processing)
curl -X POST "http://localhost:8000/api/v2/qc/evaluate" \
  -H "Content-Type: application/json" \
  -d '{"chat_id": "ticket-123", "chat_conversation": [...]}'

# Check task status
curl "http://localhost:8000/tasks/{task_id}"

# Health check
curl "http://localhost:8000/health"
```

## How It Works

Define your QC agent in YAML:

```yaml
agent_name: "qc-agent"

agent_state:
  chat_id: str
  chat_conversation: list[dict]
  analysis_result: dict
  qc_evaluation: dict

models:
  generator: "openrouter/openai/gpt-4o-mini"

graph:
  entry_point: "analyze"
  nodes:
    analyze:
      type: "generator"
      prompt: "qc_agent/analysis.jinja2"
      next: "evaluate"
    evaluate:
      type: "generator"
      prompt: "qc_agent/evaluation.jinja2"
      next: "__end__"
```

The framework automatically validates, builds, and executes your QC agent workflow.

## Project Structure

```text
ai-support-qc/
├── agent_config/          # Agent configurations
│   ├── qc_agent.yml       # QC agent definition
│   ├── apis/              # API endpoint configs (auto-discovered)
│   │   └── qc_api.yml
│   ├── custom_tools/      # Custom tools
│   └── custom_nodes/      # Custom nodes
├── prompts/               # Jinja2 prompt templates
│   └── qc_agent/          # QC-specific prompts
├── data/                  # Training data
│   └── training/          # DSPy training data
├── scripts/               # Utility scripts
├── src/                   # Framework code
│   ├── agent/             # Agent factory and state
│   ├── api/               # FastAPI server and task management
│   ├── config/            # Settings and configuration
│   ├── nodes/             # Node implementations
│   ├── llm/               # LLM providers and DSPy adapter
│   ├── middleware/         # Retry and logging middleware
│   ├── kafka/             # Kafka producer/consumer
│   └── utils/             # Tool loader, Langfuse client
└── tests/                 # Test suite
```

## Node Types

| Type | Purpose |
|------|---------|
| `generator` | LLM text generation with Jinja2 templates |
| `dspy` | Programmatic prompt optimization using training data |
| `validator` | Input schema validation |
| `router` | Conditional branching |
| `tool_executor` | Execute registered tools |
| `passthrough` | Pass state unchanged |

## Configuration

### Environment Variables

See `.env.example` for all options. Key settings:

```bash
# LLM Provider (required)
LLMS__OPENROUTER_API_KEY=your-key-here

# Kafka
KAFKA__ENABLED=false
KAFKA__BOOTSTRAP_SERVERS=localhost:9092
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v2/qc/evaluate` | Evaluate support ticket (returns task_id) |
| POST | `/api/v2/qc/cancel/{task_id}` | Cancel running task |
| GET | `/tasks/{task_id}` | Get task status |
| GET | `/tasks` | List all tasks |
| GET | `/health` | Health check |
| GET | `/status` | Server status |

## Testing

```bash
# Run all tests
uv run pytest tests/ -v

# Unit tests only (no external dependencies)
uv run pytest -m "not integration" tests/

# Integration tests (requires Kafka on localhost:9092)
docker compose up -d kafka zookeeper
uv run pytest -m integration tests/

# Code quality
uv run black src/ tests/
uv run ruff check src/ tests/ --fix
uv run mypy src/
```

## Adding a New QC Agent

1. Create `agent_config/my_agent.yml` with graph definition
2. Create prompt templates in `prompts/my_agent/`
3. Create API config in `agent_config/apis/my_api.yml`
4. Restart server - API auto-discovered

## Adding Custom Tools

Create `agent_config/custom_tools/my_tool.py`:

```python
from langchain_core.tools import tool

@tool
async def my_tool(input_data: str) -> str:
    """Description of what your tool does."""
    return result
```

Reference in your agent config:
```yaml
tools:
  - "my_tool"
```

## What We Don't Handle

1. **Failed messages are not retried.** The consumer commits the Kafka offset after every message, regardless of whether processing succeeded or failed. If your agent throws an error, the error is published to the results topic, but the original message will not be redelivered. If you need retry semantics, implement a Dead Letter Queue (DLQ) on your side.

2. **Agents must be idempotent.** The framework provides at-least-once delivery, not exactly-once. In edge cases (e.g., crash after processing but before offset commit), a message may be delivered again. Design your agents to handle duplicate inputs safely.

3. **Input/output transformers are mandatory and we crash on format mismatches by design.** Both `input_transformer` and `output_transformer` must be provided to `AgentKafkaService`. If the upstream message format changes unexpectedly, the transformer will raise an exception and the service will crash. This is intentional -- a breaking schema change should surface immediately, not be silently swallowed.
