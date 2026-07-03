FROM python:3.13.9-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1 \
  UV_SYSTEM_PYTHON=1 \
  UV_COMPILE_BYTECODE=1 \
  UV_LINK_MODE=copy \
  UV_PYTHON_DOWNLOADS=never \
  PYTHONPATH=/app \
  PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc

RUN apt-get update && apt-get install -y --no-install-recommends \
  ca-certificates \
  curl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install uv from PyPI
RUN pip install --no-cache-dir uv

# Copy dependency files first for layer caching
COPY pyproject.toml uv.lock ./

# Install dependencies without project code (layer cache optimization)
RUN uv sync --frozen --no-install-project --no-dev --python $(which python3)

# Copy application code
COPY src/ src/
COPY scripts/ scripts/
COPY agent_config/ agent_config/
COPY prompts/ prompts/
COPY data/ data/
COPY mcp/ mcp/

# Install project itself
RUN uv sync --frozen --no-dev --python $(which python3)

# Cleanup
RUN find /usr/local -type d -name '__pycache__' -exec rm -r {} + 2>/dev/null || true && \
  find /usr/local -type f -name '*.pyc' -delete && \
  find /usr/local -type f -name '*.pyo' -delete

RUN mkdir -p /tmp/prometheus_multiproc

# HTTP API port
EXPOSE 8000

CMD ["uv", "run", "--no-sync", "uvicorn", "src.api.server:app", "--host", "0.0.0.0", "--port", "8000"]
