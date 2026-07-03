"""
ASGI entrypoint for the QC HTTP API.

Exposes a module-level ``app`` so the server can be run directly with uvicorn:

    uv run uvicorn src.api.server:app --host 0.0.0.0 --port 8000

The agent config is taken from ``settings.api.agent_config_path`` (override with
``API__AGENT_CONFIG_PATH``). The agent graph is built once on startup.
"""

from .app import create_app

app = create_app()

__all__ = ["app"]
