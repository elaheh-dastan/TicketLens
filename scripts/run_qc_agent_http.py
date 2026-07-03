#!/usr/bin/env python3
"""
QC Agent HTTP Server

Runs the QC agent as an HTTP API. Clients submit a ticket (chat conversation)
via `POST /api/v2/qc/evaluate` and receive the QC evaluation synchronously.
This is an alternative to the Kafka ingestion path (run_qc_agent_kafka.py).

Usage:
    python scripts/run_qc_agent_http.py

    # Custom agent config, host and port
    python scripts/run_qc_agent_http.py --config agent_config/qc_agent.yml \
        --host 0.0.0.0 --port 8000

Environment variables:
    API__HOST=0.0.0.0
    API__PORT=8000
    API__AGENT_CONFIG_PATH=agent_config/qc_agent.yml
"""

import argparse
import logging

import uvicorn

from src.api import create_app
from src.config.settings import get_settings

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Run the QC Agent HTTP API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--config",
        default=settings.api.agent_config_path,
        help="Path to agent configuration file",
    )
    parser.add_argument("--host", default=settings.api.host, help="Bind host")
    parser.add_argument("--port", type=int, default=settings.api.port, help="Bind port")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger.info(
        f"Starting QC HTTP API on {args.host}:{args.port} (config={args.config})"
    )
    app = create_app(agent_config_path=args.config)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
