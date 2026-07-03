#!/usr/bin/env python3
"""
QC Agent Kafka Consumer Script

This script runs the QC Agent Kafka Service, which:
1. Consumes QC input messages from Kafka
2. Processes them through the QC agent
3. Publishes evaluation results to Kafka

The Kafka I/O is handled by the service layer, keeping the agent
focused on pure business logic (QC evaluation).

Usage:
    python scripts/run_qc_agent_kafka.py

    # With custom config
    python scripts/run_qc_agent_kafka.py --config agent_config/qc_agent_template.yml

    # With custom topics
    python scripts/run_qc_agent_kafka.py --input-topic my-input --output-topic my-output

Environment variables:
    KAFKA__ENABLED=true
    KAFKA__BOOTSTRAP_SERVERS=localhost:9092
    KAFKA__INPUT_TOPIC=qc-input
    KAFKA__RESULTS_TOPIC=qc-results
    KAFKA__CONSUMER_GROUP=qc-agent
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

from prometheus_client import CollectorRegistry, start_http_server
from prometheus_client.multiprocess import MultiProcessCollector

from src.config.settings import get_settings
from src.kafka.qc_service import QCAgentKafkaService

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run QC Agent Kafka Service",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--config",
        default="agent_config/qc_agent.yml",
        help="Path to agent configuration file (default: agent_config/qc_agent_dspy.yml)",
    )

    parser.add_argument(
        "--input-topic", default=None, help="Kafka input topic (default: from settings)"
    )

    parser.add_argument(
        "--output-topic",
        default=None,
        help="Kafka output topic (default: from settings)",
    )

    parser.add_argument(
        "--consumer-group",
        default=None,
        help="Kafka consumer group (default: from settings)",
    )

    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=10,
        help="Maximum concurrent agent invocations (default: 10)",
    )

    parser.add_argument(
        "--source-agent",
        default="QC_Agent",
        help="Source agent name for envelope metadata (default: QC_Agent)",
    )

    return parser.parse_args()


async def main():
    """Main entry point for the QC Agent Kafka Service."""
    args = parse_args()

    # Load settings
    settings = get_settings()

    # Check if Kafka is enabled
    if not settings.kafka.enabled:
        logger.error(
            "Kafka is not enabled. Set KAFKA__ENABLED=true in environment variables."
        )
        sys.exit(1)

    # Validate config file exists
    config_path = Path(args.config)
    if not config_path.exists():
        logger.error(f"Agent config file not found: {args.config}")
        sys.exit(1)

    logger.info("Starting QC Agent Kafka Service")
    logger.info(f"  Agent config: {args.config}")

    # Log all Kafka settings for debugging configmap/secret issues
    k = settings.kafka
    logger.info("Kafka configuration:")
    logger.info(f"  bootstrap_servers: {k.bootstrap_servers}")
    logger.info(f"  enabled: {k.enabled}")
    logger.info(f"  security_protocol: {k.security_protocol}")
    logger.info(f"  sasl_mechanism: {k.sasl_mechanism}")
    logger.info(f"  sasl_username: {k.sasl_username}")
    logger.info(f"  sasl_password: {'***' if k.sasl_password else None}")
    logger.info(f"  input_topic: {k.input_topic}")
    logger.info(f"  results_topic: {k.results_topic}")
    logger.info(f"  status_topic: {k.status_topic}")
    logger.info(f"  consumer_group: {k.consumer_group}")
    logger.info(f"  auto_offset_reset: {k.auto_offset_reset}")
    logger.info(f"  max_poll_interval_ms: {k.max_poll_interval_ms}")
    logger.info(f"  serialization_format: {k.serialization_format}")
    logger.info(f"  producer_acks: {k.producer_acks}")
    logger.info(f"  producer_retries: {k.producer_retries}")
    logger.info(f"  producer_compression_type: {k.producer_compression_type}")
    logger.info(f"  topic_partitions: {k.topic_partitions}")
    logger.info(f"  topic_replication_factor: {k.topic_replication_factor}")
    logger.info(f"  max_concurrent_tasks: {k.max_concurrent_tasks}")
    logger.info(f"  publish_results: {k.publish_results}")
    logger.info(f"  consume_results: {k.consume_results}")
    logger.info(f"  avro_schema_path: {k.avro_schema_path}")
    logger.info(f"  ssl_verify: {k.ssl_verify}")
    auth = k.auth_kwargs
    logger.info(f"  auth_kwargs keys: {list(auth.keys())}")
    if "ssl_context" in auth:
        ctx = auth["ssl_context"]
        logger.info(f"  ssl_context.protocol: {ctx.protocol}")
        logger.info(f"  ssl_context.verify_mode: {ctx.verify_mode}")
        logger.info(f"  ssl_context.check_hostname: {ctx.check_hostname}")

    # Create service
    service = QCAgentKafkaService(
        agent_config_path=args.config,
        input_topic=args.input_topic,
        output_topic=args.output_topic,
        consumer_group=args.consumer_group,
        source_agent=args.source_agent,
        max_concurrent_tasks=args.max_concurrent,
    )

    # Setup signal handlers for graceful shutdown
    loop = asyncio.get_event_loop()
    shutdown_event = asyncio.Event()

    def signal_handler():
        logger.info("Received shutdown signal")
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    # Start Prometheus metrics HTTP server if metrics are enabled
    if settings.metrics.enabled:
        metrics_host = settings.metrics.host
        metrics_port = settings.metrics.port
        if os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
            registry = CollectorRegistry()
            MultiProcessCollector(registry)
            start_http_server(metrics_port, addr=metrics_host, registry=registry)
        else:
            start_http_server(metrics_port, addr=metrics_host)
        logger.info(f"Prometheus metrics server started on {metrics_host}:{metrics_port}")

    try:
        # Initialize service
        await service.initialize()

        # Run service with shutdown handling
        service_task = asyncio.create_task(service.start())
        shutdown_task = asyncio.create_task(shutdown_event.wait())

        # Wait for either service to complete or shutdown signal
        done, pending = await asyncio.wait(
            [service_task, shutdown_task], return_when=asyncio.FIRST_COMPLETED
        )

        # Cancel pending tasks
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.exception(f"Service error: {e}")
        sys.exit(1)
    finally:
        await service.stop()

        # Print final statistics
        stats = service.get_stats()
        logger.info("Final Statistics:")
        logger.info(f"  Messages consumed: {stats['messages_consumed']}")
        logger.info(f"  Messages processed: {stats['messages_processed']}")
        logger.info(f"  Messages failed: {stats['messages_failed']}")
        logger.info(f"  Messages published: {stats['messages_published']}")


if __name__ == "__main__":
    asyncio.run(main())
