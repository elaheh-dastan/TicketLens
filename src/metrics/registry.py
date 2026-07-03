"""
Centralized Prometheus metrics definitions.

All metrics are defined here as a single source of truth. Use get_metrics()
to obtain the metrics singleton — returns None when metrics are disabled.
"""

import os
from dataclasses import dataclass
from typing import Optional

from prometheus_client import Counter, Gauge, Histogram


HTTP_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
AGENT_BUCKETS = (0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0)

_multiprocess = bool(os.environ.get("PROMETHEUS_MULTIPROC_DIR"))


@dataclass(frozen=True)
class Metrics:
    # HTTP
    http_requests_total: Counter
    http_request_duration_seconds: Histogram
    http_requests_in_progress: Gauge

    # Tasks
    tasks_created_total: Counter
    tasks_completed_total: Counter
    task_duration_seconds: Histogram
    tasks_in_progress: Gauge

    # Kafka
    kafka_messages_produced_total: Counter
    kafka_produce_errors_total: Counter
    kafka_messages_consumed_total: Counter
    kafka_consume_errors_total: Counter

    # Node execution
    node_executions_total: Counter
    node_execution_duration_seconds: Histogram

    # LLM
    llm_calls_total: Counter
    llm_call_errors_total: Counter
    llm_call_duration_seconds: Histogram
    llm_tokens_used_total: Counter


_metrics: Optional[Metrics] = None


def classify_llm_error(exc: BaseException) -> str:
    """Classify an LLM exception into a coarse, bounded `reason` label.

    Returned values are kept to a small, stable set so cardinality stays low:
    quota_exceeded, rate_limited, auth, timeout, connection, bad_request,
    server_error, other.
    """
    status = getattr(exc, "status_code", None) or getattr(exc, "code", None)
    try:
        status = int(status) if status is not None else None
    except (TypeError, ValueError):
        status = None

    msg = str(exc).lower()

    if status == 429 or "rate limit" in msg or "too many requests" in msg:
        return "rate_limited"
    if status == 403 and ("limit" in msg or "quota" in msg or "exceeded" in msg):
        return "quota_exceeded"
    if "quota" in msg or "key limit exceeded" in msg or "insufficient_quota" in msg:
        return "quota_exceeded"
    if status in (401, 403):
        return "auth"
    if isinstance(exc, TimeoutError) or "timeout" in msg or "timed out" in msg:
        return "timeout"
    if "connection" in msg or "connect" in msg or "network" in msg:
        return "connection"
    if status == 400 or "bad request" in msg:
        return "bad_request"
    if status is not None and 500 <= status < 600:
        return "server_error"
    return "other"


def get_metrics() -> Optional[Metrics]:
    """Return the metrics singleton, or None if metrics are disabled."""
    global _metrics
    if _metrics is not None:
        return _metrics

    try:
        from src.config.settings import get_settings
        if not get_settings().metrics.enabled:
            return None
    except Exception:
        return None

    _metrics = Metrics(
        # HTTP
        http_requests_total=Counter(
            "ai_support_qc_http_requests_total",
            "Total HTTP requests",
            ["method", "endpoint", "status_code"],
        ),
        http_request_duration_seconds=Histogram(
            "ai_support_qc_http_request_duration_seconds",
            "HTTP request duration in seconds",
            ["method", "endpoint"],
            buckets=HTTP_BUCKETS,
        ),
        http_requests_in_progress=Gauge(
            "ai_support_qc_http_requests_in_progress",
            "HTTP requests currently in progress",
            ["method"],
            multiprocess_mode="livesum",
        ),
        # Tasks
        tasks_created_total=Counter(
            "ai_support_qc_tasks_created_total",
            "Total tasks created",
            ["agent_name"],
        ),
        tasks_completed_total=Counter(
            "ai_support_qc_tasks_completed_total",
            "Total tasks completed",
            ["agent_name", "status"],
        ),
        task_duration_seconds=Histogram(
            "ai_support_qc_task_duration_seconds",
            "Task duration in seconds",
            ["agent_name", "status"],
            buckets=AGENT_BUCKETS,
        ),
        tasks_in_progress=Gauge(
            "ai_support_qc_tasks_in_progress",
            "Tasks currently in progress",
            ["agent_name"],
            multiprocess_mode="livesum",
        ),
        # Kafka
        kafka_messages_produced_total=Counter(
            "ai_support_qc_kafka_messages_produced_total",
            "Total Kafka messages produced",
            ["topic"],
        ),
        kafka_produce_errors_total=Counter(
            "ai_support_qc_kafka_produce_errors_total",
            "Total Kafka produce errors",
            ["topic"],
        ),
        kafka_messages_consumed_total=Counter(
            "ai_support_qc_kafka_messages_consumed_total",
            "Total Kafka messages consumed",
            ["topic"],
        ),
        kafka_consume_errors_total=Counter(
            "ai_support_qc_kafka_consume_errors_total",
            "Total Kafka consume errors",
            ["topic"],
        ),
        # Node execution
        node_executions_total=Counter(
            "ai_support_qc_node_executions_total",
            "Total node executions",
            ["node_name", "node_type", "status"],
        ),
        node_execution_duration_seconds=Histogram(
            "ai_support_qc_node_execution_duration_seconds",
            "Node execution duration in seconds",
            ["node_name", "node_type"],
            buckets=AGENT_BUCKETS,
        ),
        # LLM
        llm_calls_total=Counter(
            "ai_support_qc_llm_calls_total",
            "Total LLM calls",
            ["node_name", "model", "status"],
        ),
        llm_call_errors_total=Counter(
            "ai_support_qc_llm_call_errors_total",
            "Total LLM call errors",
            ["node_name", "model", "error_type", "reason"],
        ),
        llm_call_duration_seconds=Histogram(
            "ai_support_qc_llm_call_duration_seconds",
            "LLM call duration in seconds",
            ["node_name", "model"],
            buckets=AGENT_BUCKETS,
        ),
        llm_tokens_used_total=Counter(
            "ai_support_qc_llm_tokens_used_total",
            "Total LLM tokens used",
            ["node_name", "model", "token_type"],
        ),
    )
    return _metrics
