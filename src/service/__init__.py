"""Transport-agnostic QC service layer.

Wraps the QC agent so it can be driven from any transport (Kafka, HTTP, ...)
without duplicating input/output transformation or graph-building logic.
"""

from .qc_processor import QCProcessor

__all__ = ["QCProcessor"]
