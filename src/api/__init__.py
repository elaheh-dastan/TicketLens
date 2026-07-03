"""HTTP API for submitting QC tickets over HTTP instead of Kafka."""

from .app import create_app

__all__ = ["create_app"]
