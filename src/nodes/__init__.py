"""
Node system for LangGraph agent framework.

Nodes are the core building blocks of agents. Each node is a function that:
1. Receives the current state
2. Performs some computation or I/O
3. Returns updated state

This module provides:
- BaseNode: Abstract base class for all nodes
- CoreNodes: Generator, Router, End nodes
- Node registry for dynamic loading
"""

from .base_node import BaseNode
from .core_nodes import (
    GeneratorNode,
    RouterNode,
    EndNode,
    PassthroughNode,
)

__all__ = [
    "BaseNode",
    "GeneratorNode",
    "RouterNode",
    "EndNode",
    "PassthroughNode",
]
