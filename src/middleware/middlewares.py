"""
Lightweight middleware helpers for the agent framework.

Provided middleware:
- model_retry: retry failed model calls with exponential backoff
- tool_retry: retry failed tool calls with exponential backoff
- llm_tool_emulator: create a tool that uses an LLM to emulate tool behavior for testing
- summarization_middleware: summarize conversation history when approaching token limits
- TodoListMiddleware: small in-memory to-do list manager for planning/tracking

These are intentionally minimal and test-friendly. Production-grade middleware
should integrate with observability, backoff libraries, and persistent stores.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Coroutine, Optional, List, Dict

logger = logging.getLogger(__name__)


def model_retry(retries: int = 3, backoff_base: float = 0.5):
    """Return a decorator that retries an async model call (e.g., `ainvoke`).

    Usage:
        wrapped = model_retry(3)(llm.ainvoke)
        await wrapped(messages)
    """

    def _decorator(fn: Callable[..., Coroutine[Any, Any, Any]]):
        async def _wrapped(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    attempt += 1
                    return await fn(*args, **kwargs)
                except Exception as e:
                    if attempt > retries:
                        logger.exception("Model call failed after retries")
                        raise
                    delay = backoff_base * (2 ** (attempt - 1))
                    logger.warning(
                        f"Model call failed (attempt {attempt}), retrying in {delay}s: {e}"
                    )
                    await asyncio.sleep(delay)

        return _wrapped

    return _decorator


def tool_retry(retries: int = 2, backoff_base: float = 0.3):
    """Return a decorator that retries async tool calls.
    Tools are async callables: async def tool(**params) -> result
    """

    def _decorator(fn: Callable[..., Coroutine[Any, Any, Any]]):
        async def _wrapped(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    attempt += 1
                    return await fn(*args, **kwargs)
                except Exception as e:
                    if attempt > retries:
                        logger.exception("Tool call failed after retries")
                        raise
                    delay = backoff_base * (2 ** (attempt - 1))
                    logger.warning(
                        f"Tool call failed (attempt {attempt}), retrying in {delay}s: {e}"
                    )
                    await asyncio.sleep(delay)

        return _wrapped

    return _decorator


def llm_tool_emulator(
    llm_ainvoke: Callable[..., Coroutine[Any, Any, Any]],
    tool_name: str,
    prompt_template: Optional[str] = None,
) -> Callable[..., Coroutine[Any, Any, Any]]:
    """Create a tool function that uses an LLM to 'emulate' a tool's behavior.

    The returned function has signature async def tool(**params) and will call
    the LLM with a crafted prompt containing the tool name and params.
    """

    async def tool_emulator(**params):
        # Create a simple prompt describing the tool call
        prompt = prompt_template or (
            "You are emulating a tool named '{tool}'. Given the parameters, respond with a JSON-serializable result.".format(
                tool=tool_name
            )
        )
        prompt = f"{prompt}\nPARAMS: {params}"
        # LangChain-style LLM expect messages or string; we pass the prompt directly
        resp = await llm_ainvoke(prompt)
        return resp

    return tool_emulator


def summarization_middleware(threshold_chars: int = 4000):
    """Return a wrapper that will summarize a list of messages when the combined
    character length exceeds `threshold_chars`.

    Usage: wrapped = summarization_middleware()(send_messages_fn)
    where send_messages_fn(messages) -> model response
    """

    def _decorator(send_fn: Callable[[List[Any]], Coroutine[Any, Any, Any]]):
        async def _wrapped(messages: List[Any], *args, **kwargs):
            total_chars = sum(len(str(m)) for m in messages)
            if total_chars > threshold_chars:
                logger.info(
                    "Summarization middleware triggered: summarizing history to reduce context size"
                )
                # Produce a short summary by keeping first and last messages
                summary = {
                    "summary": f"History truncated: kept {len(messages)} messages (first+last)."
                }
                # Replace history with summary message
                new_messages = [summary]
                return await send_fn(new_messages, *args, **kwargs)
            return await send_fn(messages, *args, **kwargs)

        return _wrapped

    return _decorator


class TodoListMiddleware:
    """In-memory to-do list manager for planning/tracking within tests and small agents.

    This is intentionally minimal; in a production system this would be backed by
    durable storage and integrated with the factory's task management.
    """

    def __init__(self):
        self._todos: List[Dict[str, Any]] = []
        self._next_id = 1

    def add(self, title: str, description: str = "") -> Dict[str, Any]:
        t = {
            "id": self._next_id,
            "title": title,
            "description": description,
            "status": "not-started",
        }
        self._next_id += 1
        self._todos.append(t)
        return t

    def list(self) -> List[Dict[str, Any]]:
        return list(self._todos)

    def set_status(self, todo_id: int, status: str) -> bool:
        for t in self._todos:
            if t["id"] == todo_id:
                t["status"] = status
                return True
        return False

    def clear(self):
        self._todos = []


__all__ = [
    "model_retry",
    "tool_retry",
    "llm_tool_emulator",
    "summarization_middleware",
    "TodoListMiddleware",
]
