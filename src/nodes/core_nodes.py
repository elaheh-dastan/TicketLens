"""
Core built-in nodes for the agent framework.

These are the fundamental node types that form the building blocks of agents:
- GeneratorNode: Calls an LLM to generate responses
- RouterNode: Routes based on conditional logic
- EndNode: Terminal node that signals completion
- PassthroughNode: Simple node that passes through state
"""

from typing import Optional, Dict, Any, Callable
import logging
import asyncio
import time

from .base_node import BaseNode
import importlib
from jinja2 import Environment, FileSystemLoader, Template
import os

from src.metrics.registry import get_metrics, classify_llm_error

logger = logging.getLogger(__name__)

# Module-level template cache to avoid recreating Jinja2 environment on every request
_template_cache: Dict[str, Template] = {}
_template_lock = asyncio.Lock()


class GeneratorNode(BaseNode):
    """
    Node that generates content using an LLM.
    
    The generator node is the workhorse of agent frameworks. It:
    1. Extracts prompt template and parameters from state
    2. Calls the LLM with the rendered prompt
    3. Returns the LLM response in the state
    
    Configuration:
        - llm: The language model to use (injected at runtime)
        - prompt_key: State key containing the prompt or prompt template
        - output_key: State key to store the response (default: "output")
        - temperature: LLM temperature (optional)
        - max_tokens: Max tokens to generate (optional)
    
    Example usage in YAML:
        nodes:
          generate:
            type: generator
            llm_model: openai/gpt-4o-mini
            prompt_key: current_prompt
            output_key: llm_response
    """
    
    def __init__(
        self,
        name: str = "generator",
        prompt_key: str = "messages",
        output_key: str = "output",
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        model: Optional[str] = None,
        provider: Optional[str] = None,
        tools: Optional[list] = None,
        response_format: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize the generator node.
        Args:
            name: Node name
            prompt_key: Key in state containing prompt or messages
            output_key: Key to store the response
            temperature: LLM temperature (optional)
            max_tokens: Max tokens to generate (optional)
            model: (optional) Override model for this node
            provider: (optional) Override provider for this node
            tools: (optional) List of tools to bind to the LLM
            response_format: (optional) Response format (e.g., {'type': 'json_object'})
        """
        super().__init__(name)
        self.prompt_key = prompt_key
        self.output_key = output_key
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.model = model
        self.provider = provider
        self.tools = tools or []
        self.response_format = response_format
        self.llm = None  # Injected by factory
        self._middleware_config = {}  # Middleware config injected by factory
    
    async def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a response using the LLM.

        Args:
            state: Graph state containing prompt in self.prompt_key

        Returns:
            Updated state with LLM response in self.output_key
        """
        logger.info(f"Executing GeneratorNode: {self.name}")
        
        # Langfuse tracing — use the parent span created by BaseNode.__call__()
        # instead of creating a separate trace (which would produce duplicates).
        langfuse_parent = None
        try:
            from src.utils.langfuse_client import LANGFUSE_TRACE_KEY

            langfuse_parent = state.get(LANGFUSE_TRACE_KEY)
        except Exception as e:
            logger.warning(
                f"Failed to get Langfuse parent span for GeneratorNode '{self.name}': {e}"
            )
        
        # LLM initialization span
        llm_init_start = time.time()
        if self.llm is None:
            # Lazily create an LLM client if not injected by the factory. This
            # avoids synchronous event-loop nesting when AgentFactory is used
            # from an async test harness. The factory may inject a pre-wrapped
            # client; if not, create one using the global LLM factory.
            try:
                llm_factory = importlib.import_module("src.llm.factory")
                
                # Build kwargs, only including response_format if it's not None
                llm_kwargs = {
                    "provider": self.provider,
                    "model": self.model
                }
                if self.response_format is not None:
                    llm_kwargs["response_format"] = self.response_format
                
                self.llm = await llm_factory.create_llm_client(**llm_kwargs)
                
                # Apply middleware if configured
                if self._middleware_config.get('model_retry'):
                    from src.middleware import middlewares as mw
                    retries = self._middleware_config.get('model_retry_retries', 3)
                    # Wrap the ainvoke method with retry middleware
                    original_ainvoke = self.llm.ainvoke
                    wrapped_ainvoke = mw.model_retry(retries=retries)(original_ainvoke)
                    # Create a wrapper object that has the wrapped ainvoke method
                    class WrappedLLM:
                        def __init__(self, original_llm, wrapped_ainvoke):
                            self._original_llm = original_llm
                            self.ainvoke = wrapped_ainvoke
                            
                        def __getattr__(self, name):
                            # Delegate all other attributes to the original LLM
                            return getattr(self._original_llm, name)
                    
                    self.llm = WrappedLLM(self.llm, wrapped_ainvoke)
                    
            except Exception as e:
                logger.exception(f"Failed to create LLM for node '{self.name}': {e}")
                raise RuntimeError(f"GeneratorNode '{self.name}' has no LLM configured and lazy creation failed: {e}")
        
        llm_init_time = time.time() - llm_init_start
        logger.debug(f"LLM initialization took {llm_init_time:.2f}s")
        
        # Get prompt/messages from state
        # If prompt_key ends with .jinja2, treat it as a template path and render it
        prompt_render_start = time.time()
        if self.prompt_key.endswith(".jinja2"):
            async with _template_lock:
                if self.prompt_key not in _template_cache:
                    # Look for templates in the prompts/ directory relative to the current working directory
                    prompts_dir = os.path.join(os.getcwd(), "prompts")
                    # If prompts directory doesn't exist, fall back to current working directory
                    if not os.path.exists(prompts_dir):
                        prompts_dir = os.getcwd()

                    env = Environment(loader=FileSystemLoader(prompts_dir))
                    try:
                        _template_cache[self.prompt_key] = env.get_template(self.prompt_key)
                        logger.info(f"Cached template: {self.prompt_key}")
                    except Exception as e:
                        logger.error(f"Failed to load template '{self.prompt_key}' in search path: {prompts_dir}: {e}")
                        raise ValueError(f"Failed to load template '{self.prompt_key}' in search path: {prompts_dir}: {e}")

                template = _template_cache[self.prompt_key]

            try:
                logger.info(f"Rendering template: {self.prompt_key}")
                logger.info(f"Available state keys for template: {list(state.keys())}")
                prompt = template.render(**state)
                logger.info(f"Rendered prompt (first 200 chars): {prompt[:200]}")
            except Exception as e:
                logger.error(f"Failed to render template '{self.prompt_key}': {e}")
                raise ValueError(f"Failed to render template '{self.prompt_key}': {e}")
        else:
            prompt = state.get(self.prompt_key)
            if prompt is None:
                raise ValueError(f"State missing key '{self.prompt_key}' required for generation")
        
        prompt_render_time = time.time() - prompt_render_start
        logger.debug(f"Prompt rendering took {prompt_render_time:.2f}s")
        
        logger.info(f"Generating with prompt key: {self.prompt_key}")
        
        # Bind tools if available
        llm_to_use = self.llm
        if self.tools:
            # If tools are strings, we need to resolve them to actual tool objects
            # But here we expect self.tools to be a list of tool objects or names?
            # The factory should probably resolve them or we resolve them here if we have access to tool registry.
            # However, GeneratorNode doesn't have access to tool registry.
            # So the factory should pass the resolved tools.
            # Let's assume self.tools contains the actual tool objects (LangChain tools).
            llm_to_use = self.llm.bind_tools(self.tools)

        # Call LLM (ainvoke handles async)
        logger.info(f"Calling LLM for node '{self.name}'...")
        llm_call_start = time.time()
        
        try:
            # Add timeout to prevent hanging
            response = await asyncio.wait_for(
                llm_to_use.ainvoke(prompt),
                timeout=300.0  # 5 minute timeout
            )
            llm_call_time = time.time() - llm_call_start
            logger.info(f"LLM call completed in {llm_call_time:.2f}s")

            # Prometheus metrics for successful LLM call
            try:
                _m = get_metrics()
                if _m:
                    _model_label = self.model or getattr(self.llm, "model_name", "unknown")
                    _m.llm_calls_total.labels(node_name=self.name, model=_model_label, status="success").inc()
                    _m.llm_call_duration_seconds.labels(node_name=self.name, model=_model_label).observe(llm_call_time)
                    _usage = getattr(response, "usage_metadata", None)
                    if _usage:
                        _m.llm_tokens_used_total.labels(node_name=self.name, model=_model_label, token_type="input").inc(_usage.get("input_tokens", 0))
                        _m.llm_tokens_used_total.labels(node_name=self.name, model=_model_label, token_type="output").inc(_usage.get("output_tokens", 0))
            except Exception:
                pass

            # Create Langfuse generation observation under the parent span
            if langfuse_parent:
                try:
                    # Get model name — strip provider prefix for Langfuse pricing
                    # (e.g. "openai/gpt-4o-mini" → "gpt-4o-mini")
                    model_name = self.model or getattr(
                        self.llm, "model_name", "unknown"
                    )
                    rm = getattr(response, "response_metadata", None) or {}
                    model_name = rm.get("model_name") or rm.get("model") or model_name
                    if "/" in model_name:
                        model_name = model_name.split("/", 1)[1]

                    prompt_text = str(prompt)[:10000] if prompt else ""
                    response_text = str(response)[:10000] if response else ""

                    # Extract token usage from LangChain response for cost tracking
                    usage = None
                    usage_meta = getattr(response, "usage_metadata", None)
                    if usage_meta:
                        usage = {
                            "input": usage_meta.get("input_tokens", 0),
                            "output": usage_meta.get("output_tokens", 0),
                            "total": usage_meta.get("total_tokens", 0),
                        }
                    elif rm:
                        token_usage = rm.get("token_usage") or rm.get("usage", {})
                        if token_usage:
                            usage = {
                                "input": token_usage.get("prompt_tokens", 0),
                                "output": token_usage.get("completion_tokens", 0),
                                "total": token_usage.get("total_tokens", 0),
                            }

                    generation = langfuse_parent.start_generation(
                        name=f"llm_call:{self.name}",
                        model=model_name,
                        input=prompt_text,
                        output=response_text,
                        usage_details=usage,
                        metadata={
                            "node_name": self.name,
                            "prompt_key": self.prompt_key,
                            "output_key": self.output_key,
                            "llm_init_time_seconds": llm_init_time,
                            "prompt_render_time_seconds": prompt_render_time,
                            "llm_call_time_seconds": llm_call_time,
                            "has_tools": len(self.tools) > 0 if self.tools else False,
                            "tool_count": len(self.tools) if self.tools else 0,
                        },
                    )
                    generation.end()
                except Exception as e:
                    logger.warning(
                        f"Failed to create Langfuse generation observation: {e}"
                    )
            
            # Extract content from response
            content = response
            if hasattr(response, 'content'):
                content = response.content
                
            # Try to parse JSON if it looks like JSON
            if isinstance(content, str):
                content = content.strip()
                # Remove markdown code blocks if present
                if content.startswith("```json"):
                    content = content[7:]
                if content.startswith("```"):
                    content = content[3:]
                if content.endswith("```"):
                    content = content[:-3]
                content = content.strip()
                
                if (content.startswith("{") and content.endswith("}")) or \
                   (content.startswith("[") and content.endswith("]")):
                    try:
                        import json
                        content = json.loads(content)
                        logger.info("Successfully parsed JSON response")
                    except json.JSONDecodeError:
                        logger.warning("Failed to parse JSON response, returning raw string")

            # Return the response
            if isinstance(content, (dict, list)):
                logger.info(f"Generated structured response with {len(content)} items")
            else:
                logger.info(f"Generated response (first 200 chars): {str(content)[:200]}")
            
            logger.info(f"Storing response in state key: '{self.output_key}'")
            
            # Update Langfuse parent span with output
            if langfuse_parent:
                try:
                    langfuse_parent.update(output={self.output_key: str(content)[:1000]})
                except Exception as e:
                    logger.warning(f"Failed to update Langfuse span output: {e}")
            
            # Flatten dict responses to top-level state (similar to DSPyNode behavior)
            # This allows JSON responses with multiple fields to be accessible as separate state keys
            result = {}
            if isinstance(content, dict):
                # Flatten dict fields to top-level state
                result.update(content)
                logger.info(f"Flattened dict response to top-level state keys: {list(content.keys())}")
                
                # DEBUG: Log specific routing keys
                if "has_tool_calls" in content:
                    logger.warning(f"[DEBUG] has_tool_calls found in response: {content['has_tool_calls']} (type: {type(content['has_tool_calls']).__name__})")
                if "test_complete" in content:
                    logger.warning(f"[DEBUG] test_complete found in response: {content['test_complete']} (type: {type(content['test_complete']).__name__})")
                if "tool_calls" in content:
                    logger.warning(f"[DEBUG] tool_calls found in response: {len(content['tool_calls'])} items")
            else:
                logger.warning(f"[DEBUG] Content is not a dict, type={type(content).__name__}, content={str(content)[:200]}")
            
            # Also store under output_key for backward compatibility
            result[self.output_key] = content
            
            return result
            
        except asyncio.TimeoutError:
            llm_call_time = time.time() - llm_call_start
            logger.error(f"LLM call timed out after {llm_call_time:.2f}s (max 300s)")
            try:
                _m = get_metrics()
                if _m:
                    _model_label = self.model or getattr(self.llm, "model_name", "unknown")
                    _m.llm_calls_total.labels(node_name=self.name, model=_model_label, status="timeout").inc()
                    _m.llm_call_errors_total.labels(
                        node_name=self.name,
                        model=_model_label,
                        error_type="TimeoutError",
                        reason="timeout",
                    ).inc()
                    _m.llm_call_duration_seconds.labels(node_name=self.name, model=_model_label).observe(llm_call_time)
            except Exception:
                pass
            raise RuntimeError(f"LLM call for node '{self.name}' timed out after 300 seconds")
        except Exception as e:
            llm_call_time = time.time() - llm_call_start
            logger.error(f"LLM call failed after {llm_call_time:.2f}s: {e}", exc_info=True)
            try:
                _m = get_metrics()
                if _m:
                    _model_label = self.model or getattr(self.llm, "model_name", "unknown")
                    _m.llm_calls_total.labels(node_name=self.name, model=_model_label, status="error").inc()
                    _m.llm_call_errors_total.labels(
                        node_name=self.name,
                        model=_model_label,
                        error_type=type(e).__name__,
                        reason=classify_llm_error(e),
                    ).inc()
                    _m.llm_call_duration_seconds.labels(node_name=self.name, model=_model_label).observe(llm_call_time)
            except Exception:
                pass
            
            # Update Langfuse with error
            if langfuse_parent:
                try:
                    langfuse_parent.update(
                        level="ERROR",
                        status_message=str(e),
                        metadata={
                            "error_type": type(e).__name__,
                            "llm_call_time_seconds": llm_call_time,
                        },
                    )
                except Exception as langfuse_error:
                    logger.warning(
                        f"Failed to update Langfuse span with error: {langfuse_error}"
                    )
            
            raise


class RouterNode(BaseNode):
    """
    Node that routes to different branches based on state.
    
    The router node implements conditional logic without LLM calls. It:
    1. Evaluates a condition based on state
    2. Returns routing information
    
    The routing decision is then handled by conditional_edges in the graph.
    
    Example usage in YAML:
        nodes:
          route:
            type: router
            routes:
              - condition: "state['confidence'] > 0.8"
                next: "approve"
              - condition: "true"
                next: "review"
    """
    
    def __init__(
        self,
        name: str = "router",
        route_func: Optional[Callable[[Dict[str, Any]], str]] = None,
    ):
        """
        Initialize the router node.
        
        Args:
            name: Node name
            route_func: Function that takes state and returns next node name
        """
        super().__init__(name)
        self.route_func = route_func
    
    async def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Route based on state.

        This is typically used with conditional_edges in the graph.
        The return value should indicate which node to route to.

        Args:
            state: Current graph state

        Returns:
            Updated state with routing information
        """
        logger.info(f"Executing RouterNode: {self.name}")
        if self.route_func is None:
            raise RuntimeError(f"RouterNode '{self.name}' has no route function")
        
        result = self.route_func(state)
        logger.debug(f"Routing decision: {result}")
        
        return {"route": result}


class EndNode(BaseNode):
    """
    Terminal node that signals end of execution.
    
    The end node doesn't do any processing. It's used as a final step
    before the END token in the graph.
    
    This is typically used to:
    1. Extract final output
    2. Format response
    3. Log completion
    """
    
    def __init__(self, name: str = "end", output_key: str = "output"):
        """
        Initialize the end node.
        
        Args:
            name: Node name
            output_key: Key to extract as final output
        """
        super().__init__(name)
        self.output_key = output_key
    
    async def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process final state before ending.

        Args:
            state: Final graph state

        Returns:
            Final state updates
        """
        logger.info(f"Executing EndNode: {self.name}")
        logger.debug(f"Ending execution with output key: {self.output_key}")
        return {}  # No updates needed, just terminal marker


class PassthroughNode(BaseNode):
    """
    Simple node that passes state through unchanged.
    
    Used for:
    - Testing graph structure
    - Logging/observability
    - Placeholder nodes
    """
    
    def __init__(self, name: str = "passthrough"):
        """
        Initialize the passthrough node.
        
        Args:
            name: Node name
        """
        super().__init__(name)
    
    async def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Pass state through unchanged.

        Args:
            state: Current graph state

        Returns:
            Empty dict (no changes)
        """
        logger.info(f"Executing PassthroughNode: {self.name}")
        logger.debug(f"Passthrough node: {self.name}")
        return {}


class ToolExecutorNode(BaseNode):
    """
    Node that executes a named tool (function) and records the result.
    
    Can operate in two modes:
    1. Single tool mode: Executes a specific tool configured via tool_name
    2. Multi-tool mode: Executes a tool specified in the state (e.g. from LLM tool call)

    Configuration:
      - tool_name: name of the tool to call (optional)
      - tools: dict mapping tool names to callables (optional, for multi-tool mode)
      - input_key: state key containing parameters for the tool (dict)
      - output_key: state key to store the tool result
    """

    def __init__(
        self,
        name: str = "tool_executor",
        tool_name: str | None = None,
        tools: Dict[str, Any] | None = None,
        input_key: str = "tool_input",
        output_key: str = "tool_result"
    ):
        super().__init__(name)
        self.tool_name = tool_name
        self.tools = tools or {}
        self.input_key = input_key
        self.output_key = output_key
        self.tool = None  # Callable to be injected by factory/runtime (for single tool mode)

    async def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        logger.info(f"ToolExecutorNode '{self.name}': Starting execution")
        logger.info(f"ToolExecutorNode '{self.name}': Available tools: {list(self.tools.keys())}")
        logger.info(f"ToolExecutorNode '{self.name}': State keys: {list(state.keys())}")
        
        # Check if we're in multi-tool mode (handling tool calls from LLM)
        # This expects the state to contain a 'tool_calls' list or similar structure
        # For now, let's support the simple case where input_key contains the tool call info
        
        # If we have a specific tool configured, use it (Single Tool Mode)
        if self.tool is not None:
            logger.info(f"ToolExecutorNode '{self.name}': Executing single tool '{self.tool_name}'")
            params = state.get(self.input_key, {})
            
            if not isinstance(params, dict):
                # allow single param value - pass directly as first argument
                result = await self.tool(params)
            else:
                result = await self.tool(**params)
                
            return {self.output_key: result}
            
        # Multi-tool mode: Look for tool calls in state
        # We expect the state to contain 'tool_calls' or the input_key to contain tool execution info
        # Based on standard LangChain/LLM patterns, tool_calls are usually in the message or a specific key
        
        # Let's check if we have a 'tool_calls' key in state (common pattern)
        tool_calls = state.get("tool_calls", [])
        logger.info(f"ToolExecutorNode '{self.name}': tool_calls from state: {tool_calls}")
        
        if not tool_calls and self.input_key in state:
            # Try to get from input_key if it looks like a tool call
            input_data = state.get(self.input_key)
            logger.info(f"ToolExecutorNode '{self.name}': Checking input_key '{self.input_key}': {type(input_data).__name__}")
            
            if isinstance(input_data, list):
                tool_calls = input_data
                logger.info(f"ToolExecutorNode '{self.name}': Found tool_calls list in input_key: {len(tool_calls)} items")
            elif isinstance(input_data, dict):
                # Check if input_data itself contains tool_calls
                if "tool_calls" in input_data:
                    tool_calls = input_data["tool_calls"]
                    logger.info(f"ToolExecutorNode '{self.name}': Found tool_calls in input_data dict: {len(tool_calls)} items")
                elif "name" in input_data:
                    tool_calls = [input_data]
                    logger.info(f"ToolExecutorNode '{self.name}': Treating input_data as single tool call")
                
        if not tool_calls:
            logger.warning(f"ToolExecutorNode '{self.name}': No tool calls found in state")
            logger.warning(f"ToolExecutorNode '{self.name}': Checked 'tool_calls' key and input_key '{self.input_key}'")
            return {}
            
        results = []
        for call in tool_calls:
            # Handle different tool call formats
            # 1. OpenAI format: {'function': {'name': '...', 'arguments': '...'}}
            # 2. Simple format: {'name': '...', 'args': {...}}
            # 3. LangChain format: ToolCall object
            
            tool_name = None
            tool_args = {}
            call_id = None
            
            if hasattr(call, "name"): # Object access
                tool_name = call.name
                tool_args = call.args if hasattr(call, "args") else {}
                call_id = getattr(call, "id", None)
            elif isinstance(call, dict):
                if "function" in call: # OpenAI format
                    tool_name = call["function"].get("name")
                    import json
                    args_str = call["function"].get("arguments", "{}")
                    try:
                        tool_args = json.loads(args_str) if isinstance(args_str, str) else args_str
                    except:
                        tool_args = {}
                    call_id = call.get("id")
                else: # Simple format
                    tool_name = call.get("name")
                    tool_args = call.get("args", {})
                    call_id = call.get("id")
            
            if not tool_name or tool_name not in self.tools:
                logger.warning(f"Tool '{tool_name}' not found in available tools: {list(self.tools.keys())}")
                results.append({
                    "tool_call_id": call_id,
                    "output": f"Error: Tool '{tool_name}' not found",
                    "name": tool_name
                })
                continue
                
            # Execute the tool
            try:
                tool_func = self.tools[tool_name]
                logger.info(f"Executing tool '{tool_name}' with args: {tool_args}")
                
                # Handle async tools
                if asyncio.iscoroutinefunction(tool_func) or (hasattr(tool_func, "ainvoke") and asyncio.iscoroutinefunction(tool_func.ainvoke)):
                    if hasattr(tool_func, "ainvoke"):
                        tool_result = await tool_func.ainvoke(tool_args)
                    else:
                        tool_result = await tool_func(**tool_args)
                else:
                    # Sync tool
                    if hasattr(tool_func, "invoke"):
                        tool_result = tool_func.invoke(tool_args)
                    else:
                        tool_result = tool_func(**tool_args)
                        
                results.append({
                    "tool_call_id": call_id,
                    "output": tool_result,
                    "name": tool_name
                })
                logger.info(f"Tool '{tool_name}' execution successful")
                
            except Exception as e:
                logger.error(f"Error executing tool '{tool_name}': {e}", exc_info=True)
                results.append({
                    "tool_call_id": call_id,
                    "output": f"Error executing tool: {str(e)}",
                    "name": tool_name
                })

        # Return results
        # If we have a single result and output_key is specified, we might want to store it there
        # But for multi-tool execution, we usually want to store the list of messages/results
        
        return {
            self.output_key: results,
            "messages": results # Append to messages for chat history
        }


class ValidatorNode(BaseNode):
    """
    Node that validates parts of state against a simple schema.

    The schema is a dict mapping keys to expected Python type names (e.g. 'str', 'int').
    This is intentionally minimal for v0 and can be replaced with jsonschema or pydantic later.
    """

    TYPE_MAP = {
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
        "list": list,
        "dict": dict,
    }

    def __init__(self, name: str = "validator", schema: dict | None = None, input_key: str = "input"):
        super().__init__(name)
        self.schema = schema or {}
        self.input_key = input_key

    async def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        logger.info(f"Executing ValidatorNode: {self.name}")
        data = state.get(self.input_key, {})
        errors = []

        for key, expected in (self.schema or {}).items():
            expected_type = self.TYPE_MAP.get(expected, None)
            val = data.get(key) if isinstance(data, dict) else None
            if expected_type is None:
                # unknown expected type, skip
                continue
            if val is None:
                errors.append(f"missing:{key}")
            elif not isinstance(val, expected_type):
                errors.append(f"type:{key}:{type(val).__name__}!={expected}")

        valid = len(errors) == 0
        logger.debug(f"Validation result for node '{self.name}': valid={valid}, errors={errors}")
        return {"validation": {"valid": valid, "errors": errors}}


class ParallelNode(BaseNode):
    """
    Node for parallel execution of multiple branches.

    Executes multiple node paths concurrently using asyncio.gather()
    and merges their results into the state.

    Configuration:
        - branches: List of node names to execute in parallel
        - merge_strategy: How to merge results (default: "update")
    """

    def __init__(
        self,
        name: str = "parallel_node",
        branches: Optional[list[str]] = None,
        merge_strategy: str = "update"
    ):
        """
        Initialize the parallel node.

        Args:
            name: Node name
            branches: List of node names to execute in parallel
            merge_strategy: Strategy for merging results ("update", "extend", "replace")
        """
        super().__init__(name)
        self.branches = branches or []
        self.merge_strategy = merge_strategy

    async def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute multiple branches in parallel.

        This node doesn't execute the branches itself - it just signals
        that the specified branches should be executed concurrently.
        The actual parallel execution is handled by the graph orchestration.

        Args:
            state: Current graph state

        Returns:
            Updated state (typically unchanged, as branches handle their own outputs)
        """
        logger.info(f"Executing ParallelNode: {self.name} with branches: {self.branches}")

        # In LangGraph, parallel execution is typically handled at the graph level
        # by defining conditional edges or using the parallel execution features.
        # This node serves as a coordination point.

        # For now, we'll just log and pass through - the actual parallel execution
        # should be configured in the graph structure using LangGraph's features.

        logger.debug(f"Parallel execution coordination for branches: {self.branches}")

        # Return empty dict - the parallel branches will update the state directly
        return {}


class ScoreCalculatorNode(BaseNode):
    """
    Node that computes a final score from sub-scores using a weighted formula.

    Reads numeric sub-score fields from state, applies configured weights,
    and writes the result to the output key.
    """

    def __init__(
        self,
        name: str = "score_calculator",
        formula: Optional[Dict[str, float]] = None,
        output_key: str = "score",
    ):
        super().__init__(name)
        self.formula = formula or {}
        self.output_key = output_key

    async def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        total = 0.0
        for field, weight in self.formula.items():
            raw = state.get(field)
            try:
                value = float(raw)
            except (TypeError, ValueError):
                logger.warning(
                    f"ScoreCalculatorNode '{self.name}': "
                    f"could not parse '{field}' value '{raw}' as float, using 0"
                )
                value = 0.0
            total += weight * value

        score = round(total, 1)
        logger.info(f"ScoreCalculatorNode '{self.name}': computed {self.output_key}={score}")
        return {self.output_key: str(score)}
