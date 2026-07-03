"""
Minimal AgentFactory to build LangGraph StateGraph from YAML agent configs.

This is an initial implementation supporting basic sequential graphs:
- Loads YAML config and validates with `AgentConfig` from `src.config.app_config`.
- Maps node `type` to internal node classes (GeneratorNode, RouterNode, PassthroughNode).
- Adds nodes and edges to a `StateGraph` and compiles it.

Limitations for v0:
- Conditional routing is only supported if `next` is omitted and `conditions` present (basic mapping);
  complex condition expressions are not evaluated yet.
- Generator nodes require an LLM to be injected after creation; factory does not create LLM clients yet.

"""
from __future__ import annotations

import yaml
import logging
from pathlib import Path
from typing import Dict, Any
import aiofiles

from langgraph.graph import StateGraph, START, END

from ..agent.state import AgentState
from ..config.app_config import AgentConfig, NodeConfig
from src.nodes.core_nodes import GeneratorNode, RouterNode, PassthroughNode, ToolExecutorNode, ValidatorNode, ParallelNode, ScoreCalculatorNode
from src.nodes.dspy_node import DSPyNode
from src.llm.dspy_adapter import create_dspy_lm, configure_dspy_lm
import asyncio
from src.middleware import middlewares as mw_fallback
import importlib

logger = logging.getLogger(__name__)


class AgentFactory:
    """Factory to create LangGraph StateGraph from YAML config files."""

    NODE_TYPE_MAP = {
        "generator": GeneratorNode,
        "conditional": RouterNode,
        "router": RouterNode,
        "passthrough": PassthroughNode,
        "tool_executor": ToolExecutorNode,
        "validator": ValidatorNode,
        "parallel": ParallelNode,
        "dspy": DSPyNode,
        "score_calculator": ScoreCalculatorNode,
        # fallback: any other type -> Passthrough
        }

    def __init__(self, config_path: str | Path):
        self.config_path = Path(config_path)
        self.agent_config: AgentConfig | None = None
        self.graph: StateGraph | None = None
        self.mcp_manager = None  # Keep MCP client alive for tool execution

    async def load_config(self) -> AgentConfig:
        """Load and validate YAML config into `AgentConfig`.
        """
        logger.debug(f"Loading agent config from: {self.config_path}")
        async with aiofiles.open(self.config_path, 'r') as f:
            content = await f.read()
        raw = yaml.safe_load(content)
        # Expect top-level 'agent' key per ConfigFile model
        if "agent" in raw:
            raw = raw["agent"]
        self.agent_config = AgentConfig(**raw)
        logger.info(f"Loaded agent config: {self.agent_config.agent_name}")
        return self.agent_config


    def _create_node_instance(self, name: str, cfg: NodeConfig, tool_map: Dict[str, Any] = None):
        """Create a node instance for a NodeConfig, supporting per-node model/provider overrides."""
        if tool_map is None:
            tool_map = {}
        
        node_type = cfg.type.value if hasattr(cfg.type, "value") else str(cfg.type)
        cls = self.NODE_TYPE_MAP.get(node_type, PassthroughNode)

        # For RouterNode, create with a simple route function if conditions provided
        if cls is RouterNode and cfg.conditions:
            def route_func(state: Dict[str, Any]):
                # Check conditions in order - first match wins
                logger.warning(f"[DEBUG] Router evaluating state keys: {list(state.keys())}")
                for cond_key, next_node in (cfg.conditions or {}).items():
                    cond_value = state.get(cond_key)
                    logger.warning(f"[DEBUG] Router checking condition '{cond_key}': value={cond_value}, type={type(cond_value).__name__}")
                    
                    # Check if condition is truthy (non-empty, non-None, True)
                    if cond_value is None or cond_value == "":
                        logger.warning(f"[DEBUG] Condition '{cond_key}' skipped: value is None or empty string")
                        continue
                    
                    # For boolean values, check if True
                    if isinstance(cond_value, bool):
                        if cond_value:
                            logger.warning(f"[DEBUG] Router condition '{cond_key}' matched (boolean True), routing to '{next_node}'")
                            return next_node
                        else:
                            logger.warning(f"[DEBUG] Condition '{cond_key}' is boolean False, skipping")
                        continue
                    
                    # For lists/dicts, check if they have content
                    if isinstance(cond_value, (list, dict)):
                        if len(cond_value) > 0:
                            logger.warning(f"[DEBUG] Router condition '{cond_key}' matched (non-empty {type(cond_value).__name__}), routing to '{next_node}'")
                            return next_node
                        else:
                            logger.warning(f"[DEBUG] Condition '{cond_key}' is empty {type(cond_value).__name__}, skipping")
                        continue
                    
                    # For other truthy values (numbers, strings, etc.)
                    if cond_value:
                        logger.warning(f"[DEBUG] Router condition '{cond_key}' matched (truthy), routing to '{next_node}'")
                        return next_node
                    else:
                        logger.warning(f"[DEBUG] Condition '{cond_key}' is falsy, skipping")
                
                # Default to next if no conditions matched
                default_next = cfg.next if cfg.next and cfg.next != "__end__" else END
                logger.warning(f"[DEBUG] No router conditions matched, defaulting to '{default_next}'")
                return default_next
            return RouterNode(name=name, route_func=route_func)
        
        # For ToolExecutorNode, auto-populate tools from tool_map if not specified
        if cls is ToolExecutorNode:
            # Get tools from config or use all available tools
            if cfg.tools:
                # Filter tool_map to only include specified tools
                node_tools = {k: v for k, v in tool_map.items() if k in cfg.tools}
                logger.debug(f"ToolExecutorNode '{name}' using {len(node_tools)} specified tools")
            else:
                # Use all available tools
                node_tools = tool_map
                logger.debug(f"ToolExecutorNode '{name}' using all {len(node_tools)} available tools")
            return ToolExecutorNode(name=name, tools=node_tools, input_key=cfg.input_key, output_key=cfg.output_key)

        # For GeneratorNode, support per-node model/provider override
        if cls is GeneratorNode:
            prompt_key = cfg.prompt or "messages"
            output_key = cfg.output_key or "output"
            # Per-node model/provider (if present in cfg), else fallback to agent config
            model = getattr(cfg, "model", None)
            provider = getattr(cfg, "provider", None)
            response_format = getattr(cfg, "response_format", None)
            # Fallback to agent config if not set
            if self.agent_config:
                if not model:
                    model = self.agent_config.models.generator
                if not provider:
                    provider_obj = self.agent_config.models.provider
                    provider = provider_obj.value if hasattr(provider_obj, "value") else str(provider_obj)
            node = GeneratorNode(
                name=name,
                prompt_key=prompt_key,
                output_key=output_key,
                model=model,
                provider=provider,
                tools=cfg.tools, # Pass tool names initially
                response_format=response_format,
            )
            return node

        # Tool executor node
        if node_type == "tool_executor":
            tool_name = cfg.tools[0] if cfg.tools else None
            input_key = cfg.input_key or cfg.prompt or "tool_input"
            output_key = cfg.output_key or "tool_result"
            return ToolExecutorNode(name=name, tool_name=tool_name, input_key=input_key, output_key=output_key)

        # Validator node
        if node_type == "validator":
            schema = cfg.validation_schema or {}
            input_key = cfg.prompt or "input"
            return ValidatorNode(name=name, schema=schema, input_key=input_key)

        # Parallel node
        if node_type == "parallel":
            branches = getattr(cfg, "branches", [])
            merge_strategy = getattr(cfg, "merge_strategy", "update")
            return ParallelNode(
                name=name,
                branches=branches,
                merge_strategy=merge_strategy
            )

        # Score calculator node
        if node_type == "score_calculator":
            formula = getattr(cfg, "formula", None) or {}
            output_key = cfg.output_key or "score"
            return ScoreCalculatorNode(name=name, formula=formula, output_key=output_key)

        # DSPy node
        if node_type == "dspy":
            # Merge global and node-level DSPy config
            dspy_config = getattr(cfg, "dspy", None)
            if not dspy_config:
                # Try to get from agent-level config
                if self.agent_config and getattr(self.agent_config, "dspy", None):
                    dspy_config = self.agent_config.dspy
                else:
                    logger.warning(f"DSPy config missing for node '{name}', falling back to PassthroughNode")
                    return PassthroughNode(name=name)
            
            # Initialize DSPy LLM adapter if not already done
            try:
                from src.config.settings import get_settings
                settings = get_settings()
                provider = "openrouter"
                # Get model from agent config
                model = self.agent_config.models.generator if self.agent_config and self.agent_config.models else "openrouter/openai/gpt-4o-mini"
                
                # Get API key
                api_key = settings.llms.openrouter_api_key
                
                dspy_lm = create_dspy_lm(provider=provider, model=model, api_key=api_key)
                configure_dspy_lm(dspy_lm)
            except Exception as e:
                logger.error(f"Failed to initialize DSPy LLM: {e}")
                return PassthroughNode(name=name)
            
            # Determine which output fields should be parsed as lists
            # based on the agent_state field_type definitions
            list_fields = set()
            if self.agent_config and self.agent_config.agent_state and dspy_config.signature:
                for field_name in dspy_config.signature.output_fields:
                    state_field = self.agent_config.agent_state.get(field_name)
                    if state_field and state_field.field_type == "list":
                        list_fields.add(field_name)

            return DSPyNode(
                name=name,
                dspy_config=dspy_config,
                output_key=cfg.output_key or "output",
                list_fields=list_fields,
            )

        # Default: PassthroughNode
        return PassthroughNode(name=name)

    async def build_graph(self) -> StateGraph:
        """Build and compile the LangGraph StateGraph from loaded config."""
        if self.agent_config is None:
            await self.load_config()

        cfg = self.agent_config
        
        # Create custom state class if agent_state is defined in config
        from src.agent.state import AgentState as BaseAgentState, create_agent_state
        from typing import Optional, Any
        
        if cfg.agent_state:
            # Build state fields from config
            state_fields = {}
            for field_name, field_config in cfg.agent_state.items():
                # Map field_type string to actual Python type
                field_type_str = field_config.field_type
                if field_type_str == "str":
                    state_fields[field_name] = Optional[str]
                elif field_type_str == "int":
                    state_fields[field_name] = Optional[int]
                elif field_type_str == "float":
                    state_fields[field_name] = Optional[float]
                elif field_type_str == "bool":
                    state_fields[field_name] = Optional[bool]
                elif field_type_str == "dict":
                    state_fields[field_name] = Optional[dict]
                elif field_type_str == "list":
                    state_fields[field_name] = Optional[list]
                else:
                    # Default to Any for unknown types
                    state_fields[field_name] = Optional[Any]
            
            # Create custom state class
            CustomState = create_agent_state(state_fields, base_class=BaseAgentState)
            graph_builder = StateGraph(CustomState)
        else:
            # Use default AgentState
            graph_builder = StateGraph(BaseAgentState)

        # Load tools once and prepare callable map
        # IMPORTANT: Keep mcp_manager alive to prevent MCP server process from dying
        try:
            from src.utils.tool_loader import load_all_tools
            tools_map, self.mcp_manager = await load_all_tools(agent_config=cfg)
            if self.mcp_manager:
                logger.info("MCP client manager initialized and will be kept alive")
        except Exception as e:
            logger.error(f"Failed to load tools: {e}")
            tools_map = {}
            self.mcp_manager = None

        # Create node instances and register
        # Store router instances separately so we can access their route_func later
        router_instances = {}
        nodes = cfg.graph.nodes
        for node_name, node_cfg in nodes.items():
            node_instance = self._create_node_instance(node_name, node_cfg, tools_map)
            
            # Store router instances for later use in conditional edges
            if isinstance(node_instance, RouterNode):
                router_instances[node_name] = node_instance

            # For GeneratorNode, inject middleware config so it can apply it when creating LLM
            if isinstance(node_instance, GeneratorNode):
                # Resolve tools if present
                if node_instance.tools:
                    resolved_tools = []
                    for tool_name in node_instance.tools:
                        if tool_name in tools_map:
                            resolved_tools.append(tools_map[tool_name])
                        else:
                            logger.warning(f"Tool '{tool_name}' not found in tools_map. Available tools: {list(tools_map.keys())}")
                    node_instance.tools = resolved_tools

                # Pass middleware config to the node so it can apply it when creating LLM
                if cfg.features.middleware.model_retry:
                    node_instance._middleware_config = {
                        'model_retry': True,
                        'model_retry_retries': cfg.features.middleware.model_retry_retries
                    }
                else:
                    node_instance._middleware_config = {}
                pass

            # Inject tools into ToolExecutorNode and wrap with tool_retry middleware
            if isinstance(node_instance, ToolExecutorNode):
                tool_name = getattr(node_instance, "tool_name", None)
                if tool_name:
                    if tool_name in tools_map:
                        tool_callable = tools_map[tool_name]
                        # Apply langchain tool middleware if present else fallback
                        try:
                            langchain_mw = importlib.import_module("langchain.middleware")
                            if getattr(cfg.features.middleware, "tool_retry", False) and hasattr(langchain_mw, "tool_retry"):
                                wrapped_tool = langchain_mw.tool_retry(retries=cfg.features.middleware.tool_retry_retries)(tool_callable)
                            elif getattr(cfg.features.middleware, "tool_retry", False):
                                wrapped_tool = mw_fallback.tool_retry(retries=cfg.features.middleware.tool_retry_retries)(tool_callable)
                            else:
                                wrapped_tool = tool_callable
                        except Exception:
                            wrapped_tool = mw_fallback.tool_retry(retries=cfg.features.middleware.tool_retry_retries)(tool_callable) if getattr(cfg.features.middleware, "tool_retry", False) else tool_callable

                        node_instance.tool = wrapped_tool
                    else:
                        logger.warning(f"Tool '{tool_name}' not found in tools_map. Available tools: {list(tools_map.keys())}")

            # LangGraph supports both sync and async callables. Our node
            # instances expose an async `__call__`. To allow using the
            # synchronous `graph.invoke()` test helper, wrap async nodes
            # into a synchronous callable via `asyncio.run`.
            # If the node instance exposes an async __call__, wrap it
            # into a plain synchronous function that LangGraph can invoke
            # via `graph.invoke()` in tests.
            if hasattr(node_instance, "__call__"):
                async_callable = node_instance
                try:
                    loop_running = asyncio.get_event_loop().is_running()
                except Exception:
                    loop_running = False

                if loop_running:
                    # We're already in an async context (tests or async runtime) — register the async callable
                    graph_builder.add_node(node_name, async_callable)
                else:
                    def make_sync_wrapper(ac):
                        def sync_fn(state):
                            return asyncio.run(ac(state))

                        return sync_fn

                    wrapped = make_sync_wrapper(async_callable)
                    graph_builder.add_node(node_name, wrapped)
            else:
                graph_builder.add_node(node_name, node_instance)

        # Wire edges: entry point
        graph_builder.add_edge(START, cfg.graph.entry_point)

        # Add next edges (sequential)
        for node_name, node_cfg in nodes.items():
            # Check if this node has conditional routing (conditions specified)
            if node_cfg.conditions:
                # ANY node can have conditional routing, not just RouterNode
                # Create a route function based on the conditions
                def create_route_func(conditions, default_next, current_node_name):
                    def route_func(state: Dict[str, Any]):
                        logger.warning(f"[DEBUG] Conditional routing for '{current_node_name}' evaluating state keys: {list(state.keys())}")
                        for cond_key, next_node in conditions.items():
                            cond_value = state.get(cond_key)
                            logger.warning(f"[DEBUG] Checking condition '{cond_key}': value={cond_value}, type={type(cond_value).__name__}")
                            
                            # Check if condition is truthy
                            if cond_value is None or cond_value == "":
                                logger.warning(f"[DEBUG] Condition '{cond_key}' skipped: value is None or empty")
                                continue
                            
                            if isinstance(cond_value, bool):
                                if cond_value:
                                    logger.warning(f"[DEBUG] Condition '{cond_key}' matched (boolean True), routing to '{next_node}'")
                                    return next_node
                                else:
                                    logger.warning(f"[DEBUG] Condition '{cond_key}' is False, skipping")
                                continue
                            
                            if isinstance(cond_value, (list, dict)):
                                if len(cond_value) > 0:
                                    logger.warning(f"[DEBUG] Condition '{cond_key}' matched (non-empty {type(cond_value).__name__}), routing to '{next_node}'")
                                    return next_node
                                else:
                                    logger.warning(f"[DEBUG] Condition '{cond_key}' is empty, skipping")
                                continue
                            
                            if cond_value:
                                logger.warning(f"[DEBUG] Condition '{cond_key}' matched (truthy), routing to '{next_node}'")
                                return next_node
                        
                        # Default to next if no conditions matched
                        logger.warning(f"[DEBUG] No conditions matched for '{current_node_name}', defaulting to '{default_next}'")
                        return default_next
                    return route_func
                
                # Build the path map
                path_map = {}
                for next_node in node_cfg.conditions.values():
                    if next_node == "__end__":
                        path_map[next_node] = END
                    else:
                        path_map[next_node] = next_node
                
                # Add default next to path map
                default_next = node_cfg.next if node_cfg.next else END
                if default_next == "__end__":
                    default_next = END
                if default_next not in path_map:
                    path_map[default_next] = default_next if default_next != END else END
                
                logger.debug(f"Adding conditional edges for node '{node_name}' with path_map: {path_map}")
                
                # Add conditional edges
                graph_builder.add_conditional_edges(
                    node_name,
                    create_route_func(node_cfg.conditions, default_next, node_name),
                    path_map
                )
            elif node_cfg.next:
                # Regular edge (no conditions)
                if node_cfg.type == "parallel":
                    # For parallel nodes, add edges to branches instead of direct to next
                    for branch in getattr(node_cfg, "branches", []):
                        graph_builder.add_edge(node_name, branch)
                    # Add edges from branches to the parallel node's next
                    parallel_next = node_cfg.next
                    for branch in getattr(node_cfg, "branches", []):
                        if parallel_next == "__end__":
                            graph_builder.add_edge(branch, END)
                        else:
                            graph_builder.add_edge(branch, parallel_next)
                else:
                    if node_cfg.next == "__end__":
                        graph_builder.add_edge(node_name, END)
                    else:
                        graph_builder.add_edge(node_name, node_cfg.next)

        # Compile graph
        self.graph = graph_builder.compile()
        logger.info(f"Compiled graph for agent: {cfg.agent_name}")
        return self.graph

    async def create(self):
        """Convenience: load config and build compiled graph."""
        await self.load_config()
        return await self.build_graph()


__all__ = ["AgentFactory"]
