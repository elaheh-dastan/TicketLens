"""
Dynamic tool loader for agent framework.

Scans agent_config/custom_tools/ for @tool-decorated async functions and registers them with LangChain.
"""
import importlib.util
import sys
from pathlib import Path
from typing import Dict

from langchain_core.tools import BaseTool
import logging

logger = logging.getLogger(__name__)

def load_tools(tools_dir: str = "agent_config/custom_tools", settings=None) -> Dict[str, BaseTool]:
    """
    Discover and load all @tool async functions from the given directory.
    Returns a dict mapping tool name to tool instance.
    """
    tool_map = {}
    tools_path = Path(tools_dir)
    if not tools_path.exists():
        return tool_map
    for pyfile in tools_path.glob("*.py"):
        module_name = f"custom_tools_{pyfile.stem}"
        spec = importlib.util.spec_from_file_location(module_name, pyfile)
        if not spec or not spec.loader:
            continue
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        # Find all @tool objects in the module. Don't rely on __module__ equality
        # because langchain's decorator may produce tool objects coming from other
        # modules.
        for attr in dir(module):
            obj = getattr(module, attr)
            try:
                is_tool = isinstance(obj, BaseTool)
            except Exception:
                is_tool = False
            if is_tool:
                # For ease of use in the factory, we expose the callable form of the tool.
                # We wrap the tool to handle **kwargs from ToolExecutorNode and convert to tool_input
                
                def make_tool_wrapper(tool_instance):
                    # Try to determine the first argument name for wrapping single inputs
                    first_arg = "input"
                    if hasattr(tool_instance, "args") and tool_instance.args:
                        first_arg = list(tool_instance.args.keys())[0]
                    
                    async def wrapper(*args, **kwargs):
                        if args:
                            input_val = args[0]
                            if not isinstance(input_val, dict):
                                # Wrap non-dict input in a dict using the first argument name
                                logger.debug(f"Wrapping non-dict input with key: {first_arg}")
                                return await tool_instance.ainvoke({first_arg: input_val})
                            # If input is a dict, extract the first_arg value if it exists
                            if first_arg in input_val:
                                return await tool_instance.ainvoke({first_arg: input_val[first_arg]})
                            return await tool_instance.ainvoke(input_val)
                        elif kwargs:
                            # If kwargs contains a string value for the first_arg, wrap it
                            if first_arg in kwargs and isinstance(kwargs[first_arg], str):
                                logger.debug(f"Wrapping string kwarg with key: {first_arg}")
                                return await tool_instance.ainvoke({first_arg: kwargs[first_arg]})
                            return await tool_instance.ainvoke(kwargs)
                        else:
                            # No args and no kwargs, return empty dict
                            return await tool_instance.ainvoke({})
                    return wrapper

                callable_obj = make_tool_wrapper(obj)

                # Some tools may not expose `name` attribute; use attribute name as fallback
                tool_name = getattr(obj, "name", attr)
                tool_map[tool_name] = callable_obj
            elif callable(obj):
                # If the attribute is a plain callable (not a BaseTool), wrap it
                # so callers have a `.name` and callable behavior.
                class ToolWrapper:
                    def __init__(self, name, fn):
                        self.name = name
                        self._fn = fn

                    def __call__(self, *a, **kw):
                        return self._fn(*a, **kw)

                tool_map[attr] = ToolWrapper(attr, obj)
    return tool_map


async def load_all_tools(tools_dir: str = "agent_config/custom_tools", settings=None, agent_config=None) -> tuple[Dict[str, BaseTool], None]:
    """Load custom tools.

    Returns a tuple of (tool_map, None) for backward compatibility.
    """
    tool_map = load_tools(tools_dir)
    return tool_map, None
