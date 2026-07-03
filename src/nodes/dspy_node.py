"""
DSPy-powered node for optimized LLM calls.

This module provides the DSPyNode class that uses DSPy signatures and modules
for structured prompting with optional optimization.
"""

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Dict, Any, Optional, Type

import dspy

from src.nodes.base_node import BaseNode
from src.config.app_config import DSPyConfig, DSPyModule
from src.llm.dspy_adapter import create_dspy_lm, configure_dspy_lm
from src.config.settings import get_settings
from src.metrics.registry import get_metrics, classify_llm_error

logger = logging.getLogger(__name__)

def debug_log(msg):
    try:
        with open("dspy_debug.log", "a") as f:
            f.write(f"{msg}\n")
    except Exception:
        pass

class DSPyNode(BaseNode):
    """
    Node that uses DSPy modules for optimized LLM calls.
    
    This node:
    1. Wraps DSPy modules (ChainOfThought, ReAct, etc.)
    2. Loads optimized prompts if available
    3. Falls back to baseline if optimization unavailable
    4. Integrates with existing middleware
    5. Compatible with LangGraph state management
    """
    
    def __init__(
        self,
        name: str = "dspy_node",
        dspy_config: Optional[DSPyConfig] = None,
        output_key: str = "output",
        list_fields: Optional[set] = None,
    ):
        """
        Initialize DSPy node.

        Args:
            name: Node name
            dspy_config: DSPy configuration
            output_key: Key to store output in state
            list_fields: Output field names that should be typed as list[str] in the DSPy signature
        """
        super().__init__(name)
        self.dspy_config = dspy_config or DSPyConfig()
        self.output_key = output_key
        self.list_fields = list_fields or set()
        
        # DSPy components
        self.dspy_module = None
        self.signature = None
        self.optimized_prompts = None
        self._initialized = False
    
    async def _initialize_dspy(self):
        """Initialize DSPy module and load optimized prompts."""
        if self._initialized:
            return
        
        # Get settings for fallback values
        settings = get_settings()
        
        # Determine provider: Always OpenRouter
        provider = "openrouter"
        
        # Determine model: DSPy config > settings default
        if self.dspy_config and self.dspy_config.model:
            model = self.dspy_config.model
        else:
            # Fallback to a reasonable default
            model = "openrouter/openai/gpt-4o-mini"
            logger.warning(f"No model specified in DSPy config for node '{self.name}', using default: {model}")
        
        # Build kwargs for DSPy LM
        lm_kwargs = {}
        if self.dspy_config:
            if self.dspy_config.temperature is not None:
                lm_kwargs['temperature'] = self.dspy_config.temperature
            if self.dspy_config.max_tokens is not None:
                lm_kwargs['max_tokens'] = self.dspy_config.max_tokens
        
        # Always use OpenRouter API key
        api_key = settings.llms.openrouter_api_key
        if api_key:
            lm_kwargs['api_key'] = api_key
        
        # Create DSPy LM
        dspy_lm = create_dspy_lm(
            provider=provider,
            model=model,
            **lm_kwargs
        )
        
        # Store the LM for use with dspy.context() instead of global configure
        self.dspy_lm = dspy_lm
        
        # Create DSPy signature
        self.signature = self._create_signature()
        
        # Create DSPy module
        self.dspy_module = self._create_module()
        
        # Load optimized prompts if available
        if self.dspy_config.use_optimized:
            await self._load_optimized_prompts()
        
        self._initialized = True
    
    def _create_signature(self) -> Type:
        """Create DSPy signature from configuration."""
        if not self.dspy_config.signature:
            raise ValueError(f"DSPy signature not configured for node '{self.name}'")
        
        sig_config = self.dspy_config.signature
        
        # Build signature class dynamically
        class_name = f"{self.name.replace('-', '_').title()}Signature"
        
        # Prepare class dictionary with fields and annotations
        class_dict = {
            '__doc__': sig_config.instructions or f"Signature for {self.name}",
            '__annotations__': {}
        }
        
        # Add input fields
        for field_name, field_desc in sig_config.input_fields.items():
            class_dict['__annotations__'][field_name] = str
            class_dict[field_name] = dspy.InputField(desc=field_desc)
            
        # Add output fields
        for field_name, field_desc in sig_config.output_fields.items():
            class_dict['__annotations__'][field_name] = list[str] if field_name in self.list_fields else str
            class_dict[field_name] = dspy.OutputField(desc=field_desc)
            
        # Create signature class
        # We pass the fully populated dict so the metaclass sees everything at once
        signature_class = type(
            class_name,
            (dspy.Signature,),
            class_dict
        )
        
        logger.info(f"Created DSPy signature: {class_name}")
        return signature_class
    
    def _create_module(self) -> Any:
        """Create DSPy module based on configuration."""
        module_type = self.dspy_config.module
        
        if module_type == DSPyModule.CHAIN_OF_THOUGHT:
            return dspy.ChainOfThought(self.signature)
        elif module_type == DSPyModule.REACT:
            return dspy.ReAct(self.signature)
        elif module_type == DSPyModule.PROGRAM_OF_THOUGHT:
            return dspy.ProgramOfThought(self.signature)
        elif module_type == DSPyModule.MULTI_CHAIN:
            return dspy.MultiChain(self.signature)
        elif module_type == DSPyModule.CUSTOM:
            if not self.dspy_config.custom_module_path:
                raise ValueError("custom_module_path required for custom DSPy module")
            
            import importlib
            module_path, class_name = self.dspy_config.custom_module_path.rsplit('.', 1)
            module = importlib.import_module(module_path)
            custom_class = getattr(module, class_name)
            return custom_class(self.signature)
        else:
            raise ValueError(f"Unsupported DSPy module type: {module_type}")
    
    async def _load_optimized_prompts(self):
        """Load optimized prompts from storage."""
        prompts_dir = Path(self.dspy_config.optimization.prompts_output_path)
        prompts_file = prompts_dir / f"{self.name}.json"
        
        if not prompts_file.exists():
            logger.warning(
                f"No optimized prompts found for node '{self.name}' at {prompts_file}"
            )
            if not self.dspy_config.fallback_to_baseline:
                raise FileNotFoundError(
                    f"Optimized prompts required but not found: {prompts_file}"
                )
            return
        
        with open(prompts_file, 'r') as f:
            prompts_data = json.load(f)
        
        self.optimized_prompts = prompts_data
        
        # Apply optimized prompts to module if available
        if 'module_state' in prompts_data:
            self.dspy_module.load_state(prompts_data['module_state'])
        
        logger.info(
            f"Loaded optimized prompts for node '{self.name}' "
            f"(version: {prompts_data.get('version', 'unknown')})"
        )
    
    async def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute DSPy module.
        
        Args:
            state: Graph state containing inputs
        
        Returns:
            Updated state with outputs
        """
        debug_log(f"Executing DSPyNode: {self.name}")
        logger.info(f"Executing DSPyNode: {self.name}")
        logger.info(f"State keys: {list(state.keys())}")
        
        # Langfuse tracing — use the parent span created by BaseNode.__call__()
        # instead of creating a separate trace (which would produce duplicates).
        langfuse_parent = None
        langfuse_span = None
        try:
            from src.utils.langfuse_client import LANGFUSE_TRACE_KEY

            langfuse_parent = state.get(LANGFUSE_TRACE_KEY)
            if langfuse_parent:
                # Create a span for DSPy initialization
                langfuse_span = langfuse_parent.start_span(
                    name=f"dspy_init:{self.name}",
                    metadata={
                        "node_name": self.name,
                        "phase": "initialization",
                    },
                )
        except Exception as e:
            logger.warning(
                f"Failed to get Langfuse parent span for DSPyNode '{self.name}': {e}"
            )
        
        # Check if DSPy is enabled
        if not self.dspy_config.enabled:
            debug_log(f"DSPy not enabled for node '{self.name}', skipping")
            logger.warning(f"DSPy not enabled for node '{self.name}', skipping")
            if langfuse_span:
                try:
                    langfuse_span.update(
                        status_message="DSPy not enabled, skipping execution"
                    )
                    langfuse_span.end()
                except Exception:
                    pass
            return {self.output_key: {}}
        
        logger.info(f"DSPy is enabled for node '{self.name}'")
        
        # Initialize DSPy if needed
        init_start = time.time()
        await self._initialize_dspy()
        init_time = time.time() - init_start
        logger.info(f"DSPy initialized for node '{self.name}' (took {init_time:.2f}s)")
        
        # Update Langfuse span with initialization time
        if langfuse_span:
            try:
                langfuse_span.update(
                    metadata={
                        "initialization_time_seconds": init_time,
                        "model": getattr(self.dspy_lm, 'model', 'unknown') if hasattr(self, 'dspy_lm') else 'unknown',
                    }
                )
                langfuse_span.end()
            except Exception:
                pass
        
        # Check for signature
        if not self.dspy_config.signature:
            debug_log(f"DSPy signature not configured for node '{self.name}'")
            logger.error(f"DSPy signature not configured for node '{self.name}'")
            if langfuse_parent:
                try:
                    langfuse_parent.update(
                        level="ERROR",
                        status_message="DSPy signature not configured"
                    )
                except Exception:
                    pass
            return {self.output_key: {}}
        
        logger.info(f"DSPy signature configured for node '{self.name}'")
        logger.info(f"Signature input fields: {list(self.dspy_config.signature.input_fields.keys())}")
        logger.info(f"Signature output fields: {list(self.dspy_config.signature.output_fields.keys())}")
        
        # Extract inputs from state based on signature
        inputs = {}
        for field_name in self.dspy_config.signature.input_fields.keys():
            if field_name in state:
                inputs[field_name] = state[field_name]
                logger.info(f"Input field '{field_name}' found in state")
            else:
                debug_log(f"Input field '{field_name}' not found in state")
                logger.warning(f"Input field '{field_name}' not found in state")
        
        debug_log(f"Inputs extracted: {list(inputs.keys())}")
        logger.info(f"Inputs extracted: {list(inputs.keys())}")
        
        # Create a span for DSPy execution
        execution_span = None
        if langfuse_parent:
            try:
                execution_span = langfuse_parent.start_span(
                    name=f"dspy_execution:{self.name}",
                    metadata={
                        "node_name": self.name,
                        "phase": "execution",
                        "input_fields": list(inputs.keys()),
                        "output_fields": list(self.dspy_config.signature.output_fields.keys()),
                    }
                )
                
                # Log input state
                truncated_inputs = self._truncate_dict(inputs, max_length=1000)
                execution_span.update(input=truncated_inputs)
            except Exception as e:
                logger.warning(f"Failed to create Langfuse execution span: {e}")
        
        # Execute DSPy module with context to avoid async task conflicts
        # Run in thread to avoid blocking the event loop since DSPy is synchronous
        execution_start = time.time()
        try:
            debug_log(f"Starting DSPy module execution for node '{self.name}'")
            logger.info(f"Starting DSPy module execution for node '{self.name}'")
            def _run_dspy():
                with dspy.context(lm=self.dspy_lm):
                    return self.dspy_module(**inputs)

            prediction = await asyncio.to_thread(_run_dspy)
            execution_time = time.time() - execution_start
            debug_log(f"DSPy module prediction received: {prediction}")
            logger.info(f"DSPy module prediction received: {prediction} (took {execution_time:.2f}s)")

            # Prometheus metrics
            try:
                _m = get_metrics()
                if _m:
                    _model_label = getattr(self, 'dspy_lm', None)
                    _model_label = getattr(_model_label, 'model', 'unknown') if _model_label else 'unknown'
                    _m.llm_calls_total.labels(node_name=self.name, model=_model_label, status="success").inc()
                    _m.llm_call_duration_seconds.labels(node_name=self.name, model=_model_label).observe(execution_time)
            except Exception:
                pass

            # Update Langfuse span with execution time
            if execution_span:
                try:
                    execution_span.update(
                        metadata={
                            "execution_time_seconds": execution_time,
                            "prediction_type": type(prediction).__name__,
                        }
                    )
                except Exception:
                    pass
        except Exception as e:
            execution_time = time.time() - execution_start
            debug_log(f"Error executing DSPy module for node '{self.name}': {e}")
            logger.error(f"Error executing DSPy module for node '{self.name}' after {execution_time:.2f}s: {e}", exc_info=True)

            # Prometheus metrics
            try:
                _m = get_metrics()
                if _m:
                    _model_label = getattr(self, 'dspy_lm', None)
                    _model_label = getattr(_model_label, 'model', 'unknown') if _model_label else 'unknown'
                    _m.llm_calls_total.labels(node_name=self.name, model=_model_label, status="error").inc()
                    _m.llm_call_errors_total.labels(
                        node_name=self.name,
                        model=_model_label,
                        error_type=type(e).__name__,
                        reason=classify_llm_error(e),
                    ).inc()
                    _m.llm_call_duration_seconds.labels(node_name=self.name, model=_model_label).observe(execution_time)
            except Exception:
                pass

            # Update Langfuse with error
            if execution_span:
                try:
                    execution_span.update(
                        level="ERROR",
                        status_message=str(e),
                        metadata={
                            "error_type": type(e).__name__,
                            "execution_time_seconds": execution_time,
                        }
                    )
                    execution_span.end()
                except Exception:
                    pass
            
            # Propagate exception to make it visible in task status
            raise e
        
        # Extract outputs
        output = {}
        for field_name in self.dspy_config.signature.output_fields.keys():
            if hasattr(prediction, field_name):
                value = getattr(prediction, field_name)
                output[field_name] = value
                logger.info(f"Output field '{field_name}' extracted: {output[field_name]}")
            else:
                debug_log(f"Output field '{field_name}' not found in prediction")
                logger.warning(f"Output field '{field_name}' not found in prediction")
        
        debug_log(f"Final output for node '{self.name}': {output}")
        logger.info(f"Final output for node '{self.name}': {output}")
        logger.info(f"DSPy module executed successfully for node '{self.name}'")
        
        # Return outputs directly merged into state (not nested under output_key)
        # Also include the nested version under output_key for backward compatibility
        result = {**output}  # Flatten outputs to top-level state
        if self.output_key:
            result[self.output_key] = output  # Also nest under output_key
        
        debug_log(f"Returning result with keys: {list(result.keys())}")
        debug_log(f"Result content: {result}")
        logger.info(f"Returning result with keys: {list(result.keys())}")
        
        # Update Langfuse parent span with output
        if langfuse_parent:
            try:
                truncated_output = self._truncate_dict(result, max_length=1000)
                langfuse_parent.update(output=truncated_output)
            except Exception:
                pass
        
        # End execution span
        if execution_span:
            try:
                execution_span.end()
            except Exception:
                pass
        
        return result
    
    def _truncate_dict(self, data: Dict[str, Any], max_length: int = 1000) -> Dict[str, Any]:
        """
        Truncate dictionary values to avoid sending too much data to Langfuse.
        
        Args:
            data: Dictionary to truncate
            max_length: Maximum length for string values
            
        Returns:
            Truncated dictionary
        """
        truncated = {}
        for key, value in data.items():
            if isinstance(value, str):
                if len(value) > max_length:
                    truncated[key] = value[:max_length] + "... (truncated)"
                else:
                    truncated[key] = value
            elif isinstance(value, (dict, list)):
                # For nested structures, just keep the structure but truncate strings
                truncated[key] = str(type(value).__name__)
            else:
                truncated[key] = str(value)
        return truncated
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"DSPyNode(name='{self.name}', "
            f"module={self.dspy_config.module.value}, "
            f"enabled={self.dspy_config.enabled})"
        )


# Export all classes
__all__ = ['DSPyNode']