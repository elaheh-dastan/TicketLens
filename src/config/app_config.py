"""
Pydantic models for agent configuration validation.
Handles YAML configuration schemas for agents, prompts, and environment variables.
"""

from typing import Dict, List, Optional, Union, Any, Literal
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator
from enum import Enum


class LLMProvider(str, Enum):
    """Supported LLM providers"""
    OPENROUTER = "openrouter"


class NodeType(str, Enum):
    """Supported node types"""
    GENERATOR = "generator"
    RAG = "rag"
    TOOL_EXECUTOR = "tool_executor"
    VALIDATOR = "validator"
    PASSTHROUGH = "passthrough"
    CONDITIONAL = "conditional"
    ROUTER = "router"
    HUMAN_IN_LOOP = "human_in_loop"
    VISION = "vision"
    CUSTOM = "custom"
    BILL_PROCESSING = "bill_processing"
    EXPENSE_VALIDATION = "expense_validation"
    APPROVAL_DECISION = "approval_decision"
    NOTIFICATION = "notification"
    PARALLEL = "parallel"
    OCR = "ocr"
    FUSION = "fusion"
    DSPY = "dspy"
    SCORE_CALCULATOR = "score_calculator"


class ToolType(str, Enum):
    """Tool types"""
    CALCULATOR = "calculator"
    SEARCH = "search"
    EMAIL = "email"
    DATABASE = "database"
    FILESYSTEM = "filesystem"
    HTTP = "http"
    VISION = "vision"
    CUSTOM = "custom"


# =============================================================================
# DSPy Configuration Models (must be defined before NodeConfig)
# =============================================================================

class DSPyModule(str, Enum):
    """Supported DSPy modules"""
    CHAIN_OF_THOUGHT = "ChainOfThought"
    REACT = "ReAct"
    PROGRAM_OF_THOUGHT = "ProgramOfThought"
    MULTI_CHAIN = "MultiChain"
    CUSTOM = "custom"


class DSPyTeleprompter(str, Enum):
    """Supported DSPy teleprompters"""
    BOOTSTRAP_FEW_SHOT = "BootstrapFewShot"
    MIPRO = "MIPRO"
    COPRO = "COPRO"
    SIGNATURE_OPTIMIZER = "SignatureOptimizer"
    ENSEMBLE = "Ensemble"


class DSPyMetric(str, Enum):
    """Built-in DSPy metrics"""
    EXACT_MATCH = "exact_match"
    F1_SCORE = "f1_score"
    SEMANTIC_SIMILARITY = "semantic_similarity"
    PARTIAL_MATCH = "partial_match"
    SCORE_PROXIMITY = "score_proximity"
    CUSTOM = "custom"


class DSPySignatureConfig(BaseModel):
    """DSPy signature configuration"""
    
    input_fields: Dict[str, str] = Field(
        ...,
        description="Input field definitions (name -> description)"
    )
    output_fields: Dict[str, str] = Field(
        ...,
        description="Output field definitions (name -> description)"
    )
    instructions: Optional[str] = Field(
        default=None,
        description="Task instructions for the signature"
    )
    
    model_config = ConfigDict(extra="forbid")


class DSPyOptimizationConfig(BaseModel):
    """DSPy optimization configuration"""
    
    enabled: bool = Field(
        default=False,
        description="Enable DSPy optimization"
    )
    teleprompter: DSPyTeleprompter = Field(
        default=DSPyTeleprompter.BOOTSTRAP_FEW_SHOT,
        description="Teleprompter to use for optimization"
    )
    metric: DSPyMetric = Field(
        default=DSPyMetric.EXACT_MATCH,
        description="Metric for optimization"
    )
    custom_metric_path: Optional[str] = Field(
        default=None,
        description="Path to custom metric function (if metric=custom)"
    )
    
    # Training data configuration
    training_data_source: str = Field(
        default="file",
        description="Source of training data (file, database, api)"
    )
    training_data_path: Optional[str] = Field(
        default=None,
        description="Path to training data file or directory"
    )
    validation_split: float = Field(
        default=0.2,
        ge=0.0,
        le=0.5,
        description="Fraction of data for validation"
    )
    
    # Teleprompter-specific parameters
    max_bootstrapped_demos: int = Field(
        default=8,
        ge=1,
        le=20,
        description="Max examples for few-shot (BootstrapFewShot)"
    )
    max_labeled_demos: int = Field(
        default=4,
        ge=0,
        le=10,
        description="Max labeled examples (MIPRO)"
    )
    num_candidates: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Number of candidates to generate (MIPRO)"
    )
    init_temperature: float = Field(
        default=1.0,
        ge=0.0,
        le=2.0,
        description="Initial temperature for optimization"
    )
    
    # Storage configuration
    prompts_output_path: str = Field(
        default="data/optimized_prompts",
        description="Directory to save optimized prompts"
    )
    versioning: bool = Field(
        default=True,
        description="Enable prompt versioning"
    )
    
    model_config = ConfigDict(extra="forbid")


class DSPyConfig(BaseModel):
    """DSPy configuration for a node or globally"""
    
    enabled: bool = Field(
        default=False,
        description="Enable DSPy for this node"
    )
    module: DSPyModule = Field(
        default=DSPyModule.CHAIN_OF_THOUGHT,
        description="DSPy module to use"
    )
    signature: Optional[DSPySignatureConfig] = Field(
        default=None,
        description="DSPy signature definition"
    )
    custom_module_path: Optional[str] = Field(
        default=None,
        description="Path to custom DSPy module (if module=custom)"
    )
    
    # Model configuration
    model: Optional[str] = Field(
        default=None,
        description="LLM model to use for DSPy (e.g., 'openrouter/openai/gpt-4o'). If not specified, uses agent's default model."
    )
    provider: Optional[str] = Field(
        default=None,
        description="LLM provider for DSPy (e.g., 'openrouter'). If not specified, uses agent's default provider."
    )
    temperature: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=2.0,
        description="Temperature for DSPy LLM calls. If not specified, uses agent's default temperature."
    )
    max_tokens: Optional[int] = Field(
        default=None,
        ge=1,
        description="Max tokens for DSPy LLM calls. If not specified, uses agent's default max_tokens."
    )
    
    # Optimization configuration
    optimization: DSPyOptimizationConfig = Field(
        default_factory=DSPyOptimizationConfig,
        description="Optimization configuration"
    )
    
    # Runtime configuration
    use_optimized: bool = Field(
        default=True,
        description="Use optimized prompts if available"
    )
    fallback_to_baseline: bool = Field(
        default=True,
        description="Fall back to baseline if optimized prompts unavailable"
    )
    cache_prompts: bool = Field(
        default=True,
        description="Cache optimized prompts in memory"
    )
    
    model_config = ConfigDict(extra="forbid")


class AgentState(BaseModel):
    """Agent state field definitions"""
    
    model_config = ConfigDict(extra="forbid")  # Reject unknown fields
    
    field_type: str = Field(..., description="Type of the state field")
    description: Optional[str] = Field(default=None, description="Description of the state field")
    required: bool = Field(default=False, description="Whether this field is required")
    default: Optional[Any] = Field(default=None, description="Default value for the field")


class PromptTemplate(BaseModel):
    """Prompt template configuration"""
    
    template: str = Field(..., description="Jinja2 template string")
    variables: Dict[str, str] = Field(default_factory=dict, description="Template variables")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    model_config = ConfigDict(extra="forbid")


class PromptConfig(BaseModel):
    """Prompt configuration"""
    
    prompts: Dict[str, PromptTemplate] = Field(..., description="Dictionary of prompt templates")
    
    model_config = ConfigDict(extra="forbid")


class ToolConfig(BaseModel):
    """Tool configuration"""
    
    name: str = Field(..., description="Tool name")
    type: ToolType = Field(..., description="Tool type")
    description: Optional[str] = Field(default=None, description="Tool description")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Tool parameters")
    enabled: bool = Field(default=True, description="Whether tool is enabled")
    
    model_config = ConfigDict(extra="forbid")


class NodeConfig(BaseModel):
    """Node configuration"""
    
    type: NodeType = Field(..., description="Node type")
    prompt: Optional[str] = Field(default=None, description="Prompt template name")
    next: Optional[Union[str, Dict[str, str]]] = Field(default=None, description="Next node to execute (string or dict for conditional routing)")
    tools: List[str] = Field(default_factory=list, description="Tools available to this node")
    output_key: Optional[str] = Field(default=None, description="Key to store node output in state")
    input_key: Optional[str] = Field(default=None, description="Key to read input from state")
    
    # Node-specific configurations
    conditions: Optional[Dict[str, str]] = Field(default=None, description="Conditional routing")
    validation_schema: Optional[Dict[str, Any]] = Field(default=None, description="Validation schema")
    retry_count: int = Field(default=3, description="Number of retries on failure")
    timeout_seconds: Optional[int] = Field(default=None, description="Execution timeout")
    
    # LLM-specific configurations
    model: Optional[str] = Field(default=None, description="Override model for this node")
    provider: Optional[str] = Field(default=None, description="Override provider for this node")
    response_format: Optional[Dict[str, str]] = Field(default=None, description="Response format (e.g., {'type': 'json_object'})")
    
    # DSPy configuration
    dspy: Optional[DSPyConfig] = Field(default=None, description="DSPy configuration for this node")
    
    model_config = ConfigDict(extra="allow")


class GraphConfig(BaseModel):
    """Graph configuration"""
    
    entry_point: str = Field(..., description="Entry point node name")
    nodes: Dict[str, NodeConfig] = Field(..., description="Dictionary of nodes in the graph")
    
    # Optional graph-level settings
    checkpointing: bool = Field(default=True, description="Enable state checkpointing")
    streaming: bool = Field(default=False, description="Enable response streaming")
    parallel_execution: bool = Field(default=False, description="Allow parallel node execution")
    
    model_config = ConfigDict(extra="forbid")


class KafkaConfig(BaseModel):
    """Kafka configuration"""
    
    enabled: bool = Field(default=False, description="Enable Kafka functionality")
    publish_results: bool = Field(default=False, description="Publish agent results to Kafka")
    results_topic: str = Field(default="agent-results", description="Kafka topic for agent results")
    
    model_config = ConfigDict(extra="forbid")


class FeaturesConfig(BaseModel):
    """Features configuration"""
    
    kafka: KafkaConfig = Field(default_factory=KafkaConfig, description="Kafka configuration")
    dspy: DSPyConfig = Field(default_factory=DSPyConfig, description="DSPy configuration")
    
    # Middleware configuration
    class MiddlewareConfig(BaseModel):
        enabled: bool = Field(default=True, description="Enable middleware features")
        model_retry: bool = Field(default=True, description="Enable model retry middleware")
        model_retry_retries: int = Field(default=3, description="Model retry attempts")
        tool_retry: bool = Field(default=True, description="Enable tool retry middleware")
        tool_retry_retries: int = Field(default=2, description="Tool retry attempts")
        emulate_tools: bool = Field(default=False, description="Enable LLM tool emulation (testing only)")
        summarization: bool = Field(default=True, description="Enable summarization middleware")
        summarization_threshold: int = Field(default=4000, description="Character threshold to trigger summarization")

    middleware: MiddlewareConfig = Field(default_factory=MiddlewareConfig, description="Middleware configuration")
    
    model_config = ConfigDict(extra="forbid")


class ModelsConfig(BaseModel):
    """LLM models configuration"""
    
    generator: str = Field(..., description="Primary generator model")
    vision: Optional[str] = Field(default=None, description="Vision model for image processing")
    embedding: str = Field(default="text-embedding-3-small", description="Embedding model")
    
    # Provider-specific configurations
    provider: LLMProvider = Field(..., description="LLM provider")
    api_key: Optional[str] = Field(default=None, description="API key for the provider")
    api_base: Optional[str] = Field(default=None, description="Custom API base URL")
    organization: Optional[str] = Field(default=None, description="Organization ID")
    
    # Model parameters
    temperature: float = Field(default=0.7, description="Model temperature")
    max_tokens: Optional[int] = Field(default=None, description="Maximum tokens")
    top_p: float = Field(default=1.0, description="Top-p value")
    frequency_penalty: float = Field(default=0.0, description="Frequency penalty")
    presence_penalty: float = Field(default=0.0, description="Presence penalty")
    
    model_config = ConfigDict(extra="forbid")

    @field_validator('temperature', 'top_p', 'frequency_penalty', 'presence_penalty', mode='before')
    def validate_model_parameters(cls, v):
        """Validate model parameters"""
        if v is None:
            return v
        if v < -2.0 or v > 2.0:
            raise ValueError("Parameter must be between -2.0 and 2.0")
        return v


class AgentConfig(BaseModel):
    """Main agent configuration model"""
    
    agent_name: str = Field(..., description="Name of the agent")
    description: Optional[str] = Field(default=None, description="Agent description")
    
    # Core configurations
    models: ModelsConfig = Field(..., description="LLM models configuration")
    graph: GraphConfig = Field(..., description="Graph configuration")
    features: FeaturesConfig = Field(default_factory=FeaturesConfig, description="Features configuration")
    
    # Tools and prompts
    tools: List[str] = Field(default_factory=list, description="List of enabled tools")
    
    # Agent state definition
    agent_state: Dict[str, AgentState] = Field(
        default_factory=dict, 
        description="Agent state field definitions"
    )
    
    # Metadata
    version: str = Field(default="1.0.0", description="Agent configuration version")
    created_at: Optional[str] = Field(default=None, description="Creation timestamp")
    updated_at: Optional[str] = Field(default=None, description="Last update timestamp")
    
    # Environment overrides
    environment_variables: Dict[str, str] = Field(
        default_factory=dict, 
        description="Environment variable overrides"
    )
    
    model_config = ConfigDict(extra = "forbid")

    @field_validator('agent_name', mode='before')
    def validate_agent_name(cls, v):
        """Validate agent name format"""
        if not v or not str(v).strip():
            raise ValueError("Agent name cannot be empty")
        # Allow alphanumeric, hyphens, underscores
        import re
        if not re.match(r'^[a-zA-Z0-9_-]+$', str(v)):
            raise ValueError("Agent name can only contain letters, numbers, hyphens, and underscores")
        return v

    @model_validator(mode='after')
    def validate_graph_consistency(self):
        """Validate graph consistency (instance validator)

        Converted from a classmethod to an instance method to comply with
        Pydantic v2.12 deprecation guidance.
        """
        graph = getattr(self, 'graph', None)
        if not graph:
            return self

        entry_point = graph.entry_point
        nodes = graph.nodes

        # Check if entry point exists
        if entry_point not in nodes:
            raise ValueError(f"Entry point '{entry_point}' not found in nodes")

        # Check if all 'next' references exist
        for node_name, node_config in nodes.items():
            if node_config.next:
                # Handle string next
                if isinstance(node_config.next, str):
                    if node_config.next == "__end__":
                        continue  # End node is allowed
                    if node_config.next not in nodes:
                        raise ValueError(f"Node '{node_name}' references non-existent next node '{node_config.next}'")
                # Handle dict next (conditional routing)
                elif isinstance(node_config.next, dict):
                    for condition, next_node in node_config.next.items():
                        if next_node == "__end__":
                            continue  # End node is allowed
                        if next_node not in nodes:
                            raise ValueError(f"Node '{node_name}' condition '{condition}' references non-existent next node '{next_node}'")

        return self


class ConfigFile(BaseModel):
    """Model representing a complete configuration file"""
    
    agent: AgentConfig = Field(..., description="Agent configuration")
    
    model_config = ConfigDict(extra = "forbid")


class EnvironmentMapping(BaseModel):
    """Environment variable mapping configuration"""
    
    source: str = Field(..., description="Source environment variable (e.g., LLMS__OPENAI_API_KEY)")
    target: str = Field(..., description="Target configuration path (e.g., models.api_key)")
    required: bool = Field(default=True, description="Whether this variable is required")
    default: Optional[str] = Field(default=None, description="Default value if not provided")
    
    model_config = ConfigDict(extra = "forbid")


class ConfigValidationResult(BaseModel):
    """Result of configuration validation"""
    
    is_valid: bool = Field(..., description="Whether the configuration is valid")
    errors: List[str] = Field(default_factory=list, description="List of validation errors")
    warnings: List[str] = Field(default_factory=list, description="List of validation warnings")
    processed_config: Optional[Dict[str, Any]] = Field(default=None, description="Processed configuration")
    
    model_config = ConfigDict(extra = "forbid")


# Export all models
__all__ = [
    'LLMProvider',
    'NodeType',
    'ToolType',
    'KafkaConfig',
    'DSPyModule',
    'DSPyTeleprompter',
    'DSPyMetric',
    'DSPySignatureConfig',
    'DSPyOptimizationConfig',
    'DSPyConfig',
    'AgentState',
    'PromptTemplate',
    'PromptConfig',
    'ToolConfig',
    'NodeConfig',
    'GraphConfig',
    'FeaturesConfig',
    'ModelsConfig',
    'AgentConfig',
    'ConfigFile',
    'EnvironmentMapping',
    'ConfigValidationResult'
]
