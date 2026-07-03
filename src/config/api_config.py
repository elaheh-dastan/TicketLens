"""
Pydantic models for API configuration validation.
Handles YAML configuration schemas for FastAPI endpoints, request/response schemas, and state mappings.
"""

from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator
from enum import Enum


class SchemaType(str, Enum):
    """Supported schema field types"""

    STRING = "string"
    INTEGER = "integer"
    NUMBER = "number"
    BOOLEAN = "boolean"
    ARRAY = "array"
    OBJECT = "object"


class HTTPMethod(str, Enum):
    """Supported HTTP methods"""

    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"


class ExecutionMode(str, Enum):
    """Execution modes for API endpoints"""

    SYNC = "sync"
    BACKGROUND = "background"


class FieldSchema(BaseModel):
    """Schema for defining individual fields in request/response schemas.

    Attributes:
        name: Name of the field
        type: Data type (str, int, float, bool, dict, list)
        required: Whether this field is required
        description: Human-readable description of the field
        default: Default value if not provided
        validation_rules: Dictionary of validation rules (e.g., min, max, pattern, enum)
    """

    name: str = Field(..., description="Name of the field")
    type: str = Field(..., description="Field type: str, int, float, bool, dict, list")
    required: bool = Field(default=False, description="Whether this field is required")
    description: Optional[str] = Field(
        default=None, description="Description of the field"
    )
    default: Optional[Any] = Field(
        default=None, description="Default value for the field"
    )
    validation_rules: Dict[str, Any] = Field(
        default_factory=dict,
        description="Validation rules (min, max, pattern, enum, format, etc.)",
    )

    model_config = ConfigDict(extra="forbid")

    @field_validator("type", mode="before")
    def validate_field_type(cls, v):
        """Validate that the field type is supported"""
        valid_types = {
            "str",
            "string",
            "int",
            "integer",
            "float",
            "number",
            "bool",
            "boolean",
            "dict",
            "list",
            "array",
        }
        type_str = str(v).lower()
        if type_str not in valid_types:
            raise ValueError(f"Invalid field type '{v}'. Must be one of: {valid_types}")
        return type_str


class SchemaDefinition(BaseModel):
    """Schema definition for request/response bodies.

    Attributes:
        name: Name of the schema (e.g., "ResearchRequest", "TaskResponse")
        fields: List of field definitions
        description: Human-readable description of the schema
    """

    name: str = Field(..., description="Name of the schema")
    fields: List[FieldSchema] = Field(..., description="List of field definitions")
    description: Optional[str] = Field(
        default=None, description="Description of the schema"
    )

    model_config = ConfigDict(extra="forbid")


class EndpointConfig(BaseModel):
    """Configuration for a single API endpoint.

    Attributes:
        path: URL path for the endpoint (e.g., "/execute", "/status/{task_id}")
        method: HTTP method (GET, POST, PUT, DELETE, PATCH)
        description: Human-readable description of the endpoint
        execution_mode: Execution mode - sync (immediate) or background (async task)
        request_schema: Name of the request schema definition
        response_schema: Name of the response schema definition
        state_mapping: Mapping from request fields to agent state fields
        timeout: Request timeout in seconds (default 300)
    """

    path: str = Field(..., description="URL path for the endpoint")
    method: str = Field(..., description="HTTP method: GET, POST, PUT, DELETE, PATCH")
    description: Optional[str] = Field(
        default=None, description="Description of the endpoint"
    )
    execution_mode: str = Field(
        default="background", description="Execution mode: sync or background"
    )
    request_schema: Optional[str] = Field(
        default=None, description="Name of request schema"
    )
    response_schema: Optional[str] = Field(
        default=None, description="Name of response schema"
    )
    state_mapping: Dict[str, str] = Field(
        default_factory=dict, description="Maps request fields to agent state fields"
    )
    timeout: int = Field(default=300, description="Request timeout in seconds")

    model_config = ConfigDict(extra="forbid")

    @field_validator("method", mode="before")
    def validate_http_method(cls, v):
        """Validate HTTP method"""
        valid_methods = {"GET", "POST", "PUT", "DELETE", "PATCH"}
        method_upper = str(v).upper()
        if method_upper not in valid_methods:
            raise ValueError(
                f"Invalid HTTP method '{v}'. Must be one of: {valid_methods}"
            )
        return method_upper

    @field_validator("execution_mode", mode="before")
    def validate_execution_mode(cls, v):
        """Validate execution mode"""
        valid_modes = {"sync", "background"}
        mode = str(v).lower()
        if mode not in valid_modes:
            raise ValueError(
                f"Invalid execution mode '{v}'. Must be one of: {valid_modes}"
            )
        return mode

    @field_validator("timeout", mode="before")
    def validate_timeout(cls, v):
        """Validate timeout value"""
        if v is None:
            return 300
        if v < 1 or v > 3600:
            raise ValueError("Timeout must be between 1 and 3600 seconds")
        return v


class APIConfig(BaseModel):
    """Main API configuration model.

    Attributes:
        api_name: Unique name for the API
        agent_config: Path to the agent YAML configuration file
        description: Human-readable description of the API
        version: API version string
        base_path: Base path for all endpoints (e.g., "/api/v1/research")
        endpoints: List of endpoint configurations
        schemas: Dictionary of schema definitions
    """

    api_name: str = Field(..., description="Unique name for the API")
    agent_config: str = Field(..., description="Path to agent YAML configuration file")
    description: Optional[str] = Field(default=None, description="API description")
    version: str = Field(default="1.0.0", description="API version")
    base_path: str = Field(default="/api/v1", description="Base path for all endpoints")
    endpoints: List[EndpointConfig] = Field(
        ..., description="List of endpoint configurations"
    )
    schemas: Dict[str, SchemaDefinition] = Field(
        default_factory=dict, description="Dictionary of schema definitions"
    )

    model_config = ConfigDict(extra="forbid")

    @field_validator("api_name", mode="before")
    def validate_api_name(cls, v):
        """Validate API name format"""
        if not v or not str(v).strip():
            raise ValueError("API name cannot be empty")
        # Allow alphanumeric, hyphens, underscores
        import re

        if not re.match(r"^[a-zA-Z0-9_-]+$", str(v)):
            raise ValueError(
                "API name can only contain letters, numbers, hyphens, and underscores"
            )
        return v

    @model_validator(mode="after")
    def validate_endpoint_references(self):
        """Validate that endpoint schema references exist in schemas dictionary"""
        schema_names = set(self.schemas.keys())

        for endpoint in self.endpoints:
            if endpoint.request_schema and endpoint.request_schema not in schema_names:
                raise ValueError(
                    f"Endpoint '{endpoint.path}' references non-existent request_schema "
                    f"'{endpoint.request_schema}'"
                )
            if (
                endpoint.response_schema
                and endpoint.response_schema not in schema_names
            ):
                raise ValueError(
                    f"Endpoint '{endpoint.path}' references non-existent response_schema "
                    f"'{endpoint.response_schema}'"
                )

        return self


class APIFile(BaseModel):
    """Model representing a complete API configuration file.

    This wrapper model handles the common pattern where YAML files
    have a root "api" key containing the actual configuration.
    """

    api: APIConfig = Field(..., description="API configuration")

    model_config = ConfigDict(extra="forbid")


class APIValidationResult(BaseModel):
    """Result of API configuration validation"""

    is_valid: bool = Field(..., description="Whether the configuration is valid")
    errors: List[str] = Field(
        default_factory=list, description="List of validation errors"
    )
    warnings: List[str] = Field(
        default_factory=list, description="List of validation warnings"
    )
    processed_config: Optional[Dict[str, Any]] = Field(
        default=None, description="Processed configuration"
    )

    model_config = ConfigDict(extra="forbid")


# Export all models
__all__ = [
    "SchemaType",
    "HTTPMethod",
    "ExecutionMode",
    "FieldSchema",
    "SchemaDefinition",
    "EndpointConfig",
    "APIConfig",
    "APIFile",
    "APIValidationResult",
]
