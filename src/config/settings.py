"""
Configuration settings using Pydantic Settings.
Handles environment variable parsing with nested structure support.
"""
import os
from typing import Optional, List, Any
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, ConfigDict, SecretStr, field_validator, model_validator, BaseModel as PydanticBaseModel


class BaseConfigSettings(BaseSettings):
    """Base settings that apply nested env delimiter to derived settings."""
    model_config = SettingsConfigDict(env_nested_delimiter='__', case_sensitive=False)


class BaseConfigModel(PydanticBaseModel):
    """Base Pydantic model for non-settings typed objects."""
    model_config = ConfigDict(arbitrary_types_allowed = True)


class LLMsSettings(BaseConfigSettings):
    """LLM provider configurations"""
    
    # Default provider
    provider: str = Field(default="openrouter", description="Default LLM provider (openrouter)")
    
    # OpenRouter
    openrouter_api_key: Optional[str] = Field(default=None, description="OpenRouter API key")


class KafkaSettings(BaseConfigSettings):
    """Kafka configuration for message publishing and consuming"""
    
    # Connection settings
    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers (comma-separated)"
    )
    
    # Topic configuration
    input_topic: str = Field(
        default="agent-input",
        description="Topic for agent input messages"
    )
    results_topic: str = Field(
        default="agent-results",
        description="Topic for agent results"
    )
    status_topic: str = Field(
        default="agent-status",
        description="Topic for agent status updates"
    )
    topic_partitions: int = Field(
        default=3,
        description="Number of partitions for auto-created topics"
    )
    topic_replication_factor: int = Field(
        default=1,
        description="Replication factor for auto-created topics"
    )
    
    # Consumer settings
    consumer_group: str = Field(
        default="agent-framework",
        description="Consumer group ID"
    )
    auto_offset_reset: str = Field(
        default="latest",
        description="Auto offset reset policy (earliest, latest)"
    )
    max_poll_interval_ms: int = Field(
        default=300000,
        description="Maximum interval between poll calls in milliseconds. If the consumer does not poll within this interval, it will be considered dead and removed from the group."
    )
    
    # Producer settings
    producer_acks: str = Field(
        default="all",
        description="Producer acknowledgment level (0, 1, all)"
    )
    producer_retries: int = Field(
        default=3,
        description="Number of producer retries"
    )
    producer_compression_type: str = Field(
        default="snappy",
        description="Compression type (none, gzip, snappy, lz4, zstd)"
    )

    # Serialization settings
    serialization_format: str = Field(
        default="avro",
        description="Message serialization format (json, avro)"
    )
    avro_schema_path: Optional[str] = Field(
        default=None,
        description="Path to Avro schema directory (default: src/kafka/schemas)"
    )

    # Schema Registry settings (Confluent wire format)
    schema_registry_url: str = Field(
        default="http://localhost:8081",
        description="Confluent Schema Registry URL"
    )
    schema_registry_username: Optional[str] = Field(
        default=None,
        description="Schema Registry username for authenticated registries"
    )
    schema_registry_password: Optional[str] = Field(
        default=None,
        description="Schema Registry password for authenticated registries"
    )

    # Service settings
    max_concurrent_tasks: int = Field(
        default=1,
        description="Maximum concurrent agent tasks. Keep low to limit RAM usage since agent processing can be memory-intensive."
    )

    # Feature flags
    enabled: bool = Field(
        default=False,
        description="Enable Kafka integration"
    )
    publish_results: bool = Field(
        default=True,
        description="Publish agent results to Kafka"
    )
    consume_results: bool = Field(
        default=False,
        description="Consume results from Kafka (separate consumer service)"
    )

    # AUTH settings
    security_protocol: str = Field(
        default="PLAINTEXT",
        description="Security protocol (PLAINTEXT, SASL_PLAINTEXT, SSL, SASL_SSL)"
    )
    sasl_mechanism: Optional[str] = Field(
        default=None,
        description="SASL mechanism (SCRAM-SHA-512, SCRAM-SHA-256, PLAIN)"
    )
    sasl_username: Optional[str] = Field(
        default=None,
        description="SASL username"
    )
    sasl_password: Optional[SecretStr] = Field(
        default=None,
        description="SASL password"
    )
    ssl_verify: bool = Field(
        default=False,
        description="Verify SSL certificates (set to False to disable certificate verification)"
    )

    @property
    def auth_kwargs(self) -> dict:
        """Return auth-related kwargs for aiokafka clients."""
        import ssl
        kwargs = {"security_protocol": self.security_protocol}
        if "SSL" in self.security_protocol:
            ssl_context = ssl.create_default_context()
            if not self.ssl_verify:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
            kwargs["ssl_context"] = ssl_context
        if self.sasl_mechanism:
            kwargs["sasl_mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            kwargs["sasl_plain_username"] = "ai-superuser"
        if self.sasl_password:
            kwargs["sasl_plain_password"] = self.sasl_password.get_secret_value()
        return kwargs


class LoggingSettings(BaseConfigSettings):
    """Logging configuration"""
    
    level: str = Field(default="INFO", description="Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
    format: str = Field(default="json", description="Log format (json, text)")
    
    # File logging
    file_enabled: bool = Field(default=False, description="Enable file logging")
    file_path: str = Field(default="logs/app.log", description="Log file path")
    file_rotation: str = Field(default="1 day", description="Log rotation policy")
    file_retention: str = Field(default="30 days", description="Log retention policy")
    
    # Structured logging
    structured: bool = Field(default=True, description="Enable structured logging")
    
    

    @field_validator('level', mode='before')
    def _validate_level(cls, v):
        if not v:
            return v
        lvl = str(v).upper()
        valid = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
        if lvl not in valid:
            raise ValueError(f'Invalid logging level: {v}')
        return lvl


class MetricsSettings(BaseConfigSettings):
    """Prometheus metrics configuration"""

    enabled: bool = Field(default=True, description="Enable Prometheus metrics")
    host: str = Field(default="0.0.0.0", description="Prometheus metrics HTTP server bind host")
    port: int = Field(default=9090, description="Prometheus metrics HTTP server port")


class LangfuseSettings(BaseConfigSettings):
    """Langfuse observability configuration"""
    
    enabled: bool = Field(default=False, description="Enable Langfuse observability")
    secret_key: Optional[str] = Field(default=None, description="Langfuse secret key")
    public_key: Optional[str] = Field(default=None, description="Langfuse public key")
    host: str = Field(default="https://cloud.langfuse.com", description="Langfuse host URL")
    environment: Optional[str] = Field(default=None, description="Environment tag for traces (e.g. production, staging)")
    release: str = Field(default="1.0.0", description="Release version")
    debug: bool = Field(default=False, description="Enable debug mode")
    sample_rate: float = Field(default=1.0, description="Sampling rate (0.0 to 1.0)")
    timeout: int = Field(default=10, description="Timeout in seconds")
    max_retries: int = Field(default=3, description="Maximum number of retries")
    backoff_factor: float = Field(default=2.0, description="Backoff factor for retries")
    
    @field_validator('sample_rate', mode='before')
    def _validate_sample_rate(cls, v):
        if v is None:
            return v
        rate = float(v)
        if not 0.0 <= rate <= 1.0:
            raise ValueError('sample_rate must be between 0.0 and 1.0')
        return rate


class ApiSettings(BaseConfigSettings):
    """HTTP API server configuration"""

    host: str = Field(default="0.0.0.0", description="HTTP API bind host")
    port: int = Field(default=8000, description="HTTP API port")
    agent_config_path: str = Field(
        default="agent_config/qc_agent.yml",
        description="Path to the QC agent YAML configuration served over HTTP",
    )


class Settings(BaseConfigSettings):
    """Main settings class that combines all configurations"""

    # Core settings
    environment: str = Field(default="development", description="Environment (development, staging, production)")
    debug: bool = Field(default=True, description="Debug mode")
    version: str = Field(default="0.1.0", description="Application version")
    
    # Component configurations
    llms: LLMsSettings = Field(default_factory=LLMsSettings, description="LLM provider settings")
    kafka: KafkaSettings = Field(default_factory=KafkaSettings, description="Kafka settings")
    api: ApiSettings = Field(default_factory=ApiSettings, description="HTTP API server settings")
    logging: LoggingSettings = Field(default_factory=LoggingSettings, description="Logging settings")
    langfuse: LangfuseSettings = Field(default_factory=LangfuseSettings, description="Langfuse observability settings")
    metrics: MetricsSettings = Field(default_factory=MetricsSettings, description="Prometheus metrics settings")
    
    # Application settings
    agent_config_path: str = Field(default="agent_config", description="Path to agent configuration directory")
    custom_tools_path: str = Field(default="agent_config/custom_tools", description="Path to custom tools directory")
    custom_nodes_path: str = Field(default="agent_config/custom_nodes", description="Path to custom nodes directory")
    
    
    model_config = SettingsConfigDict(
        env_nested_delimiter='__',
        case_sensitive=False,
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )
    
    @field_validator('environment', mode='before')
    def validate_environment(cls, v):
        valid_environments = ['development', 'staging', 'production', 'test']
        if v not in valid_environments:
            raise ValueError(f'Environment must be one of: {valid_environments}')
        return v
    # Note: rate limit and logging level validation handled on the respective nested classes


# Global settings instance
_settings_instance: Optional[Settings] = None


def get_settings() -> Settings:
    """
    Get the global settings instance.
    Creates a new instance if one doesn't exist.
    """
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()
    return _settings_instance


def reload_settings() -> Settings:
    """
    Reload settings from environment variables.
    Useful for testing or configuration changes.
    """
    global _settings_instance
    _settings_instance = Settings()
    return _settings_instance
