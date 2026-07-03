"""
Configuration loading utilities.
Handles YAML configuration loading, environment variable substitution, and validation.
"""

import os
import re
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Union, Tuple
from dotenv import load_dotenv
from jinja2 import Template

from .app_config import (
    AgentConfig,
    PromptConfig,
    ConfigFile,
    ConfigValidationResult,
    EnvironmentMapping,
)
from .settings import get_settings

logger = logging.getLogger(__name__)


# Environment variable mapping for configuration substitution
ENV_MAPPINGS = [
    # LLMs
    EnvironmentMapping(
        source="LLMS__OPENAI_API_KEY", target="models.api_key", required=False
    ),
    EnvironmentMapping(
        source="LLMS__ANTHROPIC_API_KEY",
        target="llms.anthropic_api_key",
        required=False,
    ),
    EnvironmentMapping(
        source="LLMS__GOOGLE_API_KEY", target="llms.google_api_key", required=False
    ),
    # API
    EnvironmentMapping(
        source="API__SECRET_KEY", target="api.secret_key", required=False
    ),
]


def load_dotenv_files(env_dir: str = "agent_config") -> None:
    """
    Load environment variables from .env files in the specified directory.

    Args:
        env_dir: Directory to search for .env files
    """
    env_path = Path(env_dir)
    env_files = [
        env_path / ".env",
        env_path / f".env.{os.getenv('ENVIRONMENT', 'development')}",
        env_path / ".env.local",
    ]

    for env_file in env_files:
        if env_file.exists():
            logger.info(f"Loading environment variables from {env_file}")
            load_dotenv(env_file)


def substitute_env_vars(
    config_data: Dict[str, Any], env_mappings: Optional[list] = None
) -> Dict[str, Any]:
    """
    Substitute environment variables in configuration data.

    Args:
        config_data: Configuration dictionary to process
        env_mappings: Optional custom environment variable mappings

    Returns:
        Configuration with environment variables substituted
    """
    if env_mappings is None:
        env_mappings = ENV_MAPPINGS

    # First pass: substitute ${VAR} patterns
    config_str = yaml.dump(config_data)

    # Environment variable substitution pattern: ${VAR_NAME}
    def replace_env_var(match):
        var_name = match.group(1)
        value = os.getenv(var_name)
        if value is None:
            logger.warning(f"Environment variable '{var_name}' not found")
            return match.group(0)  # Return original if not found
        return str(value)

    # Substitute ${ENV_VAR} patterns
    config_str = re.sub(r"\$\{([^}]+)\}", replace_env_var, config_str)

    # Parse back to dict
    config_data = yaml.safe_load(config_str)

    # Second pass: apply custom mappings
    for mapping in env_mappings:
        value = os.getenv(mapping.source)
        if value is not None:
            _set_nested_value(config_data, mapping.target, value)
        elif mapping.required and mapping.default is None:
            logger.error(f"Required environment variable '{mapping.source}' not found")

    return config_data


def _set_nested_value(data: Dict[str, Any], path: str, value: Any) -> None:
    """
    Set a value in a nested dictionary using dot notation.

    Args:
        data: Dictionary to modify
        path: Dot-separated path to the value (e.g., "models.api_key")
        value: Value to set
    """
    keys = path.split(".")
    current = data

    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]

    current[keys[-1]] = value


def _get_nested_value(data: Dict[str, Any], path: str, default: Any = None) -> Any:
    """
    Get a value from a nested dictionary using dot notation.

    Args:
        data: Dictionary to search
        path: Dot-separated path to the value
        default: Default value if not found

    Returns:
        Value at the path or default
    """
    keys = path.split(".")
    current = data

    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default

    return current


def load_yaml_config(config_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Load YAML configuration file.

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        Loaded configuration dictionary

    Raises:
        FileNotFoundError: If configuration file doesn't exist
        yaml.YAMLError: If YAML parsing fails
    """
    config_file = Path(config_path)

    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)

        logger.info(f"Loaded configuration from {config_path}")
        return config_data

    except yaml.YAMLError as e:
        logger.error(f"Failed to parse YAML configuration: {e}")
        raise


def validate_config(config_data: Dict[str, Any]) -> ConfigValidationResult:
    """
    Validate configuration using Pydantic models.

    Args:
        config_data: Configuration dictionary to validate

    Returns:
        Validation result with errors and processed configuration
    """
    errors: list[str] = []
    warnings: list[str] = []

    try:
        # Validate as ConfigFile first
        config_obj = ConfigFile.model_validate(config_data)
        processed_config = config_obj.model_dump()

        # Additional validations
        _validate_agent_config(processed_config, errors, warnings)

        is_valid = len(errors) == 0

        return ConfigValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            processed_config=processed_config if is_valid else None,
        )

    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        errors.append(str(e))

        return ConfigValidationResult(
            is_valid=False, errors=errors, warnings=warnings, processed_config=None
        )


def _validate_agent_config(
    config: Dict[str, Any], errors: list, warnings: list
) -> None:
    """
    Perform additional agent-specific validations.

    Args:
        config: Configuration dictionary
        errors: List to add validation errors
        warnings: List to add validation warnings
    """
    # Check if tools directory exists
    settings = get_settings()
    custom_tools_path = Path(settings.custom_tools_path)
    if not custom_tools_path.exists():
        warnings.append(f"Custom tools directory not found: {custom_tools_path}")

    # Check if custom nodes directory exists
    custom_nodes_path = Path(settings.custom_nodes_path)
    if not custom_nodes_path.exists():
        warnings.append(f"Custom nodes directory not found: {custom_nodes_path}")

    # Validate tool references
    if "tools" in config:
        missing_tools = []
        for tool_name in config["tools"]:
            if not _tool_exists(tool_name, settings):
                missing_tools.append(tool_name)

        if missing_tools:
            warnings.append(f"Referenced tools not found: {missing_tools}")


def _tool_exists(tool_name: str, settings) -> bool:
    """
    Check if a tool exists (built-in or custom).

    Args:
        tool_name: Name of the tool to check
        settings: Application settings

    Returns:
        True if tool exists, False otherwise
    """
    # Check built-in tools (simplified check)
    built_in_tools = ["calculator", "search", "email", "vision"]
    if tool_name in built_in_tools:
        return True

    # Check custom tools directory
    custom_tools_path = Path(settings.custom_tools_path)
    if custom_tools_path.exists():
        tool_files = list(custom_tools_path.glob("*.py"))
        for tool_file in tool_files:
            if tool_name in tool_file.stem or tool_name in tool_file.name:
                return True

    return False


def load_prompts(prompts_path: Union[str, Path]) -> PromptConfig:
    """
    Load and validate prompt configuration.

    Args:
        prompts_path: Path to prompts YAML file

    Returns:
        Validated prompt configuration
    """
    prompts_data = load_yaml_config(prompts_path)

    # Substitute environment variables in prompts
    prompts_data = substitute_env_vars(prompts_data)

    # Validate with Pydantic
    try:
        prompt_config = PromptConfig.model_validate(prompts_data)
        logger.info(f"Loaded prompts from {prompts_path}")
        return prompt_config
    except Exception as e:
        logger.error(f"Failed to validate prompts configuration: {e}")
        raise


def load_configs(config_dir: str = "agent_config") -> Tuple[AgentConfig, PromptConfig]:
    """
    Load complete agent configuration (config.yml and prompts.yml).

    Args:
        config_dir: Directory containing configuration files

    Returns:
        Tuple of (agent_config, prompt_config)

    Raises:
        FileNotFoundError: If required files don't exist
        ValidationError: If configuration validation fails
    """
    config_path = Path(config_dir)

    # Load environment variables
    load_dotenv_files(config_dir)

    # Load agent configuration
    agent_config_file = config_path / "config.yml"
    if not agent_config_file.exists():
        raise FileNotFoundError(f"Agent configuration not found: {agent_config_file}")

    agent_data = load_yaml_config(agent_config_file)

    # Substitute environment variables
    agent_data = substitute_env_vars(agent_data)

    # Validate agent configuration
    validation_result = validate_config(agent_data)
    if not validation_result.is_valid:
        error_msg = "Agent configuration validation failed:\n" + "\n".join(
            validation_result.errors
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Load prompts configuration
    prompts_file = config_path / "prompts.yml"
    if prompts_file.exists():
        prompt_config = load_prompts(prompts_file)
    else:
        logger.warning(f"Prompts file not found: {prompts_file}, using empty prompts")
        prompt_config = PromptConfig(prompts={})

    # Add validation warnings to logs
    for warning in validation_result.warnings:
        logger.warning(f"Configuration warning: {warning}")

    if validation_result.processed_config is None:
        raise ValueError("Configuration validation failed")
    return validation_result.processed_config["agent"], prompt_config


# Export main functions
__all__ = [
    "load_configs",
    "load_prompts",
    "load_yaml_config",
    "validate_config",
    "substitute_env_vars",
    "load_dotenv_files",
]
