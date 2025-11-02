import yaml
from pathlib import Path


def load_pipeline_config(config_path: str) -> dict:
    """Load the pipeline configuration from a YAML file."""
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config


def load_project_config(base_dir: Path) -> dict:
    """Load the project configuration from a TOML file."""
    config_path = base_dir / ".simpletl/config.toml"
    # Use a TOML parser library like toml or tomllib to load the config
    import toml

    with open(config_path, "r") as file:
        config = toml.load(file)
    return config
