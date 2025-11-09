from yaml import safe_load

from .pipeline import Pipeline as Pipeline


def load_config(config_path: str):
    with open(config_path) as f:
        content = f.read()

    return safe_load(content)
