import pytest
from simpletl.config_utils import load_pipeline_config, load_project_config
from pathlib import Path
import yaml
def test_load_pipeline_config():
    config_path = 'sales_config.yml'
    config = load_pipeline_config(config_path)
    assert isinstance(config, dict)
    assert config['name'] == 'sales_pipeline'
    assert config['source']['url'] == 'https://website.com/sales.csv'
    assert config['destinations']['delta']['bucket_url'] == 'my-bucket'
    assert config['destinations']['delta']['prefix'] == 'data/sales_pipeline'

def test_load_project_config(tmp_path):
    config_dict = {
        'version': '0.1.0',
        'author': 'Your Name'
    }
    config_path = tmp_path / '.simpletl/config.toml'
    config_path.parent.mkdir(parents=True)
    with open(config_path, 'w') as file:
        import toml
        toml.dump(config_dict, file)
    config = load_project_config(tmp_path)
    assert isinstance(config, dict)
    assert config['version'] == '0.1.0'
    assert config['author'] == 'Your Name'