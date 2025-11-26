from typing import Any, Dict

import pytest
import yaml

from src.config import Config


@pytest.fixture
def write_test_config(tmp_path):
    def _write(data: Dict[str, Any]):
        path = tmp_path / "config.yaml"
        path.write_text(yaml.safe_dump(data, sort_keys=False))
        return path

    return _write


def test_load_data(write_test_config):
    path = write_test_config({"hello": "world"})
    loaded_data = Config._load_data(config_path=path)
    assert loaded_data == {"hello": "world"}


def test_load_data_bad_path_raises(write_test_config):
    BAD_PATH = "not/a/real/path.yaml"
    write_test_config({"hello": "world"})
    with pytest.raises(Config.ConfigLoadError, match=f'Config file "{BAD_PATH}" could not be found.'):
        Config._load_data(BAD_PATH)


def test_load_data_invalid_yaml_raises(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.dump("list: [1, 2, 3", sort_keys=False))
    with pytest.raises(Config.ConfigLoadError, match=f"YAML parsing error"):
        data = Config._load_data(path)
