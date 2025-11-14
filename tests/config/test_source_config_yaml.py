import pytest
import yaml

from koza import KozaConfig
from koza.io.yaml_loader import UniqueIncludeLoader

from ..util import get_ingest_config_yaml_path, get_ingest_rig_yaml_path, ALL_SOURCE_IDS


@pytest.mark.parametrize("source_id", ALL_SOURCE_IDS)
def test_valid_source_config_yaml(source_id):
    # get the path to a specific source config yaml file
    config_yaml_file_path = get_ingest_config_yaml_path(source_id)
    if config_yaml_file_path is None:
        # warnings.warn(f"An ingest directory exists for {source_id} but a config yaml was not found.", UserWarning)
        return

    # this is how koza opens and parses the yaml
    with config_yaml_file_path.open("r") as fh:
        config_dict = yaml.load(fh, Loader=UniqueIncludeLoader.with_file_base(str(config_yaml_file_path)))  # noqa: S506
        # just initializing the KozaConfig will run pydantic validation and catch a lot of config issues
        KozaConfig(**config_dict)


@pytest.mark.parametrize("source_id", ALL_SOURCE_IDS)
def test_valid_rig_yaml(source_id):
    # get the path to a specific rig yaml file
    rig_yaml_file_path = get_ingest_rig_yaml_path(source_id)
    if rig_yaml_file_path is None:
        raise FileNotFoundError(f"rig file not found for {source_id} - it should be named {source_id}_rig.yaml")

    with rig_yaml_file_path.open("r") as rig_file:
        rig_data = yaml.safe_load(rig_file)
        assert "source_info" in rig_data
        assert "ingest_info" in rig_data
        assert "provenance_info" in rig_data


