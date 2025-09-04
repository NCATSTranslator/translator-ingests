import pytest
import yaml

from koza import KozaConfig
from koza.io.yaml_loader import UniqueIncludeLoader

from ..util import get_ingest_config_yaml_path


@pytest.mark.parametrize("source_id",
[
    "ctd",
    "go_cam",
    "goa"
])
def test_valid_source_config_yaml(source_id):
    # get the path to a specific source config yaml file
    config_yaml_file_path = get_ingest_config_yaml_path(source_id)
    # this is how koza opens and parses the yaml
    with config_yaml_file_path.open("r") as fh:
        config_dict = yaml.load(fh, Loader=UniqueIncludeLoader.with_file_base(str(config_yaml_file_path)))  # noqa: S506
        # just initializing the KozaConfig will run pydantic validation and catch a lot of config issues
        KozaConfig(**config_dict)