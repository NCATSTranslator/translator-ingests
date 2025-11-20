import pytest

# import warnings

from koza.runner import KozaRunner

from ..util import get_ingest_config_yaml_path, ALL_SOURCE_IDS


@pytest.mark.parametrize("source_id", ALL_SOURCE_IDS)
def test_koza_runner_init_from_config(source_id, tmp_path):
    # get the path to a specific source config yaml file
    config_yaml_file_path = get_ingest_config_yaml_path(source_id)
    if config_yaml_file_path is None:
        # warnings.warn(f"An ingest directory exists for {source_id} but a config yaml was not found.", UserWarning)
        return

    # initialize a KozaRunner from the config file
    config, runner = KozaRunner.from_config_file(str(config_yaml_file_path), output_dir=str(tmp_path))
    # ensure at least one transform function was identified for every tag
    for tag, hooks in runner.hooks_by_tag.items():
        if not (hooks and (hooks.transform or hooks.transform_record)):
            pytest.fail(
                f"Transform function not recognized for {source_id}. "
                f"Must define a function decorated with `@koza.transform` or `@koza.transform_record`"
            )
