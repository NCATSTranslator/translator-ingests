import pytest
from koza.runner import KozaRunner

from tests.util import get_ingest_config_yaml_path


@pytest.mark.integration
def test_ubergraph_koza_runner_init(tmp_path):
    config_yaml_file_path = get_ingest_config_yaml_path("ubergraph")
    
    assert config_yaml_file_path is not None
    assert config_yaml_file_path.exists()
    
    config, runner = KozaRunner.from_config_file(str(config_yaml_file_path), output_dir=str(tmp_path))
    
    for tag, hooks in runner.hooks_by_tag.items():
        if not (hooks and (hooks.transform or hooks.transform_record)):
            pytest.fail(
                f"Transform function not recognized for ubergraph tag '{tag}'. "
                f"Must define a function decorated with `@koza.transform` or `@koza.transform_record`"
            )
    
    assert "redundant_graph" in runner.hooks_by_tag
    
    hooks = runner.hooks_by_tag["redundant_graph"]
    assert hooks.transform is not None
    assert hooks.prepare_data is not None
    assert hooks.on_data_begin is not None
    assert hooks.on_data_end is not None