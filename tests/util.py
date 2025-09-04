
from pathlib import Path

def find_project_root(start: Path = Path(__file__)) -> Path:
    current = start.resolve()
    for parent in [current] + list(current.parents):
        if (parent / "pyproject.toml").exists():
            return parent
    raise FileNotFoundError("Could not find project root from starting point.")

# get the absolute path for an ingest config yaml file
# source_id: the name of the source corresponding to a translator_ingest directory
def get_ingest_config_yaml_path(source_id):
    yaml_file_name = source_id + ".yaml"
    top_level_dir = find_project_root()
    config_yaml_file_path = top_level_dir / "src" / "translator_ingest" / "ingests" / source_id / yaml_file_name
    return config_yaml_file_path

