from pathlib import Path


def find_project_root(start: Path = Path(__file__)) -> Path:
    current = start.resolve()
    for parent in [current] + list(current.parents):
        if (parent / "pyproject.toml").exists():
            return parent
    raise FileNotFoundError("Could not find project root from starting point.")

ingest_dir_ignore_list = ["__pycache__"]
def get_all_source_ids():
    ingest_dir = find_project_root() / "src" / "translator_ingest" / "ingests"
    return [p.name for p in ingest_dir.iterdir() if p.is_dir() and p.name not in ingest_dir_ignore_list]

ALL_SOURCE_IDS = get_all_source_ids()

# get the absolute path for an ingest config yaml file
# source_id: the name of the source corresponding to a translator_ingest directory
def get_ingest_config_yaml_path(source_id: str) -> Path | None:
    yaml_file_name = source_id + ".yaml"
    top_level_dir = find_project_root()
    config_yaml_file_path = top_level_dir / "src" / "translator_ingest" / "ingests" / source_id / yaml_file_name
    return config_yaml_file_path if config_yaml_file_path.exists() else None
