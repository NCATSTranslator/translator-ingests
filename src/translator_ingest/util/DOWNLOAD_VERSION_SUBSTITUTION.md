# Download URL Version Substitution

The `download_utils.substitute_version_in_download_yaml()` function allows download.yaml files to use version placeholders that are automatically substituted with the actual version from `get_latest_version()` before downloading.

## Usage

### In download.yaml

Simply use `{version}` as a placeholder in your URLs:

```yaml
---
- url: https://example.com/releases/{version}/data.tsv.gz
  local_name: data.tsv.gz
- url: https://example.com/files/dataset_{version}.zip
  local_name: dataset.zip
```

### In your ingest module

Implement `get_latest_version()` as usual:

```python
def get_latest_version() -> str:
    """Return the version string to use in download URLs."""
    return "2024-01-15"  # Or fetch dynamically
```

### Automatic substitution

The pipeline automatically calls `substitute_version_in_download_yaml()` in the `download()` function, so:

1. `{version}` in download.yaml is replaced with the value from `get_latest_version()`
2. The substituted YAML is used by kghub_downloader
3. Files are downloaded from the versioned URLs

## Example: BindingDB

For an ingest that needs version-specific downloads:

**download.yaml:**
```yaml
---
- url: https://www.bindingdb.org/bind/downloads/BindingDB_All_{version}_tsv.zip
  local_name: BindingDB_All.zip
```

**bindingdb.py:**
```python
def get_latest_version() -> str:
    """Get the latest BindingDB version."""
    # Could fetch from API, web scraping, or use current date
    from datetime import datetime
    return datetime.now().strftime("%Y%m")  # e.g., "202401"
```

**Result:**
When the pipeline runs, it will download from:
```
https://www.bindingdb.org/bind/downloads/BindingDB_All_202401_tsv.zip
```

## Custom Placeholders

You can use custom placeholder strings if needed:

```python
from translator_ingest.util.download_utils import substitute_version_in_download_yaml

temp_yaml = substitute_version_in_download_yaml(
    "download.yaml",
    version="v2.0",
    placeholder="VERSION"  # Instead of default "{version}"
)
```

## Notes

- If no `{version}` placeholder is found, the original download.yaml is used unchanged
- The function creates a temporary YAML file with substituted URLs
- The temporary file is automatically used by kghub_downloader
- Multiple URLs can use the same or different placeholders
- Version substitution happens **before** kghub_downloader checks if files exist
