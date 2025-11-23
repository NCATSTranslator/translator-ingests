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
4. Note that if the original download.yaml file has no version placeholders, it is returned (hence used) unchanged, hence some care needs to be taken to avoid accidentally deleting it when clean-up of possible temporary files is performed (see [Complete Example](#complete-example) below).

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

## Complete Example

```python
from translator_ingest.util.download_utils import substitute_version_in_download_yaml

# Substitute version placeholders in download.yaml if they exist
download_yaml_with_version = substitute_version_in_download_yaml(
    download_yaml_file,
    pipeline_metadata.source_version
)
# Get a path for the subdirectory for the source data
source_data_output_dir = get_source_data_directory(pipeline_metadata)
Path.mkdir(source_data_output_dir, exist_ok=True)
try:
    # Download the data
    # Don't need to check if file(s) already downloaded, kg downloader handles that
    kghub_download(yaml_file=str(download_yaml_with_version), output_dir=str(source_data_output_dir))
finally:
    # Clean up the specified download_yaml file if it exists and
    # is a temporary file with versioning resolved but is
    # **NOT** rather the original unmodified download.yaml!
    if download_yaml_with_version and \
            download_yaml_with_version != download_yaml_file:
        download_yaml_with_version.unlink(missing_ok=True)
```

## Summary Notes

- If no `{version}` placeholder is found, the original download.yaml is returned (hence used) unchanged
- In contrast, if a `{version}` placeholder is found, then the function creates a temporary YAML file with substituted URLs
- In such cases, the temporary file is the one automatically to be used by **kghub_downloader** in the **pipeline.py** module.
- In principle, multiple URLs can use the same or different placeholders, although this is not currently supported by the Translator Ingest Framework (i.e. only the default use of `{version}` is expected), but it is possible to implement such a feature if needed, say if a given source knowledge source happens to blissfully embed the 'version' substring in its URLs. However, the Translator Ingest Framework would need to be modified to support such a feature, e.g. by adding a `version_placeholder` parameter to **pipeline_metadata: PipelineMetadata** given to the `download()` function.
- Version substitution happens **before** kghub_downloader checks if files exist and attempts to download them.
- Code to clean up possible temporary YAML files should be designed carefully to avoid accidentally deleting the original download.yaml file!
