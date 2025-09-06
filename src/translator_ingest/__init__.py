"""Translator Phase 3 Data Ingest Pipeline."""

try:
    from importlib.metadata import version
    __version__ = version("bridge")
except Exception:
    __version__ = "0.0.0"