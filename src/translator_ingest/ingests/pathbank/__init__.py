def get_latest_version() -> str:
    """Return latest available PathBank source version."""

    from .pathbank import get_latest_version as _get_latest_version

    return _get_latest_version()

__all__ = ["get_latest_version"]
