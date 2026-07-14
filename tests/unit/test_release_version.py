import pytest

from translator_ingest.util.metadata import next_release_version


@pytest.mark.parametrize(
    "previous, expected",
    [
        # No prior release, or a prior release without a valid semantic version,
        # starts versioning fresh at 1.0.0.
        (None, "1.0.0"),
        ("", "1.0.0"),
        ("2026_06_10", "1.0.0"),  # legacy date-based version
        ("v1.0.0", "1.0.0"),  # leading 'v' is not valid semver here
        ("1.0", "1.0.0"),  # not MAJOR.MINOR.PATCH
        ("1.0.0-rc1", "1.0.0"),  # pre-release suffix not supported
        # A valid semantic version bumps the patch component.
        ("1.0.0", "1.0.1"),
        ("1.0.1", "1.0.2"),
        ("1.2.9", "1.2.10"),  # patch does not roll over into minor
        ("2.5.99", "2.5.100"),
        ("10.20.30", "10.20.31"),
    ],
)
def test_next_release_version(previous, expected):
    """The patch component is bumped, or versioning starts at 1.0.0 when no
    valid semantic version precedes it."""
    assert next_release_version(previous) == expected


def test_next_release_version_is_repeatable():
    """Repeated calls on subsequent versions produce a monotonic patch sequence."""
    version = next_release_version(None)
    assert version == "1.0.0"
    for expected in ("1.0.1", "1.0.2", "1.0.3"):
        version = next_release_version(version)
        assert version == expected