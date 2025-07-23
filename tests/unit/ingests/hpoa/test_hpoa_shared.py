"""
Testing of shared HPOA code
"""

from src.translator_ingest.ingests.hpoa import get_latest_version

def test_hpoa_version():
    version = get_latest_version()
    assert version is not None and version == "2025-05-06"
