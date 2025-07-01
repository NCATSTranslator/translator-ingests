from typing import Dict

# def post_query(url: str, query: Dict, params=None, server: str = "") -> Dict:
from src.translator_ingest.util.http import post_query
from src.translator_ingest.util.normalize import NODE_NORMALIZER_SERVER

def test_post_invalid_url_query():
    returned: Dict = post_query(url="http://fake-url", query={})
    assert not returned


def test_post_query():
    returned: Dict = post_query(url=NODE_NORMALIZER_SERVER, query={'curies': ["HGNC:12791"]})
    assert "HGNC:12791" in returned.keys()
