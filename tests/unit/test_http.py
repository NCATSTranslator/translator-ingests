from src.translator_ingest.util.http import post_query

from src.translator_ingest.util.normalize import Normalizer

def test_post_invalid_url_query():
    returned: dict = post_query(url="http://fake-url", query={})
    assert not returned


def test_post_query():
    returned: dict = post_query(url=Normalizer.NODE_NORMALIZER_SERVER, query={'curies': ["HGNC:12791"]})
    assert "HGNC:12791" in returned.keys()
