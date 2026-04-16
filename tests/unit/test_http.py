from typing import Dict
from unittest.mock import patch, Mock

from src.translator_ingest.util.http_utils import post_query


def test_post_invalid_url_query():
    returned: Dict = post_query(url="http://fake-url", query={})
    assert not returned


def test_post_query():
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "HGNC:12791": {
            "id": {"identifier": "HGNC:12791", "label": "TP53"},
            "type": ["biolink:Gene"],
        }
    }

    with patch("translator_ingest.util.http_utils.requests.post", return_value=mock_response):
        returned: Dict = post_query(
            url="https://mock-url.com/get_normalized_nodes",
            query={"curies": ["HGNC:12791"]},
        )
    assert "HGNC:12791" in returned.keys()
    assert returned["HGNC:12791"]["id"]["identifier"] == "HGNC:12791"
