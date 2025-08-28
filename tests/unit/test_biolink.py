from typing import List, Dict, Optional
import pytest

from src.translator_ingest.util.biolink import build_association_knowledge_sources


# def build_association_knowledge_sources(
#             primary: str,
#             supporting: Optional[List[str]] = None,
#             aggregating: Optional[Dict[str, List[str]]] = None
#         ) -> List[RetrievalSource]:
@pytest.mark.parametrize(
    "primary,supporting,aggregating",
    [
        ("",None,None),
    ],
)
def test_build_association_knowledge_sources(
    primary: str,
    supporting: Optional[list[str]],
    aggregating: Optional[Dict[str, list[str]]]
):
    pass
