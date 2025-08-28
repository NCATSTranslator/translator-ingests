from typing import Optional
import pytest

from biolink_model.datamodel.pydanticmodel_v2 import RetrievalSource, ResourceRoleEnum
from src.translator_ingest.util.biolink import build_association_knowledge_sources


@pytest.mark.parametrize(
    "primary,supporting,aggregating,expected_sources",
    [
        (  # Query 0 - empty primary ks
                "",None,None,None
        ),
        (  # Query 1 - only primary ks
            "foo",
            None,
            None,
            [
                RetrievalSource(
                    id="bar",
                    resource_id="infores:foo",
                    resource_role=ResourceRoleEnum.primary_knowledge_source
                )
            ]
        ),
        (  # Query 2 - primary ks with supporting data sources
            "foo",

            # list[supporting], pointing back to primary
            ["infores:tweedle-dee", "tweedle-dum"],
            None,
            [
                RetrievalSource(
                    id="bar",
                    resource_id="infores:foo",
                    resource_role=ResourceRoleEnum.primary_knowledge_source,
                    upstream_resource_ids=["infores:tweedle-dee","infores:tweedle-dee"]
                ),
                RetrievalSource(
                    id="fairytale1",
                    resource_id="infores:tweedle-dee",
                    resource_role=ResourceRoleEnum.supporting_data_source
                ),
                RetrievalSource(
                    id="fairytale1",
                    resource_id="infores:tweedle-dum",
                    resource_role=ResourceRoleEnum.supporting_data_source
                )
            ]
        ),
        #   # Query 3 - primary ks with aggregating
        (
            "foo",
            None,

            # dict[aggregating, list[upstream_resource_ids]], pointing back to primary
            {"humpty-dumpty": ["foo"]},
            [
                RetrievalSource(
                    id="bar",
                    resource_id="infores:foo",
                    resource_role=ResourceRoleEnum.primary_knowledge_source
                ),
                RetrievalSource(
                    id="fairytale3",
                    resource_id="infores:humpty-dumpty",
                    resource_role=ResourceRoleEnum.aggregator_knowledge_source,
                    upstream_resource_ids=["infores:foo"]
                )
            ]
        )
    ],
)
def test_build_association_knowledge_sources(
    primary: str,
    supporting: Optional[list[str]],
    aggregating: Optional[dict[str, list[str]]],
    expected_sources: Optional[list[RetrievalSource]]
):
    sources: list[RetrievalSource] = build_association_knowledge_sources(primary, supporting, aggregating)
    for source in sources:
        assert any(
            [
                source.resource_id == entry.resource_id and
                source.resource_role == entry.resource_role
                for entry in expected_sources
            ]
        )
        # Check that upstream resource IDs are set correctly
        if source.upstream_resource_ids is not None:
            for upstream_id in source.upstream_resource_ids:
                assert any(
                    [
                        upstream_id == entry.resource_id
                        for entry in sources
                    ]
                )
