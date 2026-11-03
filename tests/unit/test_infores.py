"""Tests for infores id collection and checking against an infores catalog."""

from collections import Counter

import pytest

from translator_ingest.util.infores import (
    InforesCatalog,
    InforesStatus,
    InforesValidationResult,
    check_infores_ids,
    get_edge_infores_ids,
)


@pytest.fixture
def catalog() -> InforesCatalog:
    return InforesCatalog(
        version="1.0.0",
        url="https://example.org/infores_catalog.yaml",
        statuses={
            "infores:bgee": "released",
            "infores:medlineplus": "released",
            "infores:diseases": "released",
            "infores:changed": "modified",
            "infores:old": "deprecated",
        },
    )


@pytest.mark.parametrize(
    "edge,expected",
    [
        ({"subject": "A:1", "predicate": "biolink:related_to", "object": "B:2"}, set()),
        ({"sources": []}, set()),
        ({"sources": None}, set()),
        (
            {"sources": [{"resource_id": "infores:bgee", "resource_role": "primary_knowledge_source"}]},
            {"infores:bgee"},
        ),
        (
            {
                "sources": [
                    {"resource_id": "infores:medlineplus", "resource_role": "primary_knowledge_source"},
                    {
                        "resource_id": "infores:diseases",
                        "resource_role": "aggregator_knowledge_source",
                        "upstream_resource_ids": ["infores:medlineplus"],
                    },
                ]
            },
            {"infores:medlineplus", "infores:diseases"},
        ),
        (
            {
                "sources": [
                    {
                        "resource_id": "infores:primary",
                        "resource_role": "primary_knowledge_source",
                        "upstream_resource_ids": ["infores:supporting-a", "infores:supporting-b"],
                    },
                    {"resource_id": "infores:supporting-a", "resource_role": "supporting_data_source"},
                    {"resource_id": "infores:supporting-b", "resource_role": "supporting_data_source"},
                ]
            },
            {"infores:primary", "infores:supporting-a", "infores:supporting-b"},
        ),
        (
            # an upstream id that has no RetrievalSource of its own is still collected
            {
                "sources": [
                    {
                        "resource_id": "infores:aggregator",
                        "resource_role": "aggregator_knowledge_source",
                        "upstream_resource_ids": ["infores:only-upstream"],
                    }
                ]
            },
            {"infores:aggregator", "infores:only-upstream"},
        ),
        (
            {"sources": [{"resource_id": "infores:bgee", "upstream_resource_ids": None}]},
            {"infores:bgee"},
        ),
        (
            # null and empty ids are collected as "" so they are reported as invalid
            {"sources": [{"resource_id": None, "upstream_resource_ids": [None, ""]}]},
            {""},
        ),
        (
            {"sources": [{"resource_id": "infores:bgee", "upstream_resource_ids": [None]}]},
            {"infores:bgee", ""},
        ),
    ],
)
def test_get_edge_infores_ids(edge: dict, expected: set[str]):
    assert get_edge_infores_ids(edge) == expected


def test_get_edge_infores_ids_ignores_retrieval_source_id():
    edge = {"sources": [{"id": "infores:record-id", "resource_id": "infores:bgee"}]}
    assert get_edge_infores_ids(edge) == {"infores:bgee"}


@pytest.mark.parametrize(
    "infores_id,expected",
    [
        ("infores:bgee", InforesStatus.VALID),
        ("infores:changed", InforesStatus.VALID),
        ("infores:old", InforesStatus.DEPRECATED),
        ("infores:not-in-catalog", InforesStatus.INVALID),
        ("bgee", InforesStatus.INVALID),
    ],
)
def test_get_infores_status(catalog: InforesCatalog, infores_id: str, expected: InforesStatus):
    assert catalog.get_infores_status(infores_id) == expected


def test_check_infores_ids(catalog: InforesCatalog):
    counts = Counter({"infores:bgee": 10, "infores:old": 2, "infores:typo": 1, "infores:another-typo": 5})
    assert check_infores_ids(counts, catalog) == InforesValidationResult(
        invalid={"infores:another-typo": 5, "infores:typo": 1},
        deprecated={"infores:old": 2},
        distinct_count=4,
    )


def test_check_infores_ids_all_valid(catalog: InforesCatalog):
    result = check_infores_ids(Counter({"infores:bgee": 3, "infores:diseases": 1}), catalog)
    assert result == InforesValidationResult(invalid={}, deprecated={}, distinct_count=2)


def test_check_infores_ids_counts_across_edges(catalog: InforesCatalog):
    edges = [
        {"sources": [{"resource_id": "infores:typo"}]},
        {"sources": [{"resource_id": "infores:bgee", "upstream_resource_ids": ["infores:typo"]}]},
        {"sources": [{"resource_id": "infores:old"}]},
    ]
    counts = Counter()
    for edge in edges:
        counts.update(get_edge_infores_ids(edge))
    result = check_infores_ids(counts, catalog)
    assert result.invalid == {"infores:typo": 2}
    assert result.deprecated == {"infores:old": 1}


def test_provenance(catalog: InforesCatalog):
    assert catalog.provenance() == {
        "repository": "biolink/information-resource-registry",
        "version": "1.0.0",
        "url": "https://example.org/infores_catalog.yaml",
    }