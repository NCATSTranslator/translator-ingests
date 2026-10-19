"""Tests for publishing graph metadata under the translator-ingests vocabulary instead of ORION's."""
import pytest

from orion import (KGXGraphMetadata, KGXKnowledgeGraphSource, ORION_BABEL_VERSION, ORION_BIOLINK_VERSION,
                   ORION_BUILD_VERSION, ORION_EDGE_COUNT, ORION_NODE_COUNT)

from translator_ingest.util.metadata import (ORION_VOCABULARY, TRANSLATOR_BUILD_VERSION, TRANSLATOR_VOCABULARY,
                                             TRANSLATOR_VOCABULARY_URL, to_translator_graph_metadata)


def build_graph_metadata() -> KGXGraphMetadata:
    """Graph metadata carrying every ORION term, at both levels they appear at."""
    return KGXGraphMetadata(
        name="demo",
        version="1.0.0",
        build_version="demo_build",
        biolink_version="4.4.4",
        babel_version="2025sep1",
        kg_sources=[KGXKnowledgeGraphSource(id="https://example.test/releases/source_a/1.0.0/",
                                            name="source_a",
                                            release_version="1.0.0",
                                            build_version="source_a_build",
                                            node_count=2,
                                            edge_count=1)],
    )


@pytest.mark.parametrize("orion_term, expected_value", [
    (ORION_BUILD_VERSION, "demo_build"),
    (ORION_BIOLINK_VERSION, "4.4.4"),
    (ORION_BABEL_VERSION, "2025sep1"),
])
def test_top_level_orion_terms_are_republished_as_translator_terms(orion_term, expected_value):
    """The versions describing the graph itself are published under the translator vocabulary."""
    graph_metadata = to_translator_graph_metadata(build_graph_metadata())

    translator_term = orion_term.replace(f"{ORION_VOCABULARY}:", f"{TRANSLATOR_VOCABULARY}:")
    assert graph_metadata[translator_term] == expected_value
    assert orion_term not in graph_metadata


@pytest.mark.parametrize("orion_term, expected_value", [
    (ORION_BUILD_VERSION, "source_a_build"),
    (ORION_NODE_COUNT, 2),
    (ORION_EDGE_COUNT, 1),
])
def test_nested_orion_terms_are_republished_as_translator_terms(orion_term, expected_value):
    """Terms nested inside hasPart entries are renamed too, not just the top level ones."""
    graph_metadata = to_translator_graph_metadata(build_graph_metadata())
    part = graph_metadata["hasPart"][0]

    translator_term = orion_term.replace(f"{ORION_VOCABULARY}:", f"{TRANSLATOR_VOCABULARY}:")
    assert part[translator_term] == expected_value
    assert orion_term not in part


def test_context_declares_the_translator_vocabulary_instead_of_orions():
    """A vocabulary used by the terms has to be declared in the context for them to resolve."""
    context = to_translator_graph_metadata(build_graph_metadata())["@context"]

    assert context[TRANSLATOR_VOCABULARY] == TRANSLATOR_VOCABULARY_URL
    assert ORION_VOCABULARY not in context


def test_terms_of_other_vocabularies_are_left_alone():
    """Only ORION's terms are renamed. Everything else, including schema.org terms, is published as is."""
    graph_metadata = to_translator_graph_metadata(build_graph_metadata())

    assert graph_metadata["name"] == "demo"
    assert graph_metadata["version"] == "1.0.0"
    assert graph_metadata["@context"]["biolink"] == "https://w3id.org/biolink/"
    assert graph_metadata["hasPart"][0]["@id"] == "https://example.test/releases/source_a/1.0.0/"


def test_release_version_and_build_version_stay_distinguishable():
    """The two versions are easy to confuse, so the renamed build version must not collide with
    the release version published as the plain schema.org "version"."""
    graph_metadata = to_translator_graph_metadata(build_graph_metadata())

    assert graph_metadata["version"] == "1.0.0"
    assert graph_metadata[TRANSLATOR_BUILD_VERSION] == "demo_build"