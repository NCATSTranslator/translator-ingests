"""Source links retain the exact pair independently of predicate and normalization."""

import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from koza import KozaTransform
from koza.io.writer.jsonl_writer import JSONLWriter
from koza.model.writer import WriterConfig

from translator_ingest.ingests.string.string import transform_string_ppi
from translator_ingest.ingests.string.string_utils import (
    string_network_url,
    string_source_record_url,
    string_version_from_source_dir,
)


@pytest.mark.parametrize(
    "protein1,protein2,taxon",
    [
        ("9606.ENSP00000000233", "9606.ENSP00000371175", "9606"),
        ("10090.ENSMUSP00000000002", "10090.ENSMUSP00000000001", "10090"),
        ("10116.ENSRNOP00000000002", "10116.ENSRNOP00000000001", "10116"),
    ],
)
def test_serialized_pair_links(tmp_path: Path, protein1: str, protein2: str, taxon: str) -> None:
    """Real Koza output preserves raw IDs for all three predicates and deduplicates reversal."""
    writer = JSONLWriter(str(tmp_path), "string", WriterConfig())
    context = KozaTransform(extra_fields={}, writer=writer, mappings={})
    context.state["string_to_entrez"] = {protein1: ["NCBIGene:123"]}
    context.state["string_version"] = "v12.0"
    record = {
        "protein1": protein1,
        "protein2": protein2,
        "combined_score": "900",
        "experiments": "800",
        "coexpression": "810",
    }
    graph = transform_string_ppi(context, record)
    assert graph is not None
    assert {edge.predicate for edge in graph.edges} == {
        "biolink:functionally_associated_with",
        "biolink:directly_physically_interacts_with",
        "biolink:coexpressed_with",
    }
    context.write(*graph.nodes, *graph.edges)
    writer.finalize()
    written = [json.loads(line) for line in (tmp_path / "string_edges.jsonl").read_text().splitlines()]
    urls = set()
    for edge in written:
        assert len(edge["sources"]) == 1
        source = edge["sources"][0]
        assert source["resource_id"] == "infores:string"
        assert source["resource_role"] == "primary_knowledge_source"
        assert len(source["source_record_urls"]) == 1
        urls.update(source["source_record_urls"])
    assert len(urls) == 1
    url = urls.pop()
    parsed = urlsplit(url)
    assert parsed.scheme == "https"
    assert parsed.netloc == "version-12-0.string-db.org"
    assert parsed.path == "/cgi/network"
    assert parse_qs(parsed.query) == {
        "identifiers": ["\r".join(sorted([protein1, protein2]))],
        "species": [taxon],
        "network_flavor": ["evidence"],
        "network_type": ["functional"],
        "required_score": ["0"],
        "add_color_nodes": ["0"],
        "add_white_nodes": ["0"],
    }
    assert "%0D" in url
    assert url == string_source_record_url(protein2, protein1, "v12.0")
    assert transform_string_ppi(context, record | {"protein1": protein2, "protein2": protein1}) is None

    other = transform_string_ppi(context, record | {"protein2": protein2 + "9"})
    assert other is not None
    assert other.edges[0].sources[0].source_record_urls != [url]
    graph.edges[0].sources[0].source_record_urls.append("https://example.org/changed")
    assert graph.edges[1].sources[0].source_record_urls == [url]
    assert other.edges[0].sources[0].source_record_urls != [url]


@pytest.mark.parametrize(
    "protein1,protein2",
    [
        ("ENSP00000000233", "9606.ENSP00000371175"),
        ("9606.ENSP00000000233", "9606."),
        ("9606.ENSP00000000233", "10090.ENSMUSP00000000001"),
        ("4932.YAL001C", "4932.YAL002W"),
    ],
)
def test_pair_url_rejects_invalid_or_mixed_taxa(protein1: str, protein2: str) -> None:
    """Never produce a species-disambiguated URL for an invalid pair."""
    with pytest.raises(ValueError):
        string_source_record_url(protein1, protein2, "v12.0")


@pytest.mark.parametrize(
    "version,host",
    [("v12.0", "version-12-0.string-db.org"), ("v13.0", "version-13-0.string-db.org"), ("v12.5", "version-12-5.string-db.org")],
)
def test_links_follow_the_ingested_release(tmp_path: Path, version: str, host: str) -> None:
    """The source record URL points at the STRING release whose files were ingested."""
    source_data = tmp_path / "string" / version / "source_data"
    source_data.mkdir(parents=True)
    assert string_version_from_source_dir(source_data) == version
    assert string_network_url(version) == f"https://{host}/cgi/network"

    writer = JSONLWriter(str(tmp_path), "string", WriterConfig())
    context = KozaTransform(extra_fields={}, writer=writer, mappings={})
    context.state["string_to_entrez"] = {}
    context.state["string_version"] = version
    record = {"protein1": "9606.ENSP00000000233", "protein2": "9606.ENSP00000371175", "combined_score": "900"}
    graph = transform_string_ppi(context, record)
    assert graph is not None
    assert {urlsplit(edge.sources[0].source_record_urls[0]).netloc for edge in graph.edges} == {host}


@pytest.mark.parametrize("bad", ["12.0", "v12", "latest", "version-12-0"])
def test_unversioned_inputs_fail(tmp_path: Path, bad: str) -> None:
    """A source directory or version that does not name a STRING release is an error, not a guess."""
    with pytest.raises(ValueError):
        string_network_url(bad)
    with pytest.raises(ValueError):
        string_version_from_source_dir(tmp_path / "string" / bad / "source_data")
