"""Source record URLs use GtoPdb identifiers retained through real preparation."""

import json
from pathlib import Path

import pytest
from koza import KozaTransform
from koza.io.writer.jsonl_writer import JSONLWriter
from koza.model.writer import WriterConfig

from translator_ingest.ingests.gtopdb.gtopdb import (
    gtopdb_ligand_record_url,
    gtopdb_target_record_url,
    prepare,
    transform_ingest_all,
)


@pytest.mark.parametrize(
    "type_value,action,endogenous,predicates",
    [
        ("Agonist", "Agonist", "FALSE", {"biolink:affects", "biolink:directly_physically_interacts_with"}),
        ("Agonist", "Agonist", "TRUE", {"biolink:regulates", "biolink:directly_physically_interacts_with"}),
        ("None", "None", "FALSE", {"biolink:related_to"}),
        ("Fusion protein", "Binding", "FALSE", {"biolink:directly_physically_interacts_with"}),
    ],
)
@pytest.mark.parametrize("complex_target", [False, True])
def test_preparation_to_serialized_source_urls(
    tmp_path: Path,
    type_value: str,
    action: str,
    endogenous: str,
    predicates: set[str],
    complex_target: bool,
) -> None:
    """Ligands sharing a PubChem CID keep distinct links; membership links describe the target."""
    (tmp_path / "ligands.csv").write_text(
        '"# GtoPdb Version: test"\n"Ligand ID","PubChem CID"\n"10250","2244"\n"4073","2244"\n'
    )
    record = {
        "Target": "5-HT3AB" if complex_target else "example target",
        "Target ID": "378" if complex_target else "1",
        "Target Subunit IDs": "373|374" if complex_target else "",
        "Target Gene Symbol": "HTR3A|HTR3B" if complex_target else "GENE",
        "Target UniProt ID": "P46098|O95264" if complex_target else "P08588",
        "Target Species": "Human",
        "Ligand ID": "10250",
        "Ligand": "[125I]psalmotoxin 1",
        "Type": type_value,
        "Action": action,
        "Endogenous": endogenous,
        "Ligand Context": "",
        "PubMed ID": "123",
    }
    writer = JSONLWriter(str(tmp_path / "output"), "gtopdb", WriterConfig())
    context = KozaTransform(extra_fields={}, writer=writer, mappings={}, input_files_dir=tmp_path)
    prepared = list(prepare(context, [record, record | {"Ligand ID": "4073"}]))
    assert {row["Ligand ID"] for row in prepared} == {"10250", "4073"}
    assert {row["subject_id"] for row in prepared} == {"2244"}
    graph = list(transform_ingest_all(context, prepared))[0]
    interaction_edges = [edge for edge in graph.edges if edge.predicate != "biolink:has_part"]
    assert {edge.predicate for edge in interaction_edges} == predicates
    expected_ligand_urls = {
        "https://www.guidetopharmacology.org/GRAC/LigandDisplayForward?tab=biology&ligandId=10250",
        "https://www.guidetopharmacology.org/GRAC/LigandDisplayForward?tab=biology&ligandId=4073",
    }
    assert {edge.sources[0].source_record_urls[0] for edge in interaction_edges} == expected_ligand_urls
    components = [edge for edge in graph.edges if edge.predicate == "biolink:has_part"]
    assert len(components) == (4 if complex_target else 0)
    for edge in components:
        assert edge.sources[0].source_record_urls == [
            "https://www.guidetopharmacology.org/GRAC/ObjectDisplayForward?objectId=378"
        ]
    context.write(*graph.nodes, *graph.edges)
    writer.finalize()
    serialized = [json.loads(line) for line in (tmp_path / "output" / "gtopdb_edges.jsonl").read_text().splitlines()]
    assert len(serialized) == len(graph.edges)
    for edge in serialized:
        assert len(edge["sources"]) == 1
        source = edge["sources"][0]
        assert source["resource_id"] == "infores:gtopdb"
        assert source["resource_role"] == "primary_knowledge_source"
        assert len(source["source_record_urls"]) == 1
        expected = (
            {"https://www.guidetopharmacology.org/GRAC/ObjectDisplayForward?objectId=378"}
            if edge["predicate"] == "biolink:has_part"
            else expected_ligand_urls
        )
        assert source["source_record_urls"][0] in expected
    unaffected = [list(edge.sources[0].source_record_urls) for edge in graph.edges[1:]]
    graph.edges[0].sources[0].source_record_urls.append("https://example.org/changed")
    assert [edge.sources[0].source_record_urls for edge in graph.edges[1:]] == unaffected


@pytest.mark.parametrize("identifier", ["", " ", "PUBCHEM.COMPOUND:2244", "1&tab=other", "1.0", "nan"])
@pytest.mark.parametrize("builder", [gtopdb_ligand_record_url, gtopdb_target_record_url])
def test_source_id_validation(identifier: str, builder) -> None:
    """Reject missing, normalized, and malformed IDs instead of publishing misleading links."""
    with pytest.raises(ValueError, match="Expected a numeric GtoPdb"):
        builder(identifier)
