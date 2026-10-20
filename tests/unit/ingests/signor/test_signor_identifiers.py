"""Functional coverage for SIGNOR chemical identifiers at both edge endpoints."""

from pathlib import Path

import koza
import pytest
from koza.io.writer.jsonl_writer import JSONLWriter
from koza.model.writer import WriterConfig

from translator_ingest.ingests.signor import signor


@pytest.mark.parametrize(
    ("subject_category", "object_category", "included"),
    [
        ("protein", "protein", True),
        ("protein", "chemical", True),
        ("chemical", "protein", True),
        ("smallmolecule", "protein", True),
        ("smallmolecule", "chemical", True),
        ("smallmolecule", "smallmolecule", True),
        # These routes are excluded by the existing ingest policy.
        ("protein", "smallmolecule", False),
        ("chemical", "chemical", False),
        ("chemical", "smallmolecule", False),
    ],
)
@pytest.mark.parametrize(
    ("source_id", "expected_id"),
    [
        ("CID:24795070", "PUBCHEM.COMPOUND:24795070"),
        ("PUBCHEM.COMPOUND:24795070", "PUBCHEM.COMPOUND:24795070"),
        ("CHEBI:95061", "CHEBI:95061"),
        ("SID:134445687", "SID:134445687"),
    ],
)
def test_chemical_identifiers_in_prepared_graph(
    tmp_path: Path,
    subject_category: str,
    object_category: str,
    included: bool,
    source_id: str,
    expected_id: str,
) -> None:
    """Canonicalize compounds while preserving substance IDs and every edge reference.

    CID:24795070 is SGI-1776 in the configured SIGNOR snapshot. The
    protein/chemical combinations also cover subjects and objects independently.
    """
    subject_is_protein = subject_category == "protein"
    object_is_protein = object_category == "protein"
    record = {
        "ENTITYA": "Subject",
        "ENTITYB": "Object",
        "TYPEA": subject_category,
        "TYPEB": object_category,
        "IDA": "P11309" if subject_is_protein else source_id,
        "IDB": "P49840" if object_is_protein else source_id,
        "EFFECT": "down-regulates activity",
        "MECHANISM": "binding",
        "DIRECT": "YES",
        "PMID": "12345678",
        "SENTENCE": "Supporting evidence.",
        "SCORE": "0.75",
        "TAX_ID": "9606",
        "CELL_DATA": None,
        "TISSUE_DATA": None,
    }
    writer = JSONLWriter(str(tmp_path), "signor", WriterConfig())
    context = koza.KozaTransform(extra_fields={}, writer=writer, mappings={})
    prepared = signor.prepare(context, [record])
    assert prepared[0]["CELL_DATA"] is None
    assert prepared[0]["TISSUE_DATA"] is None
    graphs = list(signor.transform_ingest_all(context, prepared))
    writer.finalize()

    expected_subject = "UniProtKB:P11309" if subject_is_protein else expected_id
    expected_object = "UniProtKB:P49840" if object_is_protein else expected_id
    assert len(graphs) == 1
    graph = graphs[0]
    if not included:
        assert graph.nodes == []
        assert graph.edges == []
        return
    assert [node.id for node in graph.nodes] == [expected_subject, expected_object]
    assert len(graph.edges) == 2
    for edge in graph.edges:
        assert (edge.subject, edge.object) == (expected_subject, expected_object)
        assert edge.publications == ["PMID:12345678"]
        assert edge.has_confidence_score == 0.75


@pytest.mark.parametrize(
    ("cell", "tissue"),
    [(None, None), ("CL:0000000", None), (None, "UBERON:0000001")],
)
def test_prepare_preserves_absent_and_present_context(tmp_path: Path, cell: str | None, tissue: str | None) -> None:
    """Missing context must survive pandas grouping without becoming a float."""
    record = dict.fromkeys(
        ["ENTITYA", "ENTITYB", "TYPEA", "TYPEB", "IDA", "IDB", "EFFECT", "MECHANISM", "TAX_ID", "DIRECT"],
        "value",
    )
    record.update(CELL_DATA=cell, TISSUE_DATA=tissue, SCORE="0.75", PMID="123", SENTENCE="Evidence.")
    writer = JSONLWriter(str(tmp_path), "signor-context", WriterConfig())
    context = koza.KozaTransform(extra_fields={}, writer=writer, mappings={})
    prepared = list(signor.prepare(context, [record]))
    writer.finalize()
    assert len(prepared) == 1
    assert prepared[0]["CELL_DATA"] == cell
    assert prepared[0]["TISSUE_DATA"] == tissue
