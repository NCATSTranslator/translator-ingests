import json
from pathlib import Path

import pytest

from translator_ingest.ingests.gtopdb.gtopdb import TargetDescriptor, prepare, transform_ingest_all
from translator_ingest.ingests.gtopdb.rules import RULES, resolve_rule

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    ChemicalAffectsGeneAssociation,
    PairwiseMolecularInteraction,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    DirectionQualifierEnum,
    CausalMechanismQualifierEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)


GTOPDB_SOURCES = [
    RetrievalSource(
        id="infores:gtopdb",
        resource_id="infores:gtopdb",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]


def test_target_descriptor_preserves_source_identity_and_components():
    target = TargetDescriptor.from_record(
        {
            "target_id": "378",
            "target_name": "5-HT3AB",
            "target_species": "Human",
            "target_subunit_ids": "373|374",
            "target_gene_symbols": "HTR3A|HTR3B",
            "target_uniprot_ids": "P46098|O95264",
        }
    )

    assert target.source_id == "378"
    assert target.species == "Human"
    assert target.subunit_ids == ("373", "374")
    assert target.gene_symbols == ("HTR3A", "HTR3B")
    assert target.uniprot_ids == ("P46098", "O95264")
    assert target.is_composite
    assert target.single_protein_curie is None


@pytest.mark.parametrize(
    ("type_value", "action_value", "polarity"),
    [
        ("Activator", "future action", "positive"),
        ("Inhibitor", "future action", "negative"),
    ],
)
def test_legacy_type_fallbacks_remain_explicit(type_value, action_value, polarity):
    rule = resolve_rule(type_value, action_value)

    assert rule is not None
    assert rule.polarity == polarity
    assert rule.mechanism is None
    assert not rule.physical_interaction
    assert not rule.skip


def test_unknown_type_action_pair_has_no_rule():
    assert resolve_rule("unknown type", "unknown action") is None


def test_flat_rule_dictionary_is_the_registration_source():
    assert RULES[("Agonist", "Inverse agonist")] == resolve_rule(
        "Agonist", "Inverse agonist"
    )


def test_prepare_preserves_source_target_fields(tmp_path):
    (tmp_path / "ligands.csv").write_text(
        '"# GtoPdb Version: test"\n"Ligand ID","PubChem CID"\n"1","2244"\n'
    )
    context = RecordingContext()
    context.input_files_dir = tmp_path
    source_record = {
        "Target": "5-HT3AB",
        "Target ID": "378",
        "Target Subunit IDs": "373|374",
        "Target Gene Symbol": "HTR3A|HTR3B",
        "Target UniProt ID": "P46098|O95264",
        "Target Species": "Human",
        "Ligand ID": "1",
        "Ligand": "example ligand",
        "Type": "Agonist",
        "Action": "Agonist",
        "Endogenous": "FALSE",
        "Ligand Context": "",
        "PubMed ID": "11489465",
    }

    prepared = list(prepare(context, [source_record]))

    assert len(prepared) == 1
    assert prepared[0]["target_id"] == "378"
    assert prepared[0]["target_name"] == "5-HT3AB"
    assert prepared[0]["target_species"] == "Human"
    assert prepared[0]["target_subunit_ids"] == "373|374"
    assert prepared[0]["target_gene_symbols"] == "HTR3A|HTR3B"
    assert prepared[0]["target_uniprot_ids"] == "P46098|O95264"


class RecordingContext:
    def __init__(self):
        self.messages = []

    def log(self, message: str, level: str = "INFO") -> None:
        self.messages.append((level, message))


def _edge_signature(edge):
    return {
        "class": type(edge).__name__,
        "subject": edge.subject,
        "predicate": edge.predicate,
        "object": edge.object,
        "qualified_predicate": getattr(edge, "qualified_predicate", None),
        "object_aspect_qualifier": getattr(edge, "object_aspect_qualifier", None),
        "object_direction_qualifier": getattr(edge, "object_direction_qualifier", None),
        "causal_mechanism_qualifier": getattr(edge, "causal_mechanism_qualifier", None),
        "publications": getattr(edge, "publications", None),
    }


INTERACTION_RULE_GOLDEN = json.loads(
    (Path(__file__).parent / "interaction_rule_golden.json").read_text()
)


@pytest.mark.parametrize(
    "case",
    INTERACTION_RULE_GOLDEN,
    ids=lambda case: f"{case['type']}:{case['action']}:{case['endogenous']}",
)
def test_transform_matches_current_source_rule_behavior(case):
    """Freeze 2026.2 behavior before replacing the Type/Action conditional forest."""
    record = {
        "subject_id": "2244",
        "subject_name": "example ligand",
        "target_id": "1",
        "target_name": "example target",
        "target_species": "Human",
        "target_subunit_ids": "",
        "target_gene_symbols": "GENE",
        "target_uniprot_ids": "P08588",
        "Type": case["type"],
        "Action": case["action"],
        "Endogenous": case["endogenous"],
        "PubMed ID": "123|456",
    }

    graph = transform_ingest_all(RecordingContext(), [record])[0]

    assert [_edge_signature(edge) for edge in graph.edges] == case["edges"]


def test_transform_explicitly_excludes_unsupported_composite_targets():
    context = RecordingContext()
    record = {
        "subject_id": "2244",
        "subject_name": "example ligand",
        "object_id": "P46098|O95264",
        "object_name": "5-HT3AB",
        "target_id": "378",
        "target_name": "5-HT3AB",
        "target_species": "Human",
        "target_subunit_ids": "373|374",
        "target_gene_symbols": "HTR3A|HTR3B",
        "target_uniprot_ids": "P46098|O95264",
        "Type": "Agonist",
        "Action": "Agonist",
        "Endogenous": "FALSE",
        "PubMed ID": "11489465",
    }

    graph = transform_ingest_all(context, [record])[0]

    assert graph.nodes == []
    assert graph.edges == []
    assert context.messages == [
        (
            "WARNING",
            "Excluded 1 GtoPdb interaction record with an unsupported composite target; "
            "no compound UniProt CURIE was emitted.",
        )
    ]


# ── Fixtures: one per edge type declared in gtopdb_rig.yaml / gtopdb.py ────
EDGE_FIXTURES = [
    {
        "association_class": ChemicalAffectsGeneAssociation,
        "params": {
            "id": "uuid:gtopdb-chem-affects-gene",
            "subject": "PUBCHEM.COMPOUND:2244",
            "predicate": "biolink:affects",
            "object": "UniProtKB:P08588",
            "qualified_predicate": "biolink:causes",
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.agonism,
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": GTOPDB_SOURCES,
            "publications": ["PMID:12345678"],
        },
    },
    {
        "association_class": PairwiseMolecularInteraction,
        "params": {
            "id": "uuid:gtopdb-pairwise-interaction",
            "subject": "PUBCHEM.COMPOUND:2244",
            "predicate": "biolink:directly_physically_interacts_with",
            "object": "UniProtKB:P08588",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": GTOPDB_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "uuid:gtopdb-generic-related",
            "subject": "PUBCHEM.COMPOUND:5311",
            "predicate": "biolink:related_to",
            "object": "UniProtKB:Q14416",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": GTOPDB_SOURCES,
        },
    },
]


@pytest.mark.parametrize(
    "fixture",
    EDGE_FIXTURES,
    ids=lambda f: f["association_class"].__name__,
)
def test_pydantic_roundtrip(fixture):
    """Instantiate the association and round-trip through Pydantic serialization."""
    cls = fixture["association_class"]
    obj = cls(**fixture["params"])
    dumped = obj.model_dump()
    restored = cls.model_validate(dumped)
    assert restored == obj
