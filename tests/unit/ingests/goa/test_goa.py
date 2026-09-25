from pathlib import Path

import pytest
import yaml

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    GeneToGoTermAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)
from translator_ingest.ingests.goa.goa import (
    ASPECT_TO_PREDICATE,
    QUALIFIER_TO_PREDICATE,
    get_supporting_data_sources,
)
from translator_ingest.util.biolink import get_biolink_model_toolkit


GOA_RIG_PATH = (
    Path(__file__).resolve().parents[4]
    / "src"
    / "translator_ingest"
    / "ingests"
    / "goa"
    / "goa_rig.yaml"
)


def _get_goa_rig_predicates() -> set[str]:
    """Return all edge predicates declared in the GOA RIG."""
    with GOA_RIG_PATH.open(encoding="utf-8") as stream:
        rig = yaml.safe_load(stream)
    edge_type_info = rig["target_info"]["edge_type_info"]
    return {predicate for edge_type in edge_type_info for predicate in edge_type["predicates"]}


def _get_valid_predicate_curies() -> set[str]:
    """Return every canonical predicate CURIE (slot_uri) in the pinned Biolink Model.

    Mirrors ``BiolinkValidationPlugin._get_valid_predicates``: predicates are the
    ``slot_uri`` values of the descendants of ``related to``. A predicate string is
    only valid on an emitted edge if it matches one of these URIs; alias names such
    as ``involved in`` resolve via ``get_element`` but never appear as a ``slot_uri``.
    """
    toolkit = get_biolink_model_toolkit()
    valid_uris: set[str] = set()
    for name in toolkit.get_descendants("related to", reflexive=True, mixin=True):
        element = toolkit.get_element(name)
        if element is not None and getattr(element, "slot_uri", None):
            valid_uris.add(element.slot_uri)
    return valid_uris


def test_goa_predicates_in_code_match_rig() -> None:
    """Ensure GOA transform predicates align with GOA RIG edge predicate definitions."""
    rig_predicates = _get_goa_rig_predicates()
    code_predicates = set(QUALIFIER_TO_PREDICATE.values())
    assert code_predicates == rig_predicates


@pytest.mark.parametrize(
    "predicate",
    sorted(
        set(QUALIFIER_TO_PREDICATE.values())
        | set(ASPECT_TO_PREDICATE.values())
        | _get_goa_rig_predicates()
    ),
)
def test_goa_predicates_exist_in_biolink_model(predicate: str) -> None:
    """Every GOA predicate must be a real Biolink slot_uri, not just an alias.

    This is the check that would have caught issue #463: ``biolink:involved_in``
    is only an alias of ``biolink:actively_involved_in`` and has no slot_uri, so
    it is rejected here while the canonical predicate passes.
    """
    assert predicate in _get_valid_predicate_curies()


@pytest.mark.parametrize(
    ("qualifier", "expected_predicate"),
    [
        ("involved_in", "biolink:actively_involved_in"),
        ("enables", "biolink:enables"),
        ("located_in", "biolink:located_in"),
        ("is_active_in", "biolink:active_in"),
        ("active_in", "biolink:active_in"),
    ],
)
def test_goa_qualifier_mapping(qualifier: str, expected_predicate: str) -> None:
    """Check key qualifier mappings that are explicitly documented in GOA RIG."""
    assert QUALIFIER_TO_PREDICATE[qualifier] == expected_predicate


def test_goa_aspect_fallback_for_biological_process() -> None:
    """Ensure biological process fallback predicate matches GOA RIG."""
    assert ASPECT_TO_PREDICATE["P"] == "biolink:actively_involved_in"


@pytest.mark.parametrize(
    ("assigned_by", "expected_supporting"),
    [
        ("MGI", ["infores:mgi"]),
        ("RGD", ["infores:rgd"]),
        ("Reactome", ["infores:reactome"]),
        ("IntAct", ["infores:intact"]),
        ("GO_Central", ["infores:go-cam"]),
        ("GOC", ["infores:go-cam"]),
        ("UniProt", None),
        ("", None),
        (None, None),
    ],
)
def test_goa_assigned_by_to_supporting_source_mapping(
    assigned_by: str | None, expected_supporting: list[str] | None
) -> None:
    """Ensure Assigned_By provenance is mapped to supporting sources when available."""
    assert get_supporting_data_sources(assigned_by) == expected_supporting


# -- Pydantic round-trip fixtures & test --

_GOA_SOURCES = [
    RetrievalSource(
        id="infores:goa",
        resource_id="infores:goa",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

EDGE_FIXTURES = [
    {
        "association_class": Association,
        "params": {
            "id": "29954ef5-b284-499b-b7ac-6ad0ece8442e",
            "subject": "NCBIGene:26258",
            "predicate": "biolink:active_in",
            "object": "GO:0005829",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "00105410-14ed-503e-9e47-dacbfe63bc00",
            "subject": "NCBIGene:80728",
            "predicate": "biolink:active_in",
            "object": "GO:0098978",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "e62dfb23-db71-4b17-8926-a3e4911b622e",
            "subject": "NCBIGene:2053",
            "predicate": "biolink:active_in",
            "object": "GO:0005829",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "ea1d0803-f063-4c80-8d99-e4be5367a7d9",
            "subject": "NCBIGene:4490",
            "predicate": "biolink:active_in",
            "object": "GO:0005634",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "e8a8da83-fc0f-4935-a90c-65eaada2e080",
            "subject": "UniProtKB:Q6NVV9",
            "predicate": "biolink:active_in",
            "object": "GO:0005575",
            "knowledge_level": KnowledgeLevelEnum.not_provided,
            "agent_type": AgentTypeEnum.not_provided,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "3aab3249-461d-4c36-8b04-f01d2d794f92",
            "subject": "NCBIGene:8462",
            "predicate": "biolink:acts_upstream_of",
            "object": "GO:0043065",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "4ef7ff32-d777-47b8-82a9-36a1fe0c53f1",
            "subject": "NCBIGene:57128",
            "predicate": "biolink:acts_upstream_of",
            "object": "GO:0044572",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "7d204d33-22a3-4e68-b9ef-1b9465c03204",
            "subject": "NCBIGene:7161",
            "predicate": "biolink:acts_upstream_of_negative_effect",
            "object": "GO:0051726",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "f2afb3f2-ac1a-4d63-b4cf-d15b186268fa",
            "subject": "NCBIGene:1672",
            "predicate": "biolink:acts_upstream_of_or_within",
            "object": "GO:0042742",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "0028a905-c0f7-5235-938c-444f0f7f535a",
            "subject": "NCBIGene:57419",
            "predicate": "biolink:acts_upstream_of_or_within",
            "object": "GO:0010629",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "44eef348-7bd7-4f1f-a214-e8326d44f273",
            "subject": "NCBIGene:348",
            "predicate": "biolink:acts_upstream_of_or_within_positive_effect",
            "object": "GO:0097113",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "d35bab26-7b35-47d4-bade-fe4a969bb556",
            "subject": "NCBIGene:29066",
            "predicate": "biolink:acts_upstream_of_positive_effect",
            "object": "GO:0010608",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "7dc57358-1bf7-4cba-9dec-a616df00dfa4",
            "subject": "NCBIGene:2852",
            "predicate": "biolink:acts_upstream_of_positive_effect",
            "object": "GO:0042311",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "4253958d-5dc8-4d08-83b1-a890896e1e37",
            "subject": "NCBIGene:10808",
            "predicate": "biolink:colocalizes_with",
            "object": "GO:0005874",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "cff691ba-c5ed-4225-a0f8-5e9b7438df90",
            "subject": "NCBIGene:4193",
            "predicate": "biolink:colocalizes_with",
            "object": "GO:0016604",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "016560ab-5f8b-5556-bb18-8da38bb86f93",
            "subject": "NCBIGene:79778",
            "predicate": "biolink:colocalizes_with",
            "object": "GO:0001725",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "0052766c-df63-5175-a391-c82b9f77c84d",
            "subject": "NCBIGene:2561",
            "predicate": "biolink:contributes_to",
            "object": "GO:0022851",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "1aa99452-3bae-4334-896a-a61eb077c301",
            "subject": "NCBIGene:1642",
            "predicate": "biolink:contributes_to",
            "object": "GO:0003684",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "6029fca8-38fd-4cc5-be64-a46452aef9a5",
            "subject": "NCBIGene:10060",
            "predicate": "biolink:contributes_to",
            "object": "GO:0015272",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "006ce26b-8a4a-5db7-b5fb-eeebfe0e250c",
            "subject": "NCBIGene:9464",
            "predicate": "biolink:contributes_to",
            "object": "GO:0000976",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "00000665-d26e-594a-83dd-47ae3d74204f",
            "subject": "NCBIGene:2298",
            "predicate": "biolink:enables",
            "object": "GO:0003700",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "bb274b44-0215-43b2-ae23-c28b5d219efd",
            "subject": "NCBIGene:301",
            "predicate": "biolink:enables",
            "object": "GO:0005543",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "963ceee2-5914-4a51-9729-cb2e821e0d4c",
            "subject": "NCBIGene:1538",
            "predicate": "biolink:enables",
            "object": "GO:0005198",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "e18e5c46-dc40-4843-b0c7-f2099b221741",
            "subject": "NCBIGene:708",
            "predicate": "biolink:enables",
            "object": "GO:0097177",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "16da060d-9e12-44c2-b156-7e752e61e288",
            "subject": "UniProtKB:Q13166",
            "predicate": "biolink:enables",
            "object": "GO:0003674",
            "knowledge_level": KnowledgeLevelEnum.not_provided,
            "agent_type": AgentTypeEnum.not_provided,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "1b86df80-3be7-4160-a5f7-ff5fee6ab875",
            "subject": "NCBIGene:7534",
            "predicate": "biolink:actively_involved_in",
            "object": "GO:0001525",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "3a899606-d1ed-488e-b1ae-14f1c8a9bbd9",
            "subject": "NCBIGene:29082",
            "predicate": "biolink:actively_involved_in",
            "object": "GO:0016236",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "4b0ddb2c-04b4-4c6e-899b-96b16e64efdb",
            "subject": "UniProtKB:A0M8Q6",
            "predicate": "biolink:actively_involved_in",
            "object": "GO:0002250",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "28182f19-9776-4538-a27c-36460c4a4d38",
            "subject": "NCBIGene:7471",
            "predicate": "biolink:actively_involved_in",
            "object": "GO:0045165",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "0553359b-4a11-5817-b2e2-5ef1801d0ce7",
            "subject": "NCBIGene:102553861",
            "predicate": "biolink:actively_involved_in",
            "object": "GO:0140507",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "bfa31a13-f3a2-4f41-b3ed-6712d9327d63",
            "subject": "NCBIGene:643641",
            "predicate": "biolink:actively_involved_in",
            "object": "GO:0008150",
            "knowledge_level": KnowledgeLevelEnum.not_provided,
            "agent_type": AgentTypeEnum.not_provided,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "00003ec9-da3a-563d-8424-a15f3e27cc78",
            "subject": "NCBIGene:7076",
            "predicate": "biolink:located_in",
            "object": "GO:0005604",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "000b7dbc-d64f-5bd0-89a4-727f2c2cc444",
            "subject": "NCBIGene:27343",
            "predicate": "biolink:located_in",
            "object": "GO:0005654",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "19213ccb-8e41-4f9a-bb67-83c218f59691",
            "subject": "NCBIGene:320",
            "predicate": "biolink:located_in",
            "object": "GO:0016020",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "de94f47c-a586-4138-b8b4-5b3a7ec60249",
            "subject": "NCBIGene:56907",
            "predicate": "biolink:located_in",
            "object": "GO:0005938",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "00029d20-c7d9-54ef-b60e-c417f1adb308",
            "subject": "NCBIGene:51013",
            "predicate": "biolink:part_of",
            "object": "GO:0000178",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "000877e0-510a-50d6-b4de-afb4bc4b7d50",
            "subject": "NCBIGene:284217",
            "predicate": "biolink:part_of",
            "object": "GO:0005608",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "0104d9e3-2218-541f-96a5-8e995643eaa6",
            "subject": "NCBIGene:4928",
            "predicate": "biolink:part_of",
            "object": "GO:0031080",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "001949a5-296c-500e-a5ab-a3d9849bfbb0",
            "subject": "NCBIGene:5289",
            "predicate": "biolink:part_of",
            "object": "GO:0035032",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "001dc1e7-10e7-5535-a8ba-2e5daa9fb13c",
            "subject": "NCBIGene:19166",
            "predicate": "biolink:active_in",
            "object": "GO:0005737",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "98ed74ab-f85f-4eb9-b212-4c9a238448ae",
            "subject": "NCBIGene:16833",
            "predicate": "biolink:active_in",
            "object": "GO:0005829",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "4381d3b6-838e-4a74-9a61-f09b67cdb0d2",
            "subject": "NCBIGene:66489",
            "predicate": "biolink:active_in",
            "object": "GO:0098794",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "0ad78dd5-f4f5-4faf-af1d-67e3ebb43ce3",
            "subject": "NCBIGene:366116",
            "predicate": "biolink:active_in",
            "object": "GO:0005886",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "0336dbba-c60b-450c-8d45-4ceb3761b6a0",
            "subject": "MGI:3649756",
            "predicate": "biolink:active_in",
            "object": "GO:0005575",
            "knowledge_level": KnowledgeLevelEnum.not_provided,
            "agent_type": AgentTypeEnum.not_provided,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "87fb9d29-fe30-4b14-a96e-697c05d4e680",
            "subject": "NCBIGene:77125",
            "predicate": "biolink:acts_upstream_of",
            "object": "GO:0032760",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "00053b3e-6232-54e8-b91f-3976cb54f408",
            "subject": "NCBIGene:100314186",
            "predicate": "biolink:acts_upstream_of",
            "object": "GO:0002862",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "a47aa9eb-5921-4142-acfd-a1c5f6fa791e",
            "subject": "NCBIGene:15902",
            "predicate": "biolink:acts_upstream_of_negative_effect",
            "object": "GO:0045664",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "1538b9a7-518a-4e8c-ba6f-277cefb28533",
            "subject": "NCBIGene:17952",
            "predicate": "biolink:acts_upstream_of_negative_effect",
            "object": "GO:0042981",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "7ffd3387-c2e0-4fa7-968e-e2ed777aea40",
            "subject": "NCBIGene:12622",
            "predicate": "biolink:acts_upstream_of_or_within",
            "object": "GO:0008285",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "027d464b-715c-51a7-98ed-8c7231196ab4",
            "subject": "NCBIGene:329628",
            "predicate": "biolink:acts_upstream_of_or_within",
            "object": "GO:0001736",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "c3fcc0ad-a09b-4f29-bf83-8780a5760050",
            "subject": "NCBIGene:363328",
            "predicate": "biolink:acts_upstream_of_or_within",
            "object": "GO:0032722",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "b7e1cf45-d8f1-46bd-9b8c-606e0437f22b",
            "subject": "NCBIGene:19702",
            "predicate": "biolink:acts_upstream_of_or_within_negative_effect",
            "object": "GO:0010467",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "3a93cc1c-e6d3-477d-b18b-9f5652ea70ef",
            "subject": "NCBIGene:20846",
            "predicate": "biolink:acts_upstream_of_or_within_positive_effect",
            "object": "GO:0043065",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "f174475b-ba91-4441-b5fe-30d7af0fd99f",
            "subject": "NCBIGene:312372",
            "predicate": "biolink:acts_upstream_of_or_within_positive_effect",
            "object": "GO:0032868",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "bec11990-a449-4b4e-8740-3797d5bc6b08",
            "subject": "NCBIGene:12499",
            "predicate": "biolink:acts_upstream_of_positive_effect",
            "object": "GO:0006011",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "a1b766ae-199f-40ad-926e-911560565ad9",
            "subject": "NCBIGene:100363270",
            "predicate": "biolink:acts_upstream_of_positive_effect",
            "object": "GO:0010526",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "96c1dde9-296b-4b06-ab81-2deac8765f69",
            "subject": "NCBIGene:114489",
            "predicate": "biolink:colocalizes_with",
            "object": "GO:0005925",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "41acd733-283d-44df-a318-e4a85ededde8",
            "subject": "NCBIGene:319262",
            "predicate": "biolink:colocalizes_with",
            "object": "GO:0032437",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "d8a520e9-c272-4ad8-b1aa-90e78e133d16",
            "subject": "NCBIGene:305552",
            "predicate": "biolink:colocalizes_with",
            "object": "GO:0005938",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "00749c69-9728-5b86-bc4d-3bc251fd290e",
            "subject": "NCBIGene:361858",
            "predicate": "biolink:contributes_to",
            "object": "GO:0003677",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "021bcf03-df6e-57c4-8524-c1ad33150555",
            "subject": "NCBIGene:192280",
            "predicate": "biolink:contributes_to",
            "object": "GO:0004721",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "065c3629-d660-4a1d-b973-7c3a25818e6e",
            "subject": "NCBIGene:361258",
            "predicate": "biolink:contributes_to",
            "object": "GO:0004842",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "625a2724-2735-4535-a341-b13bb2785538",
            "subject": "NCBIGene:83503",
            "predicate": "biolink:contributes_to",
            "object": "GO:0003899",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "0002ce32-67b9-53c5-9228-9250e04be0e9",
            "subject": "NCBIGene:232187",
            "predicate": "biolink:enables",
            "object": "GO:0140943",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "182c768f-2404-42eb-ab60-271571f398d1",
            "subject": "NCBIGene:26558",
            "predicate": "biolink:enables",
            "object": "GO:0019904",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "01b8c762-3b0a-5356-ab8b-692cb79a8031",
            "subject": "NCBIGene:15377",
            "predicate": "biolink:enables",
            "object": "GO:0000981",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "591fd240-ff68-41b7-865c-6aa2afd9ea09",
            "subject": "NCBIGene:317432",
            "predicate": "biolink:enables",
            "object": "GO:0042802",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "62a21ba3-2da1-43a3-a1fe-645e2133d34a",
            "subject": "NCBIGene:74482",
            "predicate": "biolink:enables",
            "object": "GO:0003674",
            "knowledge_level": KnowledgeLevelEnum.not_provided,
            "agent_type": AgentTypeEnum.not_provided,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "000013d2-281c-5bb0-a0d7-619f4dcf6d93",
            "subject": "NCBIGene:24585",
            "predicate": "biolink:actively_involved_in",
            "object": "GO:0006942",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "fe9de6b5-bb24-488b-a880-1eac92c9d80b",
            "subject": "NCBIGene:66983",
            "predicate": "biolink:actively_involved_in",
            "object": "GO:0001546",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "9ea07fc8-b38c-4e5a-9c21-3e2ec2679c0f",
            "subject": "NCBIGene:75613",
            "predicate": "biolink:actively_involved_in",
            "object": "GO:0060261",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "bea22260-7d99-4246-ae09-0a1ce021fec7",
            "subject": "NCBIGene:405219",
            "predicate": "biolink:actively_involved_in",
            "object": "GO:0050911",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "0966d200-2c53-40ac-9047-d3734b29f49a",
            "subject": "NCBIGene:102637107",
            "predicate": "biolink:actively_involved_in",
            "object": "GO:0008150",
            "knowledge_level": KnowledgeLevelEnum.not_provided,
            "agent_type": AgentTypeEnum.not_provided,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "127b4d8a-9ec2-4d36-9f5f-8f5167567157",
            "subject": "NCBIGene:296501",
            "predicate": "biolink:located_in",
            "object": "GO:0005634",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "1f736755-8c37-4722-9f2a-0c8a3a5b5fc2",
            "subject": "NCBIGene:22361",
            "predicate": "biolink:located_in",
            "object": "GO:0005576",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "16353330-667f-46fd-9243-c519f46fad43",
            "subject": "NCBIGene:11767",
            "predicate": "biolink:located_in",
            "object": "GO:0005765",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "000332d9-9d3f-54ea-a4ca-2f2f645fbbf7",
            "subject": "NCBIGene:20249",
            "predicate": "biolink:located_in",
            "object": "GO:0016020",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "dadf0f7d-273d-4e74-b4a6-5957d4b1ea23",
            "subject": "NCBIGene:115488451",
            "predicate": "biolink:part_of",
            "object": "GO:0005688",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "00241260-1d25-5451-8d24-17a9dd541ce0",
            "subject": "NCBIGene:26918",
            "predicate": "biolink:part_of",
            "object": "GO:1990604",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "62e90642-dfc5-4dff-b3a6-dcf6982115d3",
            "subject": "NCBIGene:16001",
            "predicate": "biolink:part_of",
            "object": "GO:0035867",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.manual_validation_of_automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
]


@pytest.mark.parametrize(
    "fixture",
    EDGE_FIXTURES,
    ids=lambda f: (
        f"{f['association_class'].__name__}"
        f"_{f['params']['predicate'].split(':')[-1]}"
        f"_{f['params']['agent_type'].value}"
        f"_{f['params']['knowledge_level'].value}"
    ),
)
def test_pydantic_roundtrip(fixture):
    """Instantiate the association and round-trip through Pydantic serialization."""
    cls = fixture["association_class"]
    obj = cls(**fixture["params"])
    dumped = obj.model_dump()
    restored = cls.model_validate(dumped)
    assert restored == obj
