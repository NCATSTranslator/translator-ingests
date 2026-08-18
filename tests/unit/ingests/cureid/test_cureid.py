import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    ChemicalOrDrugOrTreatmentAdverseEventAssociation,
    DiseaseToPhenotypicFeatureAssociation,
    GeneToDiseaseAssociation,
    GenotypeToVariantAssociation,
    VariantToDiseaseAssociation,
    FDAIDAAdverseEventEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)


CUREID_SOURCES = [
    RetrievalSource(
        id="infores:cureid",
        resource_id="infores:cureid",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

# ── Fixtures: one per association type from cureid.py ───────────────────────
EDGE_FIXTURES = [
    {
        "association_class": ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
        "params": {
            "id": "uuid:cureid-chem-disease",
            "subject": "RXCUI:161",
            "predicate": "biolink:treats",
            "object": "MONDO:0100096",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CUREID_SOURCES,
            "publications": ["PMID:33332065"],
        },
    },
    {
        "association_class": ChemicalOrDrugOrTreatmentAdverseEventAssociation,
        "params": {
            "id": "uuid:cureid-adverse-event",
            "subject": "RXCUI:161",
            "predicate": "biolink:has_adverse_event",
            "object": "HP:0001945",
            "knowledge_level": KnowledgeLevelEnum.observation,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CUREID_SOURCES,
            "FDA_adverse_event_level": FDAIDAAdverseEventEnum.serious_adverse_event,
        },
    },
    {
        "association_class": DiseaseToPhenotypicFeatureAssociation,
        "params": {
            "id": "uuid:cureid-disease-pheno",
            "subject": "MONDO:0100096",
            "predicate": "biolink:has_phenotype",
            "object": "HP:0012735",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CUREID_SOURCES,
        },
    },
    {
        "association_class": GeneToDiseaseAssociation,
        "params": {
            "id": "uuid:cureid-gene-disease",
            "subject": "NCBIGene:7157",
            "predicate": "biolink:associated_with",
            "object": "MONDO:0100096",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CUREID_SOURCES,
        },
    },
    {
        "association_class": GenotypeToVariantAssociation,
        "params": {
            "id": "uuid:cureid-genotype-variant",
            "subject": "NCBIGene:7157",
            "predicate": "biolink:has_variant_part",
            "object": "HGVS:p.R175H",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CUREID_SOURCES,
        },
    },
    {
        "association_class": VariantToDiseaseAssociation,
        "params": {
            "id": "uuid:cureid-variant-disease",
            "subject": "HGVS:p.R175H",
            "predicate": "biolink:related_condition",
            "object": "MONDO:0100096",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CUREID_SOURCES,
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
