import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    GeneRegulatesGeneAssociation,
    PairwiseGeneToGeneInteraction,
    GeneAffectsChemicalAssociation,
    ChemicalAffectsGeneAssociation,
    ChemicalGeneInteractionAssociation,
    ChemicalEntityToChemicalEntityAssociation,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    DirectionQualifierEnum,
    CausalMechanismQualifierEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)


SIGNOR_SOURCES = [
    RetrievalSource(
        id="infores:signor",
        resource_id="infores:signor",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

# ── Fixtures: one per association type from signor.py ──────────────────────
EDGE_FIXTURES = [
    {
        "association_class": GeneRegulatesGeneAssociation,
        "params": {
            "id": "uuid:signor-gene-regulates-gene",
            "subject": "UniProtKB:P04637",
            "predicate": "biolink:regulates",
            "object": "UniProtKB:Q00987",
            "qualified_predicate": "biolink:causes",
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "object_direction_qualifier": DirectionQualifierEnum.upregulated,
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
            "species_context_qualifier": "NCBITaxon:9606",
            "publications": ["PMID:12345678"],
        },
    },
    {
        "association_class": PairwiseGeneToGeneInteraction,
        "params": {
            "id": "uuid:signor-pairwise-ppi",
            "subject": "UniProtKB:P04637",
            "predicate": "biolink:directly_physically_interacts_with",
            "object": "UniProtKB:Q00987",
            "qualified_predicate": "biolink:causes",
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "object_direction_qualifier": DirectionQualifierEnum.upregulated,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.phosphorylation,
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
            "species_context_qualifier": "NCBITaxon:9606",
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "uuid:signor-protein-part-of-complex",
            "subject": "UniProtKB:P04637",
            "predicate": "biolink:part_of",
            "object": "SIGNOR:SIGNOR-C1",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
        },
    },
    {
        "association_class": GeneAffectsChemicalAssociation,
        "params": {
            "id": "uuid:signor-gene-affects-chem",
            "subject": "UniProtKB:P04637",
            "predicate": "biolink:affects",
            "object": "CHEBI:15996",
            "qualified_predicate": "biolink:causes",
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.abundance,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.catalytic_activity,
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
            "species_context_qualifier": "NCBITaxon:9606",
        },
    },
    {
        "association_class": ChemicalAffectsGeneAssociation,
        "params": {
            "id": "uuid:signor-chem-affects-gene",
            "subject": "CHEBI:15996",
            "predicate": "biolink:affects",
            "object": "UniProtKB:P04637",
            "qualified_predicate": "biolink:causes",
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.inhibition,
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
            "species_context_qualifier": "NCBITaxon:9606",
            "anatomical_context_qualifier": ["BTO:0000007"],
        },
    },
    {
        "association_class": ChemicalGeneInteractionAssociation,
        "params": {
            "id": "uuid:signor-chem-gene-interaction",
            "subject": "CHEBI:15996",
            "predicate": "biolink:directly_physically_interacts_with",
            "object": "UniProtKB:P04637",
            "qualified_predicate": "biolink:causes",
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.binding,
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
            "species_context_qualifier": "NCBITaxon:9606",
        },
    },
    {
        "association_class": ChemicalEntityToChemicalEntityAssociation,
        "params": {
            "id": "uuid:signor-chem-chem",
            "subject": "CHEBI:15996",
            "predicate": "biolink:affects",
            "object": "CHEBI:17234",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
            "species_context_qualifier": "NCBITaxon:9606",
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
