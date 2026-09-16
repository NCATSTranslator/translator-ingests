import pytest

from typing import Optional

from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    ChemicalAffectsGeneAssociation,
    ChemicalEntityToChemicalEntityAssociation,
    ChemicalGeneInteractionAssociation,
    GeneAffectsChemicalAssociation,
    KnowledgeLevelEnum,
    MacromolecularMachineHasSubstrateAssociation,
    ResourceRoleEnum,
    RetrievalSource,
)

import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests.chembl.chembl import build_chemical_entity_from_row, transform_complexes

from tests.unit.ingests import validate_transform_result, MockKozaWriter, MockKozaTransform


@pytest.fixture(scope="package")
def mock_koza_transform() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = dict()
    return MockKozaTransform(extra_fields=dict(), writer=writer, mappings=mappings)


@pytest.fixture
def fresh_koza() -> koza.KozaTransform:
    # Function-scoped so each test gets an empty state (and thus an empty chemical_cache);
    # build_chemical_entity_from_row memoizes by molregno, which would otherwise leak across tests.
    return MockKozaTransform(extra_fields=dict(), writer=MockKozaWriter(), mappings=dict())


# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = ("id", "name", "category")

# list of slots whose values are
# to be checked in a result edge
ASSOCIATION_TEST_SLOTS = ("category", "subject", "predicate", "object", "sources", "knowledge_level", "agent_type")


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (  # Query 1 - Another record complete with PubMedIDs
            [
                {
                    "target_type": "PROTEIN COMPLEX",
                    "target_name": "Anti-estrogen binding site (AEBS)",
                    "target_chembl_id": "CHEMBL612409",
                    "organism_tax_id": "9606",
                    "component_type": "PROTEIN",
                    "accession": "Q15125",
                    "description": "3-beta-hydroxysteroid-Delta(8),Delta(7)-isomerase",
                    "organism":"Homo sapiens",
                    "component_tax_id":"9606",
                    "db_source":"SWISS-PROT"
                }
            ],
            # Captured node contents
            [
                {"id": "UniProtKB:Q15125", "category": ["biolink:Protein"]},
                {"id": "CHEMBL.TARGET:CHEMBL612409", "name": "Anti-estrogen binding site (AEBS)", "category": ["biolink:MacromolecularComplex"]},
            ],
            # Captured edge contents
            {
                "category": ["biolink:AnatomicalEntityHasPartAnatomicalEntityAssociation"],
                "subject": "CHEMBL.TARGET:CHEMBL612409",
                "predicate": "biolink:has_part",
                "object": "UniProtKB:Q15125",
                "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:chembl"}],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
            },
        ),
    ],
)
def test_ingest_transform(
    mock_koza_transform: koza.KozaTransform,
    test_record: dict,
    result_nodes: Optional[list],
    result_edge: Optional[dict],
):

    mock_koza_transform.state['chembl_proteins'] = {
        "Q15125": {
            "id": "Q15125",
            "name": "3-beta-hydroxysteroid-Delta(8),Delta(7)-isomerase"
        }
    }
    
    for result in transform_complexes(mock_koza_transform, test_record):
        validate_transform_result(
            result=result,
            expected_nodes=result_nodes,
            expected_edges=result_edge,
            node_test_slots=NODE_TEST_SLOTS,
            edge_test_slots=ASSOCIATION_TEST_SLOTS,
        )


# ===== PYDANTIC ROUNDTRIP TESTS =====

CHEMBL_TEST_SOURCES = [
    RetrievalSource(
        id="infores:chembl",
        resource_id="infores:chembl",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

EDGE_FIXTURES = [
    {
        "association_class": ChemicalAffectsGeneAssociation,
        "params": {
            "id": "144fee80-1645-46f9-ad7f-ac9531159eb2",
            "subject": "PUBCHEM.COMPOUND:166630898",
            "predicate": "biolink:affects",
            "object": "NCBIGene:6789",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": CHEMBL_TEST_SOURCES,
        },
    },
    {
        "association_class": ChemicalAffectsGeneAssociation,
        "params": {
            "id": "e84d3baf-ebe1-41e6-a386-2957644161ec",
            "subject": "PUBCHEM.COMPOUND:44337123",
            "predicate": "biolink:affects",
            "object": "NCBIGene:4987",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CHEMBL_TEST_SOURCES,
        },
    },
    {
        "association_class": ChemicalEntityToChemicalEntityAssociation,
        "params": {
            "id": "29037cf2-dae5-4f2a-a374-c08ab9cc6142",
            "subject": "PUBCHEM.COMPOUND:63023",
            "predicate": "biolink:has_metabolite",
            "object": "UNII:7YHT3131XK",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CHEMBL_TEST_SOURCES,
        },
    },
    {
        "association_class": ChemicalGeneInteractionAssociation,
        "params": {
            "id": "6ab88901-4676-469d-a8b6-1b22af1ae5ad",
            "subject": "CHEBI:156442",
            "predicate": "biolink:directly_physically_interacts_with",
            "object": "NCBIGene:23012",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": CHEMBL_TEST_SOURCES,
        },
    },
    {
        "association_class": ChemicalGeneInteractionAssociation,
        "params": {
            "id": "3b4c34ec-0dea-4326-8356-7b4da1a31f21",
            "subject": "CHEBI:107736",
            "predicate": "biolink:directly_physically_interacts_with",
            "object": "NCBIGene:3360",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CHEMBL_TEST_SOURCES,
        },
    },
    {
        "association_class": ChemicalGeneInteractionAssociation,
        "params": {
            "id": "15b0d105-9138-4737-ab60-a3c91f240e80",
            "subject": "CHEMBL.COMPOUND:CHEMBL1743077",
            "predicate": "biolink:interacts_with",
            "object": "NCBIGene:3371",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CHEMBL_TEST_SOURCES,
        },
    },
    {
        "association_class": GeneAffectsChemicalAssociation,
        "params": {
            "id": "4a1c943e-d7dd-414a-b8a8-b42bd58cf7de",
            "subject": "NCBIGene:183",
            "predicate": "biolink:affects",
            "object": "CHEMBL.COMPOUND:CHEMBL4297887",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CHEMBL_TEST_SOURCES,
        },
    },
    {
        "association_class": MacromolecularMachineHasSubstrateAssociation,
        "params": {
            "id": "3ee985e8-a944-4804-9037-9bfcd2cfac85",
            "subject": "NCBIGene:5243",
            "predicate": "biolink:has_substrate",
            "object": "PUBCHEM.COMPOUND:25245532",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": CHEMBL_TEST_SOURCES,
        },
    },
    {
        "association_class": MacromolecularMachineHasSubstrateAssociation,
        "params": {
            "id": "75f6d81a-dd69-4409-8dc5-0c480ed61475",
            "subject": "NCBIGene:146802",
            "predicate": "biolink:has_substrate",
            "object": "CHEBI:44296",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CHEMBL_TEST_SOURCES,
        },
    },
]


@pytest.mark.parametrize(
    "fixture",
    EDGE_FIXTURES,
    ids=lambda f: f"{f['association_class'].__name__}_{f['params']['predicate'].split(':')[-1]}",
)
def test_pydantic_roundtrip(fixture):
    """Instantiate the association and round-trip through Pydantic serialization."""
    cls = fixture["association_class"]
    obj = cls(**fixture["params"])
    dumped = obj.model_dump()
    restored = cls.model_validate(dumped)
    assert restored == obj


# ===== build_chemical_entity_from_row (activities pre-join) =====

def _activity_row(**overrides) -> dict:
    """A pre-joined ChEMBL activity row carrying the molecule_* fields the builder reads."""
    row = {
        "molregno": 97,
        "molecule_chembl_id": "CHEMBL2",
        "molecule_pref_name": "PRAZOSIN",
        "molecule_inchi_key": "IENZQIKPOWMVPG-UHFFFAOYSA-N",
        "molecule_smiles": "COc1cc2nc(N3CCN(C(=O)c4ccco4)CC3)nc(N)c2cc1OC",
        "molecule_synonyms": ["CP-122991", "Prazosin"],
        "molecule_availability_type": 1,
        "molecule_black_box_warning": 0,
        "molecule_natural_product": 0,
        "molecule_prodrug": 0,
    }
    row.update(overrides)
    return row


@pytest.mark.parametrize(
    "row,expected",
    [
        (  # full record: structures -> xref (InChIKey then SMILES), availability mapped, flags off
            _activity_row(),
            {
                "id": "CHEMBL.COMPOUND:CHEMBL2",
                "name": "PRAZOSIN",
                "xref": ["InChIKey:IENZQIKPOWMVPG-UHFFFAOYSA-N",
                         "SMILES:COc1cc2nc(N3CCN(C(=O)c4ccco4)CC3)nc(N)c2cc1OC"],
                "synonym": ["CP-122991", "Prazosin"],
                "chembl_availability_type": "prescription only",
                "chembl_black_box_warning": None,
                "chembl_natural_product": None,
                "chembl_prodrug": None,
            },
        ),
        (  # no structures/synonyms -> xref/synonym None; flags on; withdrawn availability
            _activity_row(
                molecule_inchi_key=None, molecule_smiles=None, molecule_synonyms=None,
                molecule_availability_type=-2, molecule_black_box_warning=1,
                molecule_natural_product=1, molecule_prodrug=1,
            ),
            {
                "xref": None,
                "synonym": None,
                "chembl_availability_type": "withdrawn",
                "chembl_black_box_warning": "True",
                "chembl_natural_product": True,
                "chembl_prodrug": True,
            },
        ),
        (  # empty synonym list collapses to None
            _activity_row(molecule_synonyms=[]),
            {"synonym": None},
        ),
    ],
)
def test_build_chemical_entity_from_row(fresh_koza, row, expected):
    entity = build_chemical_entity_from_row(fresh_koza, row)
    for slot, value in expected.items():
        assert getattr(entity, slot) == value


def test_build_chemical_entity_from_row_no_molecule_returns_none(fresh_koza):
    # a molregno absent from molecule_dictionary comes through the LEFT JOIN with a NULL chembl_id
    assert build_chemical_entity_from_row(fresh_koza, _activity_row(molecule_chembl_id=None)) is None


def test_build_chemical_entity_from_row_is_memoized(fresh_koza):
    first = build_chemical_entity_from_row(fresh_koza, _activity_row())
    # same molregno, different molecule fields -> must return the cached object, not rebuild
    second = build_chemical_entity_from_row(fresh_koza, _activity_row(molecule_pref_name="CHANGED"))
    assert second is first
    assert second.name == "PRAZOSIN"
