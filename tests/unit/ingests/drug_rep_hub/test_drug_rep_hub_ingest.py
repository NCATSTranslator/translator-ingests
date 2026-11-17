import pytest

from typing import Optional

from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    ChemicalEntity,
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    ChemicalOrDrugOrTreatmentToDiseaseOrPhenotypicFeatureAssociation,
    KnowledgeLevelEnum,
)
import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter

from translator_ingest.ingests.drug_rep_hub.drug_rep_hub import (
    build_indication_association,
    create_chemical_role_association,
    transform_drug_rep_hub_annotations,
)

from tests.unit.ingests import validate_transform_result, MockKozaWriter, MockKozaTransform


@pytest.fixture(scope="package")
def mock_koza_transform() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = dict()
    return MockKozaTransform(extra_fields=dict(), writer=writer, mappings=mappings)


# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = ("id", "name", "category")

# list of slots whose values are
# to be checked in a result edge
ASSOCIATION_TEST_SLOTS = ("category", "subject", "predicate", "object", "sources", "knowledge_level", "agent_type")


def test_chemical_role_association_keeps_drug_and_role_distinct():
    """Regression test for #281: the role node must not replace the drug node."""
    chemical = ChemicalEntity(id="PUBCHEM.COMPOUND:123", name="example drug")

    chemical_role, association = create_chemical_role_association(
        chemical=chemical,
        indication="anticoagulant",
        indication_info={"xref": "CHEBI:50249", "primary_name": "anticoagulant"},
        predicate="biolink:has_chemical_role",
    )

    assert association.subject == chemical.id
    assert association.object == chemical_role.id
    assert association.subject != association.object


@pytest.mark.parametrize(
    "clinical_phase, indication, association_class, predicate, clinical_approval_status, max_research_phase",
    [
        ("Launched", "breast cancer", ChemicalOrDrugOrTreatmentToDiseaseOrPhenotypicFeatureAssociation,
         "biolink:treats", "approved_for_condition", None),
        ("Withdrawn", "breast cancer", ChemicalOrDrugOrTreatmentToDiseaseOrPhenotypicFeatureAssociation,
         "biolink:treats_or_applied_or_studied_to_treat", "post_approval_withdrawal", None),
        ("Phase 2/Phase 3", "breast cancer", ChemicalOrDrugOrTreatmentToDiseaseOrPhenotypicFeatureAssociation,
         "biolink:in_clinical_trials_for", None, "clinical_trial_phase_2_to_3"),
        ("Preclinical", "breast cancer", ChemicalOrDrugOrTreatmentToDiseaseOrPhenotypicFeatureAssociation,
         "biolink:in_preclinical_trials_for", None, "pre_clinical_research_phase"),
        ("", "breast cancer", ChemicalOrDrugOrTreatmentToDiseaseOrPhenotypicFeatureAssociation,
         "biolink:treats_or_applied_or_studied_to_treat", None, None),
        ("Launched", "weight-gain aid", ChemicalOrDrugOrTreatmentToDiseaseOrPhenotypicFeatureAssociation,
         "biolink:ameliorates_condition", "approved_for_condition", None),
        # diagnoses is not a treatment predicate, so the edge can't carry clinical approval status
        ("Launched", "anacidity diagnostic", ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
         "biolink:diagnoses", None, None),
    ],
)
def test_indication_association_clinical_context(
    clinical_phase: str,
    indication: str,
    association_class: type,
    predicate: str,
    clinical_approval_status: Optional[str],
    max_research_phase: Optional[str],
):
    """The clinical phase sets the predicate of treatment edges, and their clinical approval status/research phase."""
    chemical = ChemicalEntity(id="PUBCHEM.COMPOUND:3385", name="5-fluorouracil")

    _, association = build_indication_association(chemical, clinical_phase, indication, disease_area="")

    assert type(association) is association_class
    assert association.predicate == predicate
    assert getattr(association, "clinical_approval_status", None) == clinical_approval_status
    assert getattr(association, "max_research_phase", None) == max_research_phase


@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [

        (  # Query 1 - Drug-Repurposing Hub record
            [
                {
                    "pert_iname": "5-fluorouracil",
                    "clinical_phase": "Launched",
                    "moa": "thymidylate synthase inhibitor",
                    "target": "",
                    "disease_area": "oncology",
                    "indication": "breast cancer",
                }
            ],
            # Captured node contents
            [
                {"id": "PUBCHEM.COMPOUND:3385", "category": ["biolink:ChemicalEntity"]},
                {"id": "MONDO:0007254", "name": "breast cancer", "category": ["biolink:DiseaseOrPhenotypicFeature"]},
            ],
            # Captured edge contents
            {
                "category": ["biolink:ChemicalOrDrugOrTreatmentToDiseaseOrPhenotypicFeatureAssociation"],
                "subject": "PUBCHEM.COMPOUND:3385",
                "predicate": "biolink:treats",
                "object": "MONDO:0007254",
                "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:drug-repurposing-hub"}],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
            },
        ),
        (  # Query 2 - Drug-Repurposing Hub record with single indication and
            [
                {
                    "pert_iname": "5-fluorouracil",
                    "clinical_phase": "Launched",
                    "moa": "thymidylate synthase inhibitor",
                    "target": "CD44",
                    "disease_area": "oncology",
                    "indication": "",
                }
            ],
            # Captured node contents
            [
                {"id": "PUBCHEM.COMPOUND:3385", "category": ["biolink:ChemicalEntity"]},
                {"id": "HGNC:1681", "name": "CD44", "category": ["biolink:Gene"], "symbol": "CD44"},
            ],
            # Captured edge contents

            {
                "category": ["biolink:ChemicalAffectsGeneAssociation"],
                "subject": "PUBCHEM.COMPOUND:3385",
                "predicate": "biolink:affects",
                "object": "HGNC:1681",
                "sources": [{"resource_role": "primary_knowledge_source", "resource_id": "infores:drug-repurposing-hub"}],
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.manual_agent,
            }
        ),
    ],
)
def test_ingest_transform(
    mock_koza_transform: koza.KozaTransform,
    test_record: dict,
    result_nodes: Optional[list],
    result_edge: Optional[dict],
):

    mock_koza_transform.state['samples'] = {
        "5-fluorouracil": {
            "PUBCHEM.COMPOUND:3385": ChemicalEntity(
                id="PUBCHEM.COMPOUND:3385",
                name="5-fluorouracil",
                category=["biolink:ChemicalEntity"],
            )
        }
    }
    for result in transform_drug_rep_hub_annotations(mock_koza_transform, test_record):
        validate_transform_result(
            result=result,
            expected_nodes=result_nodes,
            expected_edges=result_edge,
            node_test_slots=NODE_TEST_SLOTS,
            edge_test_slots=ASSOCIATION_TEST_SLOTS,
        )
