import uuid
import koza
from typing import Any, Iterable
import requests
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    Disease,
    NamedThing,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    Association, PhenotypicFeature, ChemicalOrDrugOrTreatmentSideEffectDiseaseOrPhenotypicFeatureAssociation,
    FDAIDAAdverseEventEnum, DiseaseToPhenotypicFeatureAssociation, Gene, GeneToDiseaseAssociation, SequenceVariant,
    GenotypeToVariantAssociation, VariantToDiseaseAssociation,
)
from koza.model.graphs import KnowledgeGraph
from translator_ingest.util.biolink import INFORES_CUREID

CUREID_RELEASE_METADATA_URL = 'https://opendata.ncats.nih.gov/public/cureid/cureid_version.json'

def get_adverse_event_level_from_outcome(outcome: str) -> FDAIDAAdverseEventEnum:
    if outcome.find("Non-serious Medical Event Not Requiring Intervention") > 0:
        return FDAIDAAdverseEventEnum.unexpected_adverse_event
    elif outcome.find("Treatment was Discontinued due to the Adverse Event") > 0:
        return FDAIDAAdverseEventEnum.serious_adverse_event
    elif outcome.find("Life-threatening") > 0:
        return FDAIDAAdverseEventEnum.life_threatening_adverse_event
    return FDAIDAAdverseEventEnum.unexpected_adverse_event

# a string representing the latest version of the source data.
def get_latest_version() -> str:
    """Fetch the current CURE ID release version from the metadata endpoint."""
    try:
        response = requests.get(CUREID_RELEASE_METADATA_URL, timeout=10)
        response.raise_for_status()
        metadata = response.json()
    except requests.RequestException as exc:
        raise RuntimeError(f"Unable to retrieve CURE ID release metadata from {CUREID_RELEASE_METADATA_URL}") from exc

    version = metadata.get("version")
    if not version:
        raise RuntimeError(f"CURE ID metadata from {CUREID_RELEASE_METADATA_URL} did not include a 'version' field")

    return version

def _create_node(id: str, label: str, obj_type: str, record: dict[str, Any]):
    params = {
        'id': id,
        'name': label
    }
    if obj_type == "Drug":
        return ChemicalEntity(
            **params
        )
    elif obj_type == "Disease":
        return Disease(
            **params
        )
    elif obj_type == "Gene":
        return Gene(
            **params
        )
    elif obj_type == "SequenceVariant":
        return SequenceVariant(
            **params
        )
    elif obj_type in ["PhenotypicFeature", "AdverseEvent"]:
        return PhenotypicFeature(
            **params
        )
    else:
        raise ValueError(f"Unhandled node type: {obj_type} in record: {record}")

def _create_association(record: dict[str, Any]):
    edge_type = record['association_category']
    params = {
        'id': str(uuid.uuid4()),
        'subject': record['final_subject_curie'],
        'predicate': record['biolink_predicate'],
        'object': record['final_object_curie'],
        'primary_knowledge_source': INFORES_CUREID,
        'knowledge_level': KnowledgeLevelEnum.knowledge_assertion,
        'agent_type': AgentTypeEnum.manual_agent
    }
    if edge_type == 'biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation':
        if record['object_type'] == 'AdverseEvent':
            params['FDA_adverse_event_level'] = get_adverse_event_level_from_outcome(record['outcome'])
            return ChemicalOrDrugOrTreatmentSideEffectDiseaseOrPhenotypicFeatureAssociation(
                **params
            )
        else:
            return ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation(
                **params
            )
    elif edge_type == 'biolink:DiseaseToPhenotypicFeatureAssociation':
        return DiseaseToPhenotypicFeatureAssociation(
            **params
        )
    elif edge_type == 'biolink:GeneToDiseaseAssociation':
        return GeneToDiseaseAssociation(
            **params
        )
    elif edge_type == 'biolink:GeneToVariantAssociation':
        return GenotypeToVariantAssociation(
            **params
        )
    elif edge_type == 'biolink:VariantToDiseaseAssociation':
        return VariantToDiseaseAssociation(
            **params
        )
    else:
        raise ValueError(f"Unhandled edge type: {edge_type} in record: {record}")

def _get_node(record: dict[str, Any], subject_or_object: str):
    if subject_or_object == "object":
        id = record['final_object_curie']
        label = record['final_object_label']
        type = record['object_type']
    else:
        id = record['final_subject_curie']
        label = record['final_subject_label']
        type = record['subject_type']
    return _create_node(id, label, type, record)


def get_subject_node(record: dict[str, Any]):
    return _get_node(record, "subject")

def get_object_node(record: dict[str, Any]):
    return _get_node(record, "object")

def get_edge(record: dict[str, Any]):
    return _create_association(record)

@koza.transform(tag="ingest_all")
def transform_ingest_all(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    nodes: list[NamedThing] = []
    edges: list[Association] = []

    for record in data:

        subject = get_subject_node(record)
        object = get_object_node(record)
        edge = get_edge(record)

        nodes.append(subject)
        nodes.append(object)
        edges.append(edge)

    return [KnowledgeGraph(nodes=nodes, edges=edges)]

