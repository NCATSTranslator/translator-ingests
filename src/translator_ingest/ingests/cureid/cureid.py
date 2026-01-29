import uuid
import koza
import requests
from typing import Any, Iterable
from enum import Enum
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

class CUREIDAdverseEventEnum(str, Enum):
    death = 'Death'
    life_threatening = 'Life-threatening'
    hospitalization_initial_or_prolonged = 'Hospitalization (initial or prolonged)'
    disability_or_permanent_damage = 'Disability or Permanent Damage'
    congenital_anomaly_birth_defects = 'Congenital Anomaly/Birth Defects'
    other_serious_or_important_medical_events = 'Other Serious or Important Medical Events'
    required_intervention_to_prevent_permanent_impairment_damage = 'Required Intervention to Prevent Permanent Impairment/Damage'
    non_serious_medical_event_requiring_intervention = 'Non-serious Medical Event Requiring Intervention'
    non_serious_medical_event_not_requiring_intervention = 'Non-serious Medical Event Not Requiring Intervention'
    treatment_was_discontinued_due_to_the_adverse_event = 'Treatment was Discontinued due to the Adverse Event'
    unknown = 'Unknown'

def parse_cureid_adverse_event(ae_string: str) -> CUREIDAdverseEventEnum:
    clean_ae_string = (
        ae_string.strip()
            .replace('-','_')
            .replace(' ', '_')
            .replace('(','')
            .replace(')','')
            .replace('/','_')
            .replace('<b>','')
            .replace('</b>','')
            .lower()
    )
    try:
        return CUREIDAdverseEventEnum[clean_ae_string]
    except ValueError:
        return CUREIDAdverseEventEnum.unknown

def get_adverse_event_level_from_outcomes(outcomes: list[str]) -> FDAIDAAdverseEventEnum:
    life_threatening_outcomes = [CUREIDAdverseEventEnum.death,
                                 CUREIDAdverseEventEnum.life_threatening]
    serious_outcomes = [
        CUREIDAdverseEventEnum.hospitalization_initial_or_prolonged,
        CUREIDAdverseEventEnum.disability_or_permanent_damage,
        CUREIDAdverseEventEnum.congenital_anomaly_birth_defects,
        CUREIDAdverseEventEnum.other_serious_or_important_medical_events,
        CUREIDAdverseEventEnum.treatment_was_discontinued_due_to_the_adverse_event,
        CUREIDAdverseEventEnum.required_intervention_to_prevent_permanent_impairment_damage
    ]
    suspected_outcomes = []
    unexpected_outcomes = [
        CUREIDAdverseEventEnum.non_serious_medical_event_requiring_intervention,
        CUREIDAdverseEventEnum.non_serious_medical_event_not_requiring_intervention,
        CUREIDAdverseEventEnum.unknown
    ]

    parsed_outcomes = [parse_cureid_adverse_event(outcome) for outcome in outcomes]

    if any(outcome in life_threatening_outcomes for outcome in parsed_outcomes):
        return FDAIDAAdverseEventEnum.life_threatening_adverse_event
    elif any(outcome in serious_outcomes for outcome in parsed_outcomes):
        return FDAIDAAdverseEventEnum.serious_adverse_event
    elif any(outcome in suspected_outcomes for outcome in parsed_outcomes):
        return FDAIDAAdverseEventEnum.suspected_adverse_reaction
    elif any(outcome in unexpected_outcomes for outcome in parsed_outcomes):
        return FDAIDAAdverseEventEnum.unexpected_adverse_event
    else:
        raise Exception(f'Unmapped Adverse Event: {outcomes}')


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


def _create_associations(record: dict[str, Any]):
    edge_type = record['association_category']
    subjects = record['subject_final_curie'].split("|")
    objects = record['object_final_curie'].split("|")
    associations = []

    for subject in subjects:
        for object in objects:
            params = {
                'id': str(uuid.uuid4()),
                'subject': subject,
                'predicate': record['biolink_predicate'],
                'object': object,
                'primary_knowledge_source': INFORES_CUREID,
                'knowledge_level': KnowledgeLevelEnum.knowledge_assertion,
                'agent_type': AgentTypeEnum.manual_agent
            }
            publications = []
            if record['pmid']:
                publications.append(f"PMID:{record['pmid']}")
            if record['link']:
                publications.append(record['link'])
            if len(publications) > 0:
                params['publications'] = publications

            if edge_type == 'biolink:ChemicalToDiseaseOrPhenotypicFeatureAssociation':
                if record['object_type'] == 'AdverseEvent':
                    params['FDA_adverse_event_level'] = get_adverse_event_level_from_outcomes(record['outcome'].split(';'))
                    associations.append(ChemicalOrDrugOrTreatmentSideEffectDiseaseOrPhenotypicFeatureAssociation(
                        **params
                    ))
                else:
                    associations.append(ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation(
                        **params
                    ))
            elif edge_type == 'biolink:DiseaseToPhenotypicFeatureAssociation':
                associations.append(DiseaseToPhenotypicFeatureAssociation(
                    **params
                ))
            elif edge_type == 'biolink:GeneToDiseaseAssociation':
                associations.append(GeneToDiseaseAssociation(
                    **params
                ))
            elif edge_type in ['biolink:GeneToVariantAssociation', # handle old misspelling
                               'biolink:GenotypeToVariantAssociation']:
                associations.append(GenotypeToVariantAssociation(
                    **params
                ))
            elif edge_type == 'biolink:VariantToDiseaseAssociation':
                associations.append(VariantToDiseaseAssociation(
                    **params
                ))
            else:
                raise ValueError(f"Unhandled edge type: {edge_type} in record: {record}")
    return associations

def _get_nodes(record: dict[str, Any], subject_or_object: str):
    if subject_or_object == "object":
        id = record['object_final_curie']
        label = record['object_final_label']
        type = record['object_type']
    else:
        id = record['subject_final_curie']
        label = record['subject_final_label']
        type = record['subject_type']
    ids = id.split("|")
    labels = label.split("|")
    return [_create_node(one_id, one_label, type, record) for one_id, one_label in zip(ids, labels)]


def get_subject_nodes(record: dict[str, Any]):
    return _get_nodes(record, "subject")


def get_object_nodes(record: dict[str, Any]):
    return _get_nodes(record, "object")


def get_edges(record: dict[str, Any]):
    return _create_associations(record)


@koza.transform(tag="ingest_all")
def transform_ingest_all(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    nodes: list[NamedThing] = []
    edges: list[Association] = []

    for record in data:
        subjects = get_subject_nodes(record)
        objects = get_object_nodes(record)
        edge_records = get_edges(record)

        nodes.extend(subjects)
        nodes.extend(objects)

        edges.extend(edge_records)

    return [KnowledgeGraph(nodes=nodes, edges=edges)]
