import json
import re
from enum import Enum
from typing import Any, Iterable, Mapping

import koza
import requests

from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    Association,
    ChemicalEntity,
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    ChemicalOrDrugOrTreatmentAdverseEventAssociation,
    Disease,
    DiseaseToPhenotypicFeatureAssociation,
    FDAIDAAdverseEventEnum,
    Gene,
    GeneToDiseaseAssociation,
    GenotypeToVariantAssociation,
    KnowledgeLevelEnum,
    NamedThing,
    PhenotypicFeature,
    ResourceRoleEnum,
    RetrievalSource,
    SequenceVariant,
    VariantToDiseaseAssociation,
)
from koza.model.graphs import KnowledgeGraph
from translator_ingest.util.biolink import INFORES_CUREID
from translator_ingest.util.transform_utils import entity_id

CUREID_RELEASE_METADATA_URL = 'https://opendata.ncats.nih.gov/public/cureid/rasopathies_translator_version_info.jsonl'
CUREID_TREATMENT_OUTCOME_ATTRIBUTE_PREFIX = "CUREID:treatment_outcome_"
NOT_REPORTED = "Not reported"

CUREID_OBSERVATION_EDGE_PARAMS: dict[str, Any] = {
    "primary_knowledge_source": INFORES_CUREID,
    "knowledge_level": KnowledgeLevelEnum.observation,
    "agent_type": AgentTypeEnum.manual_agent,
}

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


def extract_cureid_source_version(version_records: Iterable[Mapping[str, Any]]) -> str:
    """Return the CURE-ID source version from RASopathies Translator version records.
    """
    for version_record in version_records:
        for source_version in version_record.get("source_versions", []):
            if source_version.get("name") == "CURE-ID" and source_version.get("version"):
                return source_version["version"]
    raise RuntimeError(f"CURE ID metadata from {CUREID_RELEASE_METADATA_URL} did not include a CURE-ID source version")


def parse_cureid_version_records(metadata_text: str) -> Iterable[Mapping[str, Any]]:
    """Parse CURE ID JSONL version metadata records.
    """
    for line in metadata_text.splitlines():
        if line.strip():
            yield json.loads(line)


def get_latest_version() -> str:
    """Fetch the current CURE ID RASopathies Translator source version."""
    try:
        response = requests.get(CUREID_RELEASE_METADATA_URL, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Unable to retrieve CURE ID release metadata from {CUREID_RELEASE_METADATA_URL}") from exc

    return extract_cureid_source_version(parse_cureid_version_records(response.text))


def _unique_present_values(values: Iterable[str | None]) -> list[str]:
    """Return non-empty values de-duplicated in first-seen order.

    >>> _unique_present_values(["a", None, "", "b", "a"])
    ['a', 'b']
    """
    return list(dict.fromkeys(value for value in values if value))


def _get_cureid_case_report_urls(record: dict[str, Any]) -> list[str]:
    """Return unique CURE ID case report URLs from an aggregate JSONL record."""
    return _unique_present_values(
        evidence.get("case_report", {}).get("url")
        for evidence in record.get("evidence", [])
    )


def _primary_retrieval_source(source_record_urls: list[str] | None = None) -> RetrievalSource:
    """Return the primary CURE ID retrieval source used on emitted edges."""
    return RetrievalSource(
        id=INFORES_CUREID,
        resource_id=INFORES_CUREID,
        resource_role=ResourceRoleEnum.primary_knowledge_source,
        source_record_urls=source_record_urls,
    )


def _cureid_jsonl_edge_params(record: dict[str, Any]) -> dict[str, Any]:
    """Return common edge parameters for aggregate CURE ID JSONL records."""
    return {
        **CUREID_OBSERVATION_EDGE_PARAMS,
        "evidence_count": record["case_report_count"],
        "sources": [_primary_retrieval_source(_get_cureid_case_report_urls(record))],
    }


def _knowledge_graph(nodes: list[NamedThing], edge: Association) -> KnowledgeGraph:
    """Return a single-edge knowledge graph."""
    return KnowledgeGraph(nodes=nodes, edges=[edge])


def _get_cureid_supporting_text(record: dict[str, Any]) -> list[str]:
    """Return unique source finding values from an aggregate JSONL record."""
    return _unique_present_values(
        evidence.get("finding", {}).get("source_value")
        for evidence in record.get("evidence", [])
    )


def _get_cureid_treatment_outcomes(record: dict[str, Any]) -> list[str]:
    """Return unique treatment outcome values from an aggregate JSONL record."""
    return _unique_present_values(
        evidence.get("treatment_response", {}).get("outcome")
        for evidence in record.get("evidence", [])
    )


def _get_cureid_treatment_outcome_attribute_ids(record: dict[str, Any]) -> list[str]:
    """Return local treatment outcome attribute IDs for interim Biolink-compatible output.

    >>> _get_cureid_treatment_outcome_attribute_ids({"evidence": [
    ...     {"treatment_response": {"outcome": "Patient improved"}},
    ... ]})
    ['CUREID:treatment_outcome_patient_improved']
    """
    return [
        f"{CUREID_TREATMENT_OUTCOME_ATTRIBUTE_PREFIX}{re.sub(r'[^a-z0-9]+', '_', outcome.lower()).strip('_')}"
        for outcome in _get_cureid_treatment_outcomes(record)
    ]


def _get_cureid_treatment_supporting_text_with_outcomes(record: dict[str, Any]) -> list[str]:
    """Return source finding values and labeled treatment outcomes for supporting text."""
    return _unique_present_values(
        [
            *_get_cureid_supporting_text(record),
            *(f"treatment_outcome: {outcome}" for outcome in _get_cureid_treatment_outcomes(record)),
        ]
    )


def _get_cureid_adverse_event_outcomes(record: dict[str, Any]) -> list[str]:
    """Return unique adverse-event outcome values from an aggregate JSONL record."""
    top_level_outcomes = record.get("outcomes") or []
    evidence_outcomes = [
        outcome
        for evidence in record.get("evidence", [])
        for outcome in ((evidence.get("adverse_event", {}).get("outcomes") or []))
    ]
    return _unique_present_values([*top_level_outcomes, *evidence_outcomes])


def _get_cureid_adverse_event_supporting_text(record: dict[str, Any]) -> list[str]:
    """Return unique source adverse-event labels from an aggregate JSONL record."""
    return _unique_present_values(
        evidence.get("adverse_event", {}).get("source_label")
        for evidence in record.get("evidence", [])
    )


def _get_cureid_diagnosis_supporting_text(record: dict[str, Any]) -> list[str]:
    """Return unique diagnosis methods from an aggregate JSONL record."""
    return _unique_present_values(
        method
        for evidence in record.get("evidence", [])
        for method in (evidence.get("diagnosis", {}).get("diagnosis_methods") or [])
    )


def _get_cureid_variant_name(gene_variant: Mapping[str, Any]) -> str:
    """Return the most specific useful name available for a local CURE ID variant.

    >>> _get_cureid_variant_name({"variant_label": "Not reported", "source_gene_symbol": "BRAF"})
    'BRAF variant'
    """
    variant_label = gene_variant.get("variant_label")
    if variant_label and variant_label != NOT_REPORTED:
        return variant_label
    if gene_variant.get("source_gene_symbol"):
        return f"{gene_variant['source_gene_symbol']} variant"
    return "CURE ID variant"


def _get_cureid_variant_supporting_text(record: dict[str, Any]) -> list[str]:
    """Return local variant facts and diagnosis methods as de-duplicated support text."""
    gene_variant = record["gene_variant"]
    variant_facts = [
        f"{label}: {gene_variant[field]}"
        for field, label in [
            ("source_gene_symbol", "source_gene_symbol"),
            ("nucleotide_change", "nucleotide_change"),
            ("protein_change", "protein_change"),
            ("variant_label", "variant_label"),
        ]
        if gene_variant.get(field) and gene_variant[field] != NOT_REPORTED
    ]
    return _unique_present_values([*variant_facts, *_get_cureid_diagnosis_supporting_text(record)])


def get_condition_has_phenotype_nodes(record: dict[str, Any]) -> list[NamedThing]:
    """Return condition and phenotype nodes from a CURE ID condition-has-phenotype record."""
    condition = record["condition"]
    phenotype = record["phenotype"]
    return [
        Disease(id=condition["id"], name=condition.get("name")),
        PhenotypicFeature(id=phenotype["id"], name=phenotype.get("name")),
    ]


def get_condition_has_phenotype_edge(record: dict[str, Any]) -> DiseaseToPhenotypicFeatureAssociation:
    """Return the aggregate condition-has-phenotype association for a CURE ID JSONL record."""
    condition = record["condition"]
    phenotype = record["phenotype"]
    return DiseaseToPhenotypicFeatureAssociation(
        id=entity_id(),
        subject=condition["id"],
        predicate="biolink:has_phenotype",
        object=phenotype["id"],
        **_cureid_jsonl_edge_params(record),
        supporting_text=_get_cureid_supporting_text(record),
    )


def get_condition_has_phenotype_graph(record: dict[str, Any]) -> KnowledgeGraph:
    """Return a graph for one CURE ID condition-has-phenotype aggregate record."""
    return _knowledge_graph(get_condition_has_phenotype_nodes(record), get_condition_has_phenotype_edge(record))


def get_drug_applied_to_treat_phenotype_nodes(record: dict[str, Any]) -> list[NamedThing]:
    """Return drug and phenotype nodes from a CURE ID drug-treats-phenotype record."""
    drug = record["drug"]
    phenotype = record["phenotype"]
    return [
        ChemicalEntity(id=drug["id"], name=drug.get("name")),
        PhenotypicFeature(id=phenotype["id"], name=phenotype.get("name")),
    ]


def get_drug_applied_to_treat_phenotype_edge(
    record: dict[str, Any],
) -> ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation:
    """Return the aggregate drug-applied-to-treat-phenotype association for a CURE ID JSONL record."""
    drug = record["drug"]
    phenotype = record["phenotype"]
    return ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation(
        id=entity_id(),
        subject=drug["id"],
        predicate="biolink:applied_to_treat",
        object=phenotype["id"],
        **_cureid_jsonl_edge_params(record),
        # TODO: Use this dedicated slot after Biolink adds treatment_outcome to this association class.
        # treatment_outcome=_get_cureid_treatment_outcomes(record),
        has_attribute=_get_cureid_treatment_outcome_attribute_ids(record),
        supporting_text=_get_cureid_treatment_supporting_text_with_outcomes(record),
    )


def get_drug_applied_to_treat_phenotype_graph(record: dict[str, Any]) -> KnowledgeGraph:
    """Return a graph for one CURE ID drug-applied-to-treat-phenotype aggregate record."""
    return _knowledge_graph(
        get_drug_applied_to_treat_phenotype_nodes(record),
        get_drug_applied_to_treat_phenotype_edge(record),
    )


def get_drug_applied_to_treat_condition_nodes(record: dict[str, Any]) -> list[NamedThing]:
    """Return drug and condition nodes from a CURE ID drug-treats-condition record."""
    drug = record["drug"]
    condition = record["condition"]
    return [
        ChemicalEntity(id=drug["id"], name=drug.get("name")),
        Disease(id=condition["id"], name=condition.get("name")),
    ]


def get_drug_applied_to_treat_condition_edge(
    record: dict[str, Any],
) -> ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation:
    """Return the aggregate drug-applied-to-treat-condition association for a CURE ID JSONL record."""
    drug = record["drug"]
    condition = record["condition"]
    return ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation(
        id=entity_id(),
        subject=drug["id"],
        predicate="biolink:applied_to_treat",
        object=condition["id"],
        **_cureid_jsonl_edge_params(record),
    )


def get_drug_applied_to_treat_condition_graph(record: dict[str, Any]) -> KnowledgeGraph:
    """Return a graph for one CURE ID drug-applied-to-treat-condition aggregate record."""
    return _knowledge_graph(
        get_drug_applied_to_treat_condition_nodes(record),
        get_drug_applied_to_treat_condition_edge(record),
    )


def get_drug_has_adverse_event_nodes(record: dict[str, Any]) -> list[NamedThing]:
    """Return drug and adverse-event phenotype nodes from a CURE ID adverse-event record."""
    drug = record["drug"]
    phenotype = record["phenotype"]
    return [
        ChemicalEntity(id=drug["id"], name=drug.get("name")),
        PhenotypicFeature(id=phenotype["id"], name=phenotype.get("name")),
    ]


def get_drug_has_adverse_event_edge(record: dict[str, Any]) -> ChemicalOrDrugOrTreatmentAdverseEventAssociation:
    """Return the aggregate drug-has-adverse-event association for a CURE ID JSONL record."""
    drug = record["drug"]
    phenotype = record["phenotype"]
    outcomes = _get_cureid_adverse_event_outcomes(record)
    params: dict[str, Any] = {
        "id": entity_id(),
        "subject": drug["id"],
        "predicate": "biolink:has_adverse_event",
        "object": phenotype["id"],
        **_cureid_jsonl_edge_params(record),
        "supporting_text": _get_cureid_adverse_event_supporting_text(record),
    }
    if outcomes:
        params["FDA_adverse_event_level"] = get_adverse_event_level_from_outcomes(outcomes)
    return ChemicalOrDrugOrTreatmentAdverseEventAssociation(**params)


def get_drug_has_adverse_event_graph(record: dict[str, Any]) -> KnowledgeGraph:
    """Return a graph for one CURE ID drug-has-adverse-event aggregate record."""
    return _knowledge_graph(get_drug_has_adverse_event_nodes(record), get_drug_has_adverse_event_edge(record))


def get_gene_associated_with_condition_nodes(record: dict[str, Any]) -> list[NamedThing]:
    """Return gene and condition nodes from a CURE ID gene-associated-with-condition record."""
    gene = record["gene"]
    condition = record["condition"]
    return [
        Gene(id=gene["id"], name=gene.get("symbol")),
        Disease(id=condition["id"], name=condition.get("name")),
    ]


def get_gene_associated_with_condition_edge(record: dict[str, Any]) -> GeneToDiseaseAssociation:
    """Return the aggregate gene-associated-with-condition association for a CURE ID JSONL record."""
    gene = record["gene"]
    condition = record["condition"]
    return GeneToDiseaseAssociation(
        id=entity_id(),
        subject=gene["id"],
        predicate="biolink:associated_with",
        object=condition["id"],
        **_cureid_jsonl_edge_params(record),
        supporting_text=_get_cureid_diagnosis_supporting_text(record),
    )


def get_gene_associated_with_condition_graph(record: dict[str, Any]) -> KnowledgeGraph:
    """Return a graph for one CURE ID gene-associated-with-condition aggregate record."""
    return _knowledge_graph(
        get_gene_associated_with_condition_nodes(record),
        get_gene_associated_with_condition_edge(record),
    )


def get_gene_has_sequence_variant_nodes(record: dict[str, Any]) -> list[NamedThing]:
    """Return gene and local sequence-variant nodes from a CURE ID gene-variant record."""
    gene = record["gene"]
    gene_variant = record["gene_variant"]
    return [
        Gene(id=gene["id"], name=gene.get("symbol")),
        SequenceVariant(id=gene_variant["id"], name=_get_cureid_variant_name(gene_variant)),
    ]


def get_gene_has_sequence_variant_edge(record: dict[str, Any]) -> GenotypeToVariantAssociation:
    """Return the aggregate gene-has-sequence-variant association for a local CURE ID variant."""
    gene = record["gene"]
    gene_variant = record["gene_variant"]
    return GenotypeToVariantAssociation(
        id=entity_id(),
        subject=gene["id"],
        predicate="biolink:has_sequence_variant",
        object=gene_variant["id"],
        **_cureid_jsonl_edge_params(record),
        supporting_text=_get_cureid_variant_supporting_text(record),
    )


def get_gene_has_sequence_variant_graph(record: dict[str, Any]) -> KnowledgeGraph:
    """Return a graph for one CURE ID gene-has-sequence-variant aggregate record."""
    return _knowledge_graph(get_gene_has_sequence_variant_nodes(record), get_gene_has_sequence_variant_edge(record))


def get_sequence_variant_genetically_associated_with_condition_nodes(record: dict[str, Any]) -> list[NamedThing]:
    """Return local sequence-variant and condition nodes from a CURE ID variant-condition record."""
    gene_variant = record["gene_variant"]
    condition = record["condition"]
    return [
        SequenceVariant(id=gene_variant["id"], name=_get_cureid_variant_name(gene_variant)),
        Disease(id=condition["id"], name=condition.get("name")),
    ]


def get_sequence_variant_genetically_associated_with_condition_edge(
    record: dict[str, Any],
) -> VariantToDiseaseAssociation:
    """Return the aggregate local variant-condition association for a CURE ID JSONL record."""
    gene_variant = record["gene_variant"]
    condition = record["condition"]
    return VariantToDiseaseAssociation(
        id=entity_id(),
        subject=gene_variant["id"],
        predicate="biolink:related_condition",
        object=condition["id"],
        **_cureid_jsonl_edge_params(record),
        supporting_text=_get_cureid_variant_supporting_text(record),
    )


def get_sequence_variant_genetically_associated_with_condition_graph(record: dict[str, Any]) -> KnowledgeGraph:
    """Return a graph for one CURE ID local variant-condition aggregate record."""
    return _knowledge_graph(
        get_sequence_variant_genetically_associated_with_condition_nodes(record),
        get_sequence_variant_genetically_associated_with_condition_edge(record),
    )


def get_jsonl_graph(record: dict[str, Any]) -> KnowledgeGraph:
    """Return the mapped graph for a CURE ID JSONL record."""
    if "condition" in record and "phenotype" in record:
        return get_condition_has_phenotype_graph(record)
    if "drug" in record and record.get("predicate", {}).get("id") == "has_adverse_event":
        return get_drug_has_adverse_event_graph(record)
    if "drug" in record and "phenotype" in record:
        return get_drug_applied_to_treat_phenotype_graph(record)
    if "drug" in record and "condition" in record:
        return get_drug_applied_to_treat_condition_graph(record)
    if "gene" in record and "condition" in record:
        return get_gene_associated_with_condition_graph(record)
    if "gene" in record and "gene_variant" in record:
        return get_gene_has_sequence_variant_graph(record)
    if "gene_variant" in record and "condition" in record:
        return get_sequence_variant_genetically_associated_with_condition_graph(record)
    raise ValueError(f"Unhandled CURE ID JSONL record shape: {record}")


@koza.transform(tag="ingest_all")
def transform_ingest_all(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    nodes: list[NamedThing] = []
    edges: list[Association] = []

    for record in data:
        graph = get_jsonl_graph(record)
        nodes.extend(graph.nodes)
        edges.extend(graph.edges)

    return [KnowledgeGraph(nodes=nodes, edges=edges)]
