import uuid

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    GeneToDiseaseAssociation,
    GenotypeToDiseaseAssociation,
    VariantToDiseaseAssociation, KnowledgeLevelEnum, AgentTypeEnum,
)
from typing import Dict
from koza.cli_utils import get_koza_app

#  TODO: look at row["source"] to update this map
source_map = {
    "FB": "infores:flybase",
    "MGI": "infores:mgi",
    "RGD": "infores:rgd",
    "HGNC": "infores:rgd",  # Alliance contains RGD curation of human genes
    "SGD": "infores:sgd",
    "WB": "infores:wormbase",
    "Xenbase": "infores:xenbase",
    "ZFIN": "infores:zfin",
}

ZF_STANDARD_CONDITIONS = "Has Condition: standard conditions"

koza_app = get_koza_app("alliance_disease")

entities = []  # : Dict[str, Association] = {}
while (row := koza_app.get_row()) is not None:
    subject_category = row["DBobjectType"]
    if subject_category == 'gene':
        AssociationClass = GeneToDiseaseAssociation
        predicate = "biolink:related_to"  # TODO: sort out accurate predicates for each kind of row
    elif subject_category == 'allele':
        AssociationClass = VariantToDiseaseAssociation
        predicate = "biolink:related_to"  # TODO: sort out accurate predicates for each kind of row
    elif subject_category == 'affected_genomic_model':
        AssociationClass = GenotypeToDiseaseAssociation
        predicate = "biolink:model_of"
    else:
        # skip this row if there's an association with another kind of entity that we don't yet support, consider logging?
        continue

    ## from Alliance disease association UI:
    # "Is Implicated in" means that some variant of the gene is shown to function in causing or modifying a disease (for human) or a disease model state.
    # "Is a marker for" is used when there is evidence of an association but insufficient evidence to establish causality and does not necessarily imply that the existence of, or change in the biomarker is causal for the disease, but rather may result from it.

    ##  predicates map
    # biomarker_via_orthology
    # implicated_via_orthology
    # is_implicated_in
    # is_marker_for : biolink:biomarker_for
    # is_model_of : biolink:model_of
    # is_not_implicated_in

    if row["AssociationType"] == "is_model_of":
        predicate = "biolink:model_of"
#    elif row["AssociationType"] == "is_marker_for":
 #       predicate = "biolink:biomarker_for"
    else:
        # skip this row if there's an association type that we don't yet support
        continue

    # Exclude any rows with experimental conditions (aside from standard conditions) or modifiers
    if ((row.get("ExperimentalCondition") and row.get("ExperimentalCondition") != ZF_STANDARD_CONDITIONS)
            or row.get("Modifier")):
        continue

    association = AssociationClass(
        id=str(uuid.uuid1()),
        subject=row["DBObjectID"],
        predicate=predicate,
        object=row["DOID"],
        has_evidence=[row["EvidenceCode"]],
        # TODO: capture row["ExperimentalCondition"], probably as qualifier?
        publications=[row["Reference"]],
        primary_knowledge_source=source_map[row["DBObjectID"].split(':')[0]],
        aggregator_knowledge_source=["infores:monarchinitiative", "infores:agrkb"],
        # TODO: set KnowledgeLevelEnum and AgentType enum, it looks like there are inferred edges and that can show up in the KL/AT
        # TODO: the via_orthology association types would probably call for different KL/AT values?
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent
    )

    koza_app.write(association)
