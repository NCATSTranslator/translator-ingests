import uuid
from typing import List

# from source_translation import source_map
from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    GeneToPhenotypicFeatureAssociation,
    GenotypeToPhenotypicFeatureAssociation,
    KnowledgeLevelEnum,
    VariantToPhenotypicFeatureAssociation,
)
from koza.cli_utils import get_koza_app
from loguru import logger

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

koza_app = get_koza_app("alliance_phenotype")
entity_lookup = koza_app.get_map("alliance-entity-lookup")

while (row := koza_app.get_row()) is not None:
    if len(row["phenotypeTermIdentifiers"]) == 0:
        logger.warning("Phenotype ingest record has 0 phenotype terms: " + str(row))
        koza_app.next_row()
    if len(row["phenotypeTermIdentifiers"]) > 1:
        logger.warning("Phenotype ingest record has >1 phenotype terms: " + str(row))
        koza_app.next_row()

    id = row["objectId"]
    try:
        category = entity_lookup[id]["category"]
    except KeyError:
        continue

    phenotypic_feature_id = row["phenotypeTermIdentifiers"][0]["termId"]
    # Remove the extra WB: prefix if necessary
    phenotypic_feature_id = phenotypic_feature_id.replace("WB:WBPhenotype:", "WBPhenotype:")
    prefix = id.split(":")[0]
    if category == 'biolink:Gene':
        EdgeClass = GeneToPhenotypicFeatureAssociation
    elif category == 'biolink:Genotype':
        EdgeClass = GenotypeToPhenotypicFeatureAssociation
    elif category == 'biolink:SequenceVariant':
        EdgeClass = VariantToPhenotypicFeatureAssociation
    else:
        raise ValueError(f"Unknown category {category} for {id}")

    association = EdgeClass(
        id="uuid:" + str(uuid.uuid1()),
        subject=id,
        predicate="biolink:has_phenotype",
        object=phenotypic_feature_id,
        publications=[row["evidence"]["publicationId"]],
        aggregator_knowledge_source=["infores:monarchinitiative", "infores:agrkb"],
        primary_knowledge_source=source_map[row["objectId"].split(':')[0]],
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )

    if "conditionRelations" in row.keys() and row["conditionRelations"] is not None:
        qualifiers: List[str] = []
        for conditionRelation in row["conditionRelations"]:
            for condition in conditionRelation["conditions"]:
                if condition["conditionClassId"]:
                    qualifier_term = condition["conditionClassId"]
                    qualifiers.append(qualifier_term)
        association.qualifiers = qualifiers

    koza_app.write(association)
