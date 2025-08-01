import uuid
from typing import Iterable, Any

import koza

from biolink_model.datamodel.pydanticmodel_v2 import (
    Entity,
    Gene,
    BiologicalProcessOrActivity,
    MolecularActivity,
    Association,
    NamedThing,
    KnowledgeLevelEnum,
    AgentTypeEnum
)


def extract_curie_prefix(curie: str) -> str:
    """Extract the prefix from a CURIE (e.g., 'GO' from 'GO:0003674')."""
    return curie.split(':')[0] if ':' in curie else ''


def determine_node_category(entity_id: str, entity_type: str = None) -> list:
    """Determine the biolink category for an entity based on its ID prefix."""
    prefix = extract_curie_prefix(entity_id)

    if prefix in ['ZFIN', 'MGI', 'RGD', 'SGD', 'FlyBase', 'WormBase', 'TAIR']:
        return ["biolink:Gene"]
    elif prefix == 'GO':
        if entity_type and 'molecular_function' in entity_type.lower():
            return ["biolink:MolecularActivity"]
        elif entity_type and 'biological_process' in entity_type.lower():
            return ["biolink:BiologicalProcess"]
        else:
            return ["biolink:Entity"]
    elif prefix == 'ECO':
        return ["biolink:EvidenceType"]
    elif prefix == 'PMID':
        return ["biolink:Publication"]
    elif prefix == 'NCBITaxon':
        return ["biolink:OrganismTaxon"]
    elif 'gomodel:' in entity_id:
        return ["biolink:BiologicalProcessOrActivity"]
    else:
        return ["biolink:Entity"]


def get_entity_class_and_category(entity_id: str, entity_type: str = None):
    """Get the appropriate biolink class and category for an entity."""
    prefix = extract_curie_prefix(entity_id)

    if prefix in ['ZFIN', 'MGI', 'RGD', 'SGD', 'FlyBase', 'WormBase', 'TAIR']:
        return Gene, ["biolink:Gene"]
    elif prefix == 'GO':
        if entity_type and 'molecular_function' in entity_type.lower():
            return MolecularActivity, ["biolink:MolecularActivity"]
        elif entity_type and 'biological_process' in entity_type.lower():
            return BiologicalProcessOrActivity, ["biolink:BiologicalProcessOrActivity"]
        else:
            return Entity, ["biolink:Entity"]
    elif 'gomodel:' in entity_id:
        return BiologicalProcessOrActivity, ["biolink:BiologicalProcessOrActivity"]
    else:
        return Entity, ["biolink:Entity"]


INFORES_GO_CAM = "infores:go-cam"


@koza.transform_record()
def transform_record(koza: koza.KozaTransform, record: dict[str, Any]) -> (Iterable[NamedThing], Iterable[Association]):
    entities = []
    associations = []
    entities_written = set()
    
    model_data = record
    model_id = model_data.get('id')
    title = model_data.get('title', '')
    
    # Process objects section for entity metadata
    objects_dict = {}
    if 'objects' in model_data:
        for obj in model_data['objects']:
            obj_id = obj.get('id')
            if obj_id:
                objects_dict[obj_id] = {
                    'label': obj.get('label', ''),
                    'type': obj.get('type', '')
                }

    # Process activities
    if 'activities' in model_data:
        for activity in model_data['activities']:
            activity_id = activity.get('id')
            if not activity_id:
                continue

            # Create activity node
            if activity_id not in entities_written:
                entity_class, category = get_entity_class_and_category(activity_id)
                activity_entity = entity_class(
                    id=activity_id,
                    name=f"Activity from {title}",
                    category=category
                )
                entities.append(activity_entity)
                entities_written.add(activity_id)

            # Process enabled_by relationship
            if 'enabled_by' in activity:
                enabled_by = activity['enabled_by']
                gene_id = enabled_by.get('term')

                if gene_id:
                    # Create gene entity
                    if gene_id not in entities_written:
                        gene_label = objects_dict.get(gene_id, {}).get('label', gene_id)
                        entity_class, category = get_entity_class_and_category(gene_id)
                        gene_entity = entity_class(
                            id=gene_id,
                            name=gene_label,
                            category=category
                        )
                        entities.append(gene_entity)
                        entities_written.add(gene_id)

                    # Create enabled_by association
                    evidence_info = enabled_by.get('evidence', [{}])[0] if enabled_by.get('evidence') else {}

                    enabled_by_assoc = Association(
                        id=str(uuid.uuid4()),
                        subject=activity_id,
                        predicate="biolink:enabled_by",
                        object=gene_id,
                        category=["biolink:Association"],
                        primary_knowledge_source=INFORES_GO_CAM,
                        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                        agent_type=AgentTypeEnum.manual_agent
                    )

                    # Add evidence if present
                    if evidence_info.get('reference'):
                        enabled_by_assoc.publications = [evidence_info['reference']]

                    associations.append(enabled_by_assoc)

            # Process molecular_function relationship
            if 'molecular_function' in activity:
                mf = activity['molecular_function']
                mf_term = mf.get('term')

                if mf_term:
                    # Create molecular activity entity
                    if mf_term not in entities_written:
                        mf_label = objects_dict.get(mf_term, {}).get('label', mf_term)
                        mf_type = objects_dict.get(mf_term, {}).get('type', '')
                        entity_class, category = get_entity_class_and_category(mf_term, mf_type)
                        mf_entity = entity_class(
                            id=mf_term,
                            name=mf_label,
                            category=category
                        )
                        entities.append(mf_entity)
                        entities_written.add(mf_term)

                    # Create molecular function association
                    mf_assoc = Association(
                        id=str(uuid.uuid4()),
                        subject=activity_id,
                        predicate="biolink:has_molecular_function",
                        object=mf_term,
                        category=["biolink:Association"],
                        primary_knowledge_source=INFORES_GO_CAM,
                        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                        agent_type=AgentTypeEnum.manual_agent
                    )

                    associations.append(mf_assoc)

    # Process any remaining objects not yet written
    for obj_id, obj_data in objects_dict.items():
        if obj_id not in entities_written:
            entity_class, category = get_entity_class_and_category(obj_id, obj_data.get('type'))
            entity = entity_class(
                id=obj_id,
                name=obj_data.get('label', obj_id),
                category=category
            )
            entities.append(entity)
            entities_written.add(obj_id)

    return entities, associations