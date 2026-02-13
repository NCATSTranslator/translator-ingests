from translator_ingest.util.biolink import (
    INFORES_DRUGMATRIX,
    INFORES_PDSP_KI,
    INFORES_WOMBAT_PK,
)
from biolink_model.datamodel.pydanticmodel_v2 import (
    DirectionQualifierEnum,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    CausalMechanismQualifierEnum,
)


## hard-coded biolink predicates
BIOLINK_CONTRAINDICATED = "biolink:contraindicated_in"
BIOLINK_TREATS = "biolink:treats"
BIOLINK_PREVENTS = "biolink:preventative_for_condition"
BIOLINK_DIAGNOSES = "biolink:diagnoses"
BIOLINK_AFFECTS = "biolink:affects"
BIOLINK_DP_INTERACTS = "biolink:directly_physically_interacts_with"
BIOLINK_CAUSES = "biolink:causes"
BIOLINK_INTERACTS = "biolink:interacts_with"
BIOLINK_SUBSTRATE = "biolink:has_substrate"

## omop_relationship: hard-coded mapping of relationship_name values to biolink predicates/edge attributes
## currently only "off-label use" has an edge attribute included
OMOP_RELATION_MAPPING = {
    "contraindication": {
        "predicate": BIOLINK_CONTRAINDICATED,
        ## no "edge-attributes" key is handled in main py, by using .get(x, dict()) so "no key" returns empty dict
    },
    "indication": {
    ## approved, on-label use according to https://pmc.ncbi.nlm.nih.gov/articles/PMC10692006/#Sec10
        "predicate": BIOLINK_TREATS,
    },
    "off-label use": {
        "predicate": BIOLINK_TREATS,
        "edge-attributes": {
            "clinical_approval_status": "off_label_use"
        },
    },
    "reduce risk": {
        "predicate": BIOLINK_PREVENTS,
    }, 
    "symptomatic treatment": {
        "predicate": BIOLINK_TREATS,
    }, 
    "diagnosis": {
        "predicate": BIOLINK_DIAGNOSES,
    }, 
}

## to turn some urls into CURIEs: only used for act_table_full right now
URL_TO_PREFIX = {
    "https://pubmed.ncbi.nlm.nih.gov/": "PMID:",
    "http://dx.doi.org/": "DOI:",
    "https://doi.org/": "DOI:",
}

## infores mapping: only used for act_table_full right now
INFORES_MAPPING = {
    "DRUG MATRIX": INFORES_DRUGMATRIX,
    "PDSP": INFORES_PDSP_KI,
    "WOMBAT-PK": INFORES_WOMBAT_PK,
}

## act_table_full: action_type values -> predicate, qualifier-set, extra edge's predicate
## imported enum from pydantic (vs hard-coded values)
ACTION_TYPE_MAPPING = {
    "ACTIVATOR": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.activation,
        },
    },
    "AGONIST": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.agonism,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "ALLOSTERIC ANTAGONIST": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.allosteric_antagonism,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "ALLOSTERIC MODULATOR": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.allosteric_modulation,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "ANTAGONIST": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.antagonism,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "ANTIBODY BINDING": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.antibody_inhibition,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "ANTISENSE INHIBITOR": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.expression,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.antisense_oligonucleotide_inhibition,
        },
    },
    "BINDING AGENT": {
        "predicate": BIOLINK_DP_INTERACTS,
        "qualifiers": {
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.binding,
        },
    },
    "BLOCKER": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.molecular_channel_blockage,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "GATING INHIBITOR": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.gating_inhibition,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "INHIBITOR": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.inhibition,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "INVERSE AGONIST": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.inverse_agonism,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "MODULATOR": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.modulation,
        },
    },
    "NEGATIVE ALLOSTERIC MODULATOR": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.negative_allosteric_modulation,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "NEGATIVE MODULATOR": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.negative_modulation,
        },
    },
    "OPENER": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.molecular_channel_opening,
        },
    },
    "OTHER": {
        "predicate": BIOLINK_INTERACTS,
        ## lack of qualifiers is handled in main py, by using .get(x, dict()) so "no key" returns empty dict
    },
    "PARTIAL AGONIST": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.partial_agonism,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "PHARMACOLOGICAL CHAPERONE": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.stability,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.chaperone_mediated_stabilization,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "POSITIVE ALLOSTERIC MODULATOR": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.positive_allosteric_modulation,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "POSITIVE MODULATOR": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.positive_modulation,
        },
    },
    "RELEASING AGENT": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.transport,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.release,
        },
    },
    "SUBSTRATE": {
        ## main py should flip subject/object to match predicate direction (protein -> chem)
        "predicate": BIOLINK_SUBSTRATE,
        ## lack of qualifiers is handled in main py, by using .get(x, dict()) so "no key" returns empty dict
    },
}