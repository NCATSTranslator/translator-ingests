import translator_ingest.util.biolink as util
from biolink_model.datamodel.pydanticmodel_v2 import (
    DirectionQualifierEnum,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    CausalMechanismQualifierEnum,
)


## hard-coded biolink predicates
BIOLINK_TREATS = "biolink:treats"
BIOLINK_STUDIED_TREAT = "biolink:studied_to_treat"
BIOLINK_PRECLINICAL = "biolink:in_preclinical_trials_for"
BIOLINK_CLINICAL_TRIALS = "biolink:in_clinical_trials_for"
BIOLINK_AFFECTS = "biolink:affects"
BIOLINK_DP_INTERACTS = "biolink:directly_physically_interacts_with"
BIOLINK_CAUSES = "biolink:causes"


## hard-coded mapping of "clinical status" values to biolink "treats" predicates
## purposely doesn't include all values - rest are filtered out
CLINICAL_STATUS_MAP = {
    ## treats
    "Approved": BIOLINK_TREATS,
    "Approved (orphan drug)": BIOLINK_TREATS,
    "Approved in China": BIOLINK_TREATS,
    "Approved in EU": BIOLINK_TREATS,
    "Phase 4": BIOLINK_TREATS,
    ## studied to treat
    "Investigative": BIOLINK_STUDIED_TREAT,
    "Patented": BIOLINK_STUDIED_TREAT,
    ## in preclinical trials for
    "Preclinical": BIOLINK_PRECLINICAL,
    "IND submitted": BIOLINK_PRECLINICAL,
    ## in clinical trials for
    "Clinical Trial": BIOLINK_CLINICAL_TRIALS,
    "Clinical trial": BIOLINK_CLINICAL_TRIALS,
    "Preregistration": BIOLINK_CLINICAL_TRIALS,
    "Registered": BIOLINK_CLINICAL_TRIALS,
    "Phase 0": BIOLINK_CLINICAL_TRIALS,
    "Phase 1": BIOLINK_CLINICAL_TRIALS,
    "Phase 1b": BIOLINK_CLINICAL_TRIALS,
    "Phase 1/2": BIOLINK_CLINICAL_TRIALS,
    "Phase 1/2a": BIOLINK_CLINICAL_TRIALS,
    "Phase 1b/2a": BIOLINK_CLINICAL_TRIALS,
    "Phase 2": BIOLINK_CLINICAL_TRIALS,
    "Phase 2a": BIOLINK_CLINICAL_TRIALS,
    "Phase 2b": BIOLINK_CLINICAL_TRIALS,
    "Phase 2/3": BIOLINK_CLINICAL_TRIALS,
    "Phase 3": BIOLINK_CLINICAL_TRIALS,
    "phase 3": BIOLINK_CLINICAL_TRIALS,
    "NDA filed": BIOLINK_CLINICAL_TRIALS,
    "BLA submitted": BIOLINK_CLINICAL_TRIALS,
    "Approval submitted": BIOLINK_CLINICAL_TRIALS,
}

## indication names that are known to be problematic - not "conditions that are treated" or I'm worried how the statement will look
STRINGS_TO_FILTER = [
    "imaging",
    "radio",  ## related to imaging
    "esthesia",  ## for multiple spellings of an(a)esthesia
    "abortion",  ## problematic? but "spontaneous abortion" aka miscarriage can be treated...
    "sedation",
    "Discover",
    "icide",  ## catches Herbicide, Insecticide, etc. But catches "poisoning" due to these things too
    "procedure",
    "barrier",  ## catches Blood brain barrier
    "astringent",
    "stimul",  ## catches "Caerulein stimulated..." and ovarian stimulation
    "suppress",  ## catches Appetite suppressant
    "contrast",  ## related to imaging
    "Diagnostic",  ## diagnostic
    "vasodilator",
    "Dutch elm disease",  ## this is a plant disease
    "Exam",
    "lubricant",
    "Localisation",
    "Measur",  ## catches Measure kidney function
    "Pest attack",  ## plant disease?
    "Plant grey",  ## catches Plant grey mould disease
    "Stabil",  ## catches Stabilize muscle contraction
    "canine",  ## Canine and feline spontaneous neoplasm
]


## moa -> predicate, qualifier-set, extra edge's predicate
## imported enum from pydantic (vs hard-coded values)
## there's also no value (None), which maps to plain "interacts_with" edge and is handled in main code
MOA_MAPPING = {
    "activator": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.activation,
        },
    },
    "agonist": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.agonism,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "antagonist": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.antagonism,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    ## assuming this is same as "antisense inhibition"
    "antisense": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.expression,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.antisense_oligonucleotide_inhibition,
        },
    },
    ## same modeling. original values: "binder", "ligand"
    "BINDING": {
        "predicate": BIOLINK_DP_INTERACTS,
        "qualifiers": {
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.binding,
        },
    },
    ## same modeling. original values: "blocker", "blocker (channel blocker)"
    "BLOCKING": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.molecular_channel_blockage,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "degrader": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.abundance,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.degradation,
        },
    },
    "disrupter": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.stability,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.disruption,
        },
    },
    "inducer": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.induction,
        },
    },
    "inhibitor": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.inhibition,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "inhibitor (gating inhibitor)": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.gating_inhibition,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "inverse agonist": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.inverse_agonism,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "modulator": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.modulation,
        },
    },
    ## from Allosteric modulator - Neutral: looked like 1 edge made
    "modulator (allosteric modulator)": {
        "predicate": BIOLINK_DP_INTERACTS,
        "qualifiers": {
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.allosteric_modulation,
        },
    },
    "opener": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.molecular_channel_opening,
        },
    },
    "partial agonist": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.partial_agonism,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "potentiator": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.potentiation,
        },
    },
    "stabilizer": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.abundance,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.stabilization,
        },
    },
    "stimulator": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.stimulation,
        },
    },
    "suppressor": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.suppression,
        },
    },
}