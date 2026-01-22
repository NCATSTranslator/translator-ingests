import translator_ingest.util.biolink as util
from biolink_model.datamodel.pydanticmodel_v2 import (
    DirectionQualifierEnum,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    CausalMechanismQualifierEnum,
)


## mapping values currently in data's interaction_source_db_name column
##   to the terms we want to use in Translator ingest
supporting_data_sources = {    ## become a RetrievalSource object in sources property
  ## key: most of the values from data's interaction_source_db_name column
  ## value: infores ID. sometimes same letters as key, sometimes NOT
  "CGI": util.INFORES_CGI,
  "CIViC": util.INFORES_CIVIC,
  "CKB-CORE": util.INFORES_CKB_CORE,
  "COSMIC": util.INFORES_COSMIC,
  "CancerCommons": util.INFORES_CANCERCOMMONS,
  "ChEMBL": util.INFORES_CHEMBL,
  "ClearityFoundationBiomarkers": util.INFORES_CLEARITY_BIOMARKERS,
  "ClearityFoundationClinicalTrial": util.INFORES_CLEARITY_CLINICAL,
  "DTC": util.INFORES_DTC,
  "DoCM": util.INFORES_DOCM,
  "FDA": util.INFORES_FDA_PGX,
  "GuideToPharmacology": util.INFORES_GTOPDB,
  "MyCancerGenome": util.INFORES_MYCANCERGENOME,
  "MyCancerGenomeClinicalTrial": util.INFORES_MYCANCERGENOME_TRIALS,
  "NCI": util.INFORES_NCIT,
  "OncoKB": util.INFORES_ONCOKB,
  "PharmGKB":util.INFORES_PHARMGKB,
  "TTD": util.INFORES_TTD,
}

publications = {     ## become an element in "publications" list
  ## key: a few values from data's interaction_source_db_name column
  ## value: corresponding publication that dgidb used
  ## from citation info in https://dgidb.org/browse/sources
  "TALC": "PMID:25535693",
  "TEND": "PMID:21804595",
  "TdgClinicalTrial": "PMID:24016212",
}


## hard-coded biolink predicates
BIOLINK_AFFECTS = "biolink:affects"
BIOLINK_DP_INTERACTS = "biolink:directly_physically_interacts_with"

# not sure if this is a Biolink Model Pydantic code generation bug,
# but a Biolink CURIE is not expected in a 'qualified_predicate' field
BIOLINK_CAUSES = "causes"

BIOLINK_INTERACTS = "biolink:interacts_with"
    

## interaction_type -> predicate, qualifier-set, extra edge's predicate
## imported enum from pydantic (vs hard-coded values)
## DOESN'T INCLUDE ALL VALUES: the ones that map to plain "interacts_with" edge are saved in hard-coded variable in main py
int_type_mapping = {
    ## same modeling. original values: {"other/unknown", "~NULL"} ("~NULL" is a placeholder for no value)
    "~PLAIN_INTERACTS": {
        "predicate": BIOLINK_INTERACTS,
        ## lack of qualifiers is handled in main py, by using .get(x, dict()) so "no key" returns empty dict
    },
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
    "antibody": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.antibody_inhibition,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "antisense oligonucleotide": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.expression,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.antisense_oligonucleotide_inhibition,
        },
    },
    "binder": {
        "predicate": BIOLINK_DP_INTERACTS,
        "qualifiers": {
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.binding,
        },
    },
    "blocker": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.molecular_channel_blockage,
        },
        "extra_edge_pred": BIOLINK_DP_INTERACTS,
    },
    "cleavage": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.abundance,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.cleavage,
        },
    },
    "immunotherapy": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
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
    "negative modulator": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.decreased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.negative_modulation,
        },
    },
    "positive modulator": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "qualified_predicate": BIOLINK_CAUSES,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.positive_modulation,
        },
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
    "vaccine": {
        "predicate": BIOLINK_AFFECTS,
        "qualifiers": {
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.vaccine_antigen,
        },
    },
}