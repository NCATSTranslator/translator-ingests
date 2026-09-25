from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    ChemicalEntityToChemicalEntityAssociation,
)

## hard-coded biolink predicates
BIOLINK_RELATED_TO = "biolink:related_to"
BIOLINK_CAUSES = "biolink:causes"
BIOLINK_DRUG_INTERACT = "biolink:pharmacologically_interacts_with"
BIOLINK_PHYS_INTERACTS = "biolink:physically_interacts_with"
BIOLINK_NEG_CORRELATE = "biolink:negatively_correlated_with"
BIOLINK_POS_CORRELATE = "biolink:positively_correlated_with"
BIOLINK_TREATS_OR = "biolink:treats_or_applied_or_studied_to_treat"


## relation definitions from https://www.ncbi.nlm.nih.gov/research/pubtator3/tutorial "Relation Annotations", Table 2
## if "association" isn't assigned, using default - see main py
RELATION_MODELING = {
    ## def: "The associated relation with no specific description. This type applies to various entity pairs."
    "associate": {
        "predicate": BIOLINK_RELATED_TO,
    },
    ## def: "A positive correlation...This type includes chemical-induced diseases and genetic diseases caused by variants."
    ## currently only on Chemical - Disease, after excluding variant types
    ## HOWEVER, also used on Variant - Disease, Disease - Variant
    "cause": {
        "predicate": BIOLINK_CAUSES,
    },
    ## def: "A pharmacodynamic interaction between two chemicals that results in an array of side effects."
    ## currently only on Chemical - Chemical
    "drug_interact": {
        "predicate": BIOLINK_DRUG_INTERACT,
        "association": ChemicalEntityToChemicalEntityAssociation, 
    },
    ## def: "A negative correlation exists when the status of the two entities tends to be opposite. This type includes disease-gene and chemical-variant"
    ## currently only on Disease - Gene, after excluding variant types.
    ## HOWEVER, also used on Chemical - Variant. 
    "inhibit": {
        "predicate": BIOLINK_NEG_CORRELATE,
    },
    ## def: "Physical interaction, like protein-binding. This type includes gene-gene, gene-chemical, chemical-variant."
    ## currently on Chemical - Chemical, Chemical - Gene, Gene - Gene; after excluding variant types.
    ## HOWEVER, also used on Chemical - Variant. 
    "interact": {
        "predicate": BIOLINK_PHYS_INTERACTS,
    },
    ## def: "A negative correlation exists when the status of the two entities tends to be opposite. This type includes chemical-gene, chemical co-expression, and gene co-expression."
    ## currently on Chemical - Chemical, Chemical - Gene, Gene - Gene
    "negative_correlate": {
        "predicate": BIOLINK_NEG_CORRELATE,
    },
    ## def: "A positive correlation exists when the status of one entity tends to increase (or decrease) as the other increase (or decreases). This type includes chemical-gene, chemical co-expression, and gene co-expression."
    ## currently on Chemical - Chemical, Chemical - Gene, Gene - Gene
    "positive_correlate": {
        "predicate": BIOLINK_POS_CORRELATE,
    },
    ## def: "A negative correlation exists when the status of the two entities tends to be opposite. This type includes variant-disease."
    ## currently NOT IN DATA after excluding variant types
    ## On Variant - Disease, Disease - Variant
    "prevent": {
        "predicate": BIOLINK_NEG_CORRELATE,
    },
    ## def: "A positive correlation exists when the status of one entity tends to increase (or decrease) as the other increase (or decreases). This type includes disease-gene and disease-variant."
    ## currently on Disease - Gene; after excluding variant types.
    ## HOWEVER, also used on Chemical - Variant. 
    "stimulate": {
        "predicate": BIOLINK_POS_CORRELATE,
    },
    ## def: "A chemical/drug treats a disease."
    ## currently only on Chemical - Disease
    "treat": {
        "predicate": BIOLINK_TREATS_OR,
        "association": ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation, 
    },
}
