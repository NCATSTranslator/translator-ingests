## hard-coded biolink predicates
BIOLINK_CONTRAINDICATED = "biolink:contraindicated_in"
BIOLINK_TREATS = "biolink:treats"
BIOLINK_PREVENTS = "biolink:preventative_for_condition"
BIOLINK_DIAGNOSES = "biolink:diagnoses"

## omop_relationship: hard-coded mapping of relationship_name values to biolink predicates/edge attributes
## currently only "off-label use" has an edge attribute included
OMOP_RELATION_MAPPING = {
    "contraindication": {
        "predicate": BIOLINK_CONTRAINDICATED,
        ## no "edge-attributes" key is handled in main py, by using .get(x, dict()) so "no key" returns empty dict
    },
    "indication": {
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