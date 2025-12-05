import koza
import re

from typing import Any, Iterable
from collections import defaultdict

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    Disease,
    NamedThing,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    Association,
)
from koza.model.graphs import KnowledgeGraph
from bmt.pydantic import entity_id, build_association_knowledge_sources


inchikey_regex = re.compile('[A-Z]{14}-[A-Z]{10}-[A-Z]')

INFORES_DRUG_REP_HUB = "infores:drug-repurposing-hub"
PUBCHEM_PREFIX = "PUBCHEM.COMPOUND:"
INCHIKEY_PREFIX = "INCHIKEY:"
SMILES_PREFIX = "SMILES:"

SAMPLES = defaultdict(dict)

# Always implement a function that returns a string representing the latest version of the source data.
# Ideally, this is the version provided by the knowledge source, directly associated with a specific data download.
# If a source does not implement versioning, we need to do it. For static datasets, assign a version string
# corresponding to the current version. For sources that are updated regularly, use file modification dates if
# possible, or the current date. Versions should (ideally) be sortable (ie YYYY-MM-DD) and should contain no spaces.
def get_latest_version() -> str:
    return "2025-08-19"



@koza.transform(tag="ingest_drug_rep_hub_samples")
def transform_drug_rep_hub_samples(
    koza: koza.KozaTransform, data: Iterable[dict[str, Any]]
) -> Iterable[KnowledgeGraph]:
    global SAMPLES
    for record in data:
        nodes = []
        name = record["pert_iname"]
        inchikey = record["InChIKey"] if inchikey_regex.match(record["InChIKey"]) else None
        source_name = record["vendor_name"]
        smiles = record["smiles"]
        pubchem_id = record["pubchem_cid"]

        id = None
        xref = None
        if pubchem_id.isdigit():
            id = PUBCHEM_PREFIX + str(pubchem_id)
            xref = [INCHIKEY_PREFIX + inchikey] if inchikey else None
        elif inchikey:
            id = INCHIKEY_PREFIX + inchikey

        if id:
            synonyms = None
            if source_name and source_name.lower() != name.lower():
                synonyms = [source_name] if source_name != name else None
            chemical = ChemicalEntity(
                id=id,
                name=name,
                synonym=synonyms,
                xref=xref
            )
            nodes.append(chemical)
            SAMPLES[name][id] = chemical
        yield KnowledgeGraph(nodes=nodes, edges=[])
