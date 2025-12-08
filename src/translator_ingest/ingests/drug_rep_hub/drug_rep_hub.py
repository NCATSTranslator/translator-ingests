import koza
import json
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

feature_map = {
    'agent for': ('biolink:has_chemical_role', 'biolink:chemical_role_of'),
    'aid for': ('biolink:ameliorates_condition', 'biolink:condition_ameliorated_by'),
    'control for': ('biolink:treats', 'biolink:treated_by'),
    'diagnostic for': ('biolink:diagnoses', 'biolink:is_diagnosed_by'),
    'indication for': ('biolink:treats', 'biolink:treated_by'),
    'reversal for': ('biolink:ameliorates_condition', 'biolink:condition_ameliorated_by'),
    'support for': ('biolink:ameliorates_condition', 'biolink:condition_ameliorated_by')
}

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
            # nodes.append(chemical)
            SAMPLES[name][id] = chemical
        yield KnowledgeGraph(nodes=nodes, edges=[])


@koza.transform(tag="ingest_drug_rep_hub_annotations")
def transform_drug_rep_hub_annotations(
    koza: koza.KozaTransform, data: Iterable[dict[str, Any]]
) -> Iterable[KnowledgeGraph]:
    global SAMPLES
    for record in data:
        nodes = []
        edges = []
        pert_iname = record["pert_iname"]
        clinical_phase = record["clinical_phase"]
        moa = record["moa"]
        target = record["target"]
        disease_area = record["disease_area"]
        indication = record["indication"]
        if pert_iname in SAMPLES:
            for chem_id, chemical in SAMPLES[pert_iname].items():
                nodes.append(chemical)

        yield KnowledgeGraph(nodes=nodes, edges=edges)


if __name__ == "__main__":
    file = '/chembio/datasets/csdev/VD/data/translator/rephub/2020-03-24/repurposing_indications.txt'
    indications = {}
    feature_actions = set()
    with open(file, 'r') as f:
        f.readline()  # skip header
        for line in f:
            row = line.strip().split('\t')
            indications[row[0]] = {
                'xref': row[1],
                'primary_name': row[2].strip(),
                'feature_action': row[3],
                'predicate': feature_map[row[3]][0],
            }
            feature_actions.add(row[3])
    print(f"Found {len(indications)} indications with {len(feature_actions)} feature actions: {feature_actions}")
    print(feature_actions)
    json.dump(indications, open('src/translator_ingest/ingests/drug_rep_hub/indications_config.json', 'w'), indent=2, sort_keys=True)
