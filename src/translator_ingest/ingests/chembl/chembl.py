import uuid
import koza
import os
from typing import Any, Iterable
import tarfile
import sqlite3
import json

import biolink_model.datamodel.pydanticmodel_v2 as bm
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    ChemicalAffectsGeneAssociation,
    GeneAffectsChemicalAssociation
)
from koza.model.graphs import KnowledgeGraph

QUALIFIER_CONFIG_PATH = "src/translator_ingest/ingests/chembl/chembl_qualifiers.json"

LATEST_VERSION = "35"

INFORES_CHEMBL = "infores:chembl"

MOA_QUERY = """
        SELECT
            molecule_dictionary.chembl_id AS molecule_chembl_id,
            molecule_dictionary.pref_name AS molecule_name,
            drug_mechanism.mec_id,
            drug_mechanism.molregno,
            drug_mechanism.mechanism_of_action,
            drug_mechanism.action_type,
            drug_mechanism.direct_interaction,
            drug_mechanism.mechanism_comment,
            drug_mechanism.selectivity_comment,
            target_dictionary.tid,
            target_dictionary.chembl_id AS target_chembl_id,
            target_dictionary.pref_name AS target_name,
            target_dictionary.target_type,
            target_dictionary.tax_id AS target_organism_tax_id,
            binding_sites.site_name,
            drug_mechanism.binding_site_comment,
            source.src_description AS source_description,
            source.src_comment AS source_comment,
            docs.pubmed_id,
            docs.doi,
            docs.chembl_id AS document_chembl_id,
            variant_sequences.mutation,
            variant_sequences.accession AS mutation_accession
        FROM drug_mechanism
        JOIN molecule_dictionary ON molecule_dictionary.molregno=drug_mechanism.molregno
        JOIN target_dictionary ON target_dictionary.tid=drug_mechanism.tid
        LEFT JOIN binding_sites ON binding_sites.site_id=drug_mechanism.site_id
        LEFT JOIN compound_records ON compound_records.record_id=drug_mechanism.record_id
        LEFT JOIN docs ON (docs.doc_id=compound_records.doc_id AND compound_records.doc_id!=-1)
        LEFT JOIN source ON source.src_id=compound_records.src_id
        LEFT JOIN variant_sequences ON variant_sequences.variant_id = drug_mechanism.variant_id and drug_mechanism.variant_id != -1
    """

WHERE_DIRECT_INTERACTION = " WHERE direct_interaction = '1'"

TARGET_CLASS_MAP = {
    "ADMET": "Cell",
    "CELL-LINE": "CellLine",
    "CHIMERIC PROTEIN": "Protein",
    "LIPID": "MolecularEntity",
    "MACROMOLECULE": "MolecularEntity",
    "METAL": "MolecularEntity",
    "MOLECULAR": "MolecularEntity",
    "NO TARGET": None,
    "NON-MOLECULAR": "BiologicalEntity",
    "NUCLEIC-ACID": "NucleicAcidEntity",
    "OLIGOSACCHARIDE": "MolecularEntity",
    "ORGANISM": "CellularOrganism",
    "PROTEIN": "Protein",
    "PHENOTYPE": "PhenotypicFeature",
    "PROTEIN COMPLEX": "MacromolecularComplex",
    "PROTEIN COMPLEX GROUP": "MacromolecularComplex",
    "PROTEIN FAMILY": "ProteinFamily",
    "PROTEIN NUCLEIC-ACID COMPLEX": "MacromolecularComplex",
    "PROTEIN-PROTEIN INTERACTION": "MolecularActivity",
    "SELECTIVITY GROUP": "ProteinFamily",
    "SINGLE PROTEIN": "Protein",
    "SMALL MOLECULE": "SmallMolecule",
    "SUBCELLULAR": "CellularComponent",
    "TISSUE": "AnatomicalEntity",
    "UNCHECKED": None,
    "UNDEFINED": None,
    "UNKNOWN": None
}

CHEMBL_COMPOUND_PREFIX = "CHEMBL.COMPOUND:"
CHEMBL_TARGET_PREFIX = "CHEMBL.TARGET:"
TAX_ID_PREFIX = "NCBITaxon:"
PUBMED_PREFIX = "PMID:"
DOI_PREFIX = "DOI:"
CHEMBL_DODCUMENT_PREFIX = "CHEMBL.DOCUMENT:"

BIOLINK_DIRECTLY_INTERACTS_WITH = "biolink:directly_physically_interacts_with"

COMPONENT_QUERY = """
    SELECT
        component_sequences.component_type,
        component_sequences.accession,
        component_sequences.description,
        component_sequences.tax_id,
        component_sequences.organism,
        component_sequences.db_source
    FROM target_components
    JOIN component_sequences ON component_sequences.component_id = target_components.component_id
"""

WHERE_TID_CLAUSE = "    WHERE tid = ?"

QUALIFIER_CONFIG = {}

def load_config():
    global QUALIFIER_CONFIG
    with open(QUALIFIER_CONFIG_PATH, 'r') as f:
        config = json.load(f)
        for action_type, entry in config.items():
            association = entry["association"]
            predicate = entry["predicate"]
            qualifiers = {}
            for qualifier_type, qualifier_value in entry.get("qualifiers", {}).items():
              # skip TODO qualifiers until biolink model supports them
               if not qualifier_type.startswith("TODO:"): 
                qualifiers[qualifier_type] = qualifier_value
            QUALIFIER_CONFIG[action_type] = {
                "association": association,
                "predicate": predicate,
                "qualifiers": qualifiers
            }

load_config()


# This function returns the latest version of the data source.
def get_latest_version() -> str:
    return LATEST_VERSION


def get_connection(log=None) -> sqlite3.Connection:
    version = get_latest_version()
    download_file = f'data/chembl/chembl_{version}_sqlite.tar.gz'
    database_path = f'data/chembl/chembl_{version}/chembl_{version}_sqlite/chembl_{version}.db'
    if log:
        log(f"Using ChEMBL database at {database_path}", level="INFO")
    # uncompress tar.gz file
    if not os.path.exists(database_path):
        if log:
            log(f"Extracting {download_file} to {database_path} ...", level="INFO")
        with tarfile.open(download_file, "r:gz") as tar:
            tar.extractall(path='data/chembl/')
        if log:
            log("Extraction complete.", level="INFO")
    # create and return sqlite3 connection
    con = sqlite3.connect(database_path)
    con.row_factory = sqlite3.Row
    return con


def get_target_components(con: sqlite3.Connection, tid: int) -> list[dict[str, Any]]:
    cur = con.cursor()
    cur.execute(COMPONENT_QUERY+WHERE_TID_CLAUSE, (tid,))
    components = []
    for row in cur.fetchall():
        if row["component_type"] == "PROTEIN" and row["db_source"] in ("UNIPROT", "SWISS-PROT", "TREMBL"):
            record = {
                "accession": row["accession"],
                "description": row["description"],
                "tax_id": row["tax_id"],
                "organism": row["organism"],
                "db_source": row["db_source"]
            }
            components.append(record)
    if len(components) == 1:
        return components[0]
    return None


def build_protein_target(con, record):
    tid = record["tid"]
    id = CHEMBL_TARGET_PREFIX+record["target_chembl_id"]
    name = record["target_name"]

    components = get_target_components(con, tid)
    if components:
        if components["accession"]:
            xref = [id]
            id = "UNIPROT:"+components["accession"]
        synonym = [components["description"]]
        tax_id = [TAX_ID_PREFIX+str(components["tax_id"])]
    else:
        synonym = None
        xref = None
        tax_id = None
    return bm.Protein(
        id=id,
        name=name,
        synonym=synonym,
        xref=xref,
        in_taxon = tax_id
    )


def build_target_node(koza: koza.KozaTransform, record: dict[str, Any]):
    category = record["target_type"]
    if category not in TARGET_CLASS_MAP or TARGET_CLASS_MAP[category] is None:
        return None
    if TARGET_CLASS_MAP[category] == "Protein":
        return build_protein_target(koza.state['chembl_db_connection'], record)
    cls = getattr(bm, TARGET_CLASS_MAP[category])
    id = CHEMBL_TARGET_PREFIX+record["target_chembl_id"]
    name = record["target_name"]
    return cls(id=id, name=name)


def build_chemical_entity(record: dict[str, Any]):
    return ChemicalEntity(
        id=CHEMBL_COMPOUND_PREFIX+record["molecule_chembl_id"], 
        name=record["molecule_name"]
    )


def get_mutation_qualifier(record: dict[str, Any]):
    if record["mutation"] is not None:
        # TODO: waiting for mutation to be added to biolink model
        # return "mutation" 
        return "modified_form"
    return None


def get_species_context_qualifier(record: dict[str, Any]):
    if record["target_organism_tax_id"] is not None:
        return TAX_ID_PREFIX+str(record["target_organism_tax_id"])
    return None


def get_publications(record: dict[str, Any]):
    publications = []
    if record["pubmed_id"] is not None:
        publications.append(PUBMED_PREFIX+str(record["pubmed_id"]))
    elif record["doi"] is not None:
        publications.append(DOI_PREFIX+str(record["doi"]))
    elif record["document_chembl_id"] is not None:
        publications.append(CHEMBL_DODCUMENT_PREFIX+str(record["document_chembl_id"]))
    if len(publications) == 0:
        publications = None
    return publications


def get_association_class(association_type: str):
    if association_type == "ChemicalAffectsGeneAssociation":
        return ChemicalAffectsGeneAssociation
    if association_type == "GeneAffectsChemicalAssociation":
        return GeneAffectsChemicalAssociation
    return None

def get_association(koza, record, action_type_map):
    nodes = []
    edges = []
    chemical = build_chemical_entity(record)
    target = build_target_node(koza, record)
    if target is not None and action_type_map is not None:
        predicate = action_type_map["predicate"]
        accociation_type = action_type_map["association"]
        qualifiers = action_type_map["qualifiers"]

        nodes.append(chemical)
        nodes.append(target)

            # add qualifiers if available
        mutation_qualifier = get_mutation_qualifier(record)
        species_context_qualifier = get_species_context_qualifier(record)

            # add publications if available
        publications = get_publications(record)

        association_class = get_association_class(accociation_type)
        if association_class is None:
            koza.log(f" Unknown association class for action type {record['action_type']}", level="WARNING")
            return [], []
            # Create association
        association = association_class(
                id=str(uuid.uuid4()),
                subject=chemical.id,
                predicate=predicate,
                object=target.id,
                species_context_qualifier = species_context_qualifier,
                object_form_or_variant_qualifier = mutation_qualifier,
                primary_knowledge_source=INFORES_CHEMBL,
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                publications=publications,
                # TODO: add these fields when they are added to biolink model
                # binding_site_name = record["site_name"],
                # binding_site_comment = record["binding_site_comment"],
                # mechanism_of_action_description = record["mechanism_of_action"],
                # mechanism_of_action_comment = record["mechanism_comment"],
                # selectivity_comment = record["selectivity_comment"],
                # mutation = record["mutation"],
                # mutation_accession = record["mutation_accession"]
                **qualifiers
            )
        edges=[association]
    return nodes,edges


# The prepare function is responsible for any data download, decompression, or other preparation required
# before transformation. It should yield dictionaries, each representing a single record to be transformed.
@koza.prepare_data(tag="chembl_drug_mechanism_binding")
def prepare_bind(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:

    koza.log("ChEMBL drug mechanism binding data ...", level="INFO")
    con = get_connection(koza.log)
    koza.state['chembl_db_connection'] = con
    cur = con.cursor()
    cur.execute(MOA_QUERY + WHERE_DIRECT_INTERACTION)
    records = cur.fetchall()
    for record in records:
         yield record
    con.close()


@koza.transform(tag="chembl_drug_mechanism_binding")
def transform_bind(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    for record in data:
        action_type_map = QUALIFIER_CONFIG.get(record["action_type"])
        if action_type_map is None or action_type_map["predicate"] != "biolink:directly_physically_interacts_with":
            action_type_map = QUALIFIER_CONFIG.get("BINDING AGENT")
        nodes, edges = get_association(koza, record, action_type_map)
        yield KnowledgeGraph(nodes=nodes, edges=edges)


@koza.prepare_data(tag="chembl_drug_mechanism")
def prepare_mechanism(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:

    koza.log("ChEMBL drug mechanism data ...", level="INFO")
    con = get_connection(koza.log)
    koza.state['chembl_db_connection'] = con
    cur = con.cursor()
    cur.execute(MOA_QUERY)
    records = cur.fetchall()
    for record in records:
         yield record
    con.close()


@koza.transform(tag="chembl_drug_mechanism")
def transform_mechanism(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    for record in data:
        nodes = []
        edges = []
        action_type_map = QUALIFIER_CONFIG.get(record["action_type"])
        nodes, edges = get_association(koza, record, action_type_map)
        yield KnowledgeGraph(nodes=nodes, edges=edges)
