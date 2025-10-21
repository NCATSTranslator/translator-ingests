import uuid
import koza
import os
from typing import Any, Iterable
import tarfile
import sqlite3

import biolink_model.datamodel.pydanticmodel_v2 as bm
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    ChemicalAffectsGeneAssociation
)
from koza.model.graphs import KnowledgeGraph

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

# This function returns the latest version of the data source.
def get_latest_version() -> str:
    return LATEST_VERSION


def get_database_path():
    version = get_latest_version()
    download_file = f'data/chembl/chembl_{version}_sqlite.tar.gz'
    database_path = f'data/chembl/chembl_{version}/chembl_{version}_sqlite/chembl_{version}.db'
    # uncompress tar.gz file
    if not os.path.exists(database_path):
        print(f"Extracting {download_file} to {database_path} ...")
        with tarfile.open(download_file, "r:gz") as tar:
            tar.extractall(path='data/chembl/')
        print("Extraction complete.")
    return database_path


# The prepare function is responsible for any data download, decompression, or other preparation required
# before transformation. It should yield dictionaries, each representing a single record to be transformed.
@koza.prepare_data(tag="chembl_drug_mechanism_binding")
def prepare_bind(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:

    print("Preparing data...")
    database_path = get_database_path()
    con = sqlite3.connect(database_path)
    con.row_factory = sqlite3.Row
    koza.state['chembl_db_connection'] = con
    cur = con.cursor()
    cur.execute(MOA_QUERY + WHERE_DIRECT_INTERACTION)
    records = cur.fetchall()
    for record in records:
         yield record
    con.close()


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



def build_chemical_entity(record):
    return ChemicalEntity(
        id=CHEMBL_COMPOUND_PREFIX+record["molecule_chembl_id"], 
        name=record["molecule_name"]
    )


def get_mutation_qualifier(record):
    if record["mutation"] is not None:
        # TODO: waiting for mutation to be added to biolink model
        # return "mutation" 
        return "modified_form"
    return None


def get_species_context_qualifier(record):
    if record["target_organism_tax_id"] is not None:
        return TAX_ID_PREFIX+str(record["target_organism_tax_id"])
    return None


@koza.transform(tag="chembl_drug_mechanism_binding")
def transform_bind(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    for record in data:
        #print(record)
        nodes = []
        edges = []
        chemical = build_chemical_entity(record)
        target = build_target_node(koza, record)
        if target is not None:
            nodes.append(chemical)
            nodes.append(target)

            # add qualifiers if available
            mutation_qualifier = get_mutation_qualifier(record)
            species_context_qualifier = get_species_context_qualifier(record)

            # add publications if available
            publications = []
            if record["pubmed_id"] is not None:
                publications.append(PUBMED_PREFIX+str(record["pubmed_id"]))
            elif record["doi"] is not None:
                publications.append(DOI_PREFIX+str(record["doi"]))
            elif record["document_chembl_id"] is not None:
                publications.append(CHEMBL_DODCUMENT_PREFIX+str(record["document_chembl_id"]))
            if len(publications) == 0:
                publications = None
            
            # Create association
            association = ChemicalAffectsGeneAssociation(
                id=str(uuid.uuid4()),
                subject=chemical.id,
                predicate=BIOLINK_DIRECTLY_INTERACTS_WITH,
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
            )
            edges=[association]
        yield KnowledgeGraph(nodes=nodes, edges=edges)

