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
    GeneAffectsChemicalAssociation,
    ChemicalEntityToChemicalEntityAssociation,
    AnatomicalEntityToAnatomicalEntityPartOfAssociation
)

from koza.model.graphs import KnowledgeGraph

from bmt.pydantic import entity_id, build_association_knowledge_sources

from translator_ingest import INGESTS_PARSER_PATH

QUALIFIER_CONFIG_PATH = INGESTS_PARSER_PATH / "chembl" / "chembl_qualifiers.json"

LATEST_VERSION = "36"

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
            target_dictionary.tax_id AS organism_tax_id,
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

MOLECULE_QUERY = """
    SELECT
        molecule_dictionary.molregno,
        molecule_dictionary.pref_name,
        molecule_dictionary.chembl_id,
        molecule_dictionary.max_phase,
        molecule_dictionary.therapeutic_flag,
        molecule_dictionary.dosed_ingredient,
        lower(molecule_dictionary.molecule_type) AS molecule_type,
        molecule_dictionary.first_approval,
        molecule_dictionary.oral,
        molecule_dictionary.parenteral,
        molecule_dictionary.topical,
        molecule_dictionary.black_box_warning,
        molecule_dictionary.natural_product,
        molecule_dictionary.first_in_class,
        molecule_dictionary.chirality,
        molecule_dictionary.prodrug,
        molecule_dictionary.inorganic_flag,
        molecule_dictionary.usan_year,
        molecule_dictionary.availability_type,
        molecule_dictionary.usan_stem,
        molecule_dictionary.polymer_flag,
        molecule_dictionary.usan_substem,
        molecule_dictionary.usan_stem_definition,
        molecule_dictionary.withdrawn_flag,
        compound_structures.standard_inchi,
        compound_structures.standard_inchi_key,
        compound_structures.canonical_smiles
    FROM molecule_dictionary
    LEFT JOIN compound_structures ON compound_structures.molregno = molecule_dictionary.molregno
    WHERE molecule_dictionary.molregno = ?
"""

METABOLITES_QUERY = """
    SELECT 
        metabolism.met_id,
        metabolism.met_conversion AS metabolic_conversion,
        metabolism.met_comment AS metabolic_comment,
        metabolism.organism,
        metabolism.tax_id AS organism_tax_id,
        drug_records.molregno AS drug_molregno,
        drug_records.compound_name AS drug_name,
        substrate_records.molregno AS substrate_molregno,
        substrate_records.compound_name AS substrate_name,
        metabolite_records.molregno AS metabolite_molregno,
        metabolite_records.compound_name AS metabolite_name,
        metabolism.enzyme_tid AS tid,
        target_dictionary.chembl_id AS target_chembl_id,
        target_dictionary.pref_name AS target_name,
        target_dictionary.target_type AS enzyme_type
    FROM metabolism
    JOIN compound_records AS drug_records ON metabolism.drug_record_id = drug_records.record_id
    JOIN compound_records AS substrate_records ON metabolism.substrate_record_id = substrate_records.record_id
    JOIN compound_records AS metabolite_records ON metabolism.metabolite_record_id = metabolite_records.record_id
    LEFT JOIN target_dictionary ON (target_dictionary.tid = enzyme_tid and target_type != 'UNCHECKED')
"""

REFERENCE_QUERY = """
    SELECT 
        ref_type, ref_id, ref_url
    FROM {}
    WHERE {} = ?
"""

SYNONYM_QUERY = """
    SELECT syn_type, synonyms
    FROM molecule_synonyms
    WHERE molregno = ?
"""

COMPONENT_QUERY = """
    SELECT
        target_dictionary.tid,
        target_dictionary.target_type,
        target_dictionary.pref_name AS target_name,
        target_dictionary.chembl_id AS target_chembl_id,
        target_dictionary.tax_id AS organism_tax_id,
        component_sequences.component_type,
        component_sequences.accession,
        component_sequences.description,
        component_sequences.organism,
        component_sequences.tax_id AS component_tax_id,
        component_sequences.db_source
    FROM target_dictionary  
    JOIN target_components ON target_components.tid = target_dictionary.tid
    JOIN component_sequences ON component_sequences.component_id = target_components.component_id
    WHERE component_sequences.accession IS NOT NULL
"""

WHERE_TARGET_TYPE_IS_SINGLE_PROTEIN = """
    AND target_dictionary.target_type = 'SINGLE PROTEIN'
"""

WHERE_TARGET_TYPE_IS_COMPLEX = """
    AND target_dictionary.target_type in (
        'CHIMERIC PROTEIN',
        'PROTEIN COMPLEX',
        'PROTEIN COMPLEX GROUP',
        'PROTEIN FAMILY',
        'PROTEIN NUCLEIC-ACID COMPLEX',
        'PROTEIN-PROTEIN INTERACTION',
        'SELECTIVITY GROUP'
    )
"""

ACTIVITY_QUERY = """
    SELECT  
        activities.molregno,
        activities.activity_id,
        activities.standard_type,
        activities.standard_relation,
        activities.standard_value,
        activities.standard_units,
        activities.pchembl_value,
        activities.activity_comment,
        activities.data_validity_comment,
        activities.standard_text_value,
        activities.standard_upper_value,
        activities.uo_units,
        activities.potential_duplicate,
        activities.action_type,
        assays.chembl_id AS assay_chembl_id,
        assays.description AS assay_description,
        assays.assay_organism,
        assays.assay_cell_type,
        assays.assay_subcellular_fraction,
        assays.bao_format,
        assays.assay_category,
        assays.assay_tax_id AS organism_tax_id,
        assays.assay_tissue,
        assays.assay_cell_type,
        assays.relationship_type,
        assays.confidence_score,
        assays.curated_by,
        assays.src_id AS assay_source_id,
        assay_type.assay_desc AS assay_type,
        bioassay_ontology.label AS BAO_label,
        target_dictionary.chembl_id AS target_chembl_id,
        target_dictionary.pref_name AS target_name,
        target_dictionary.organism AS target_organism,
        target_dictionary.tax_id,
        target_dictionary.target_type,
        component_sequences.component_type,
        component_sequences.accession,
        tissue_dictionary.chembl_id AS assay_tissue_chembl_id,
        tissue_dictionary.uberon_id,
        tissue_dictionary.pref_name AS assay_tissue_name,
        docs.chembl_id AS document_chembl_id,
        docs.pubmed_id,
        docs.doi,
        relationship_type.relationship_desc AS relationship_description,
        confidence_score_lookup.description AS confidence_score_description,
        confidence_score_lookup.target_mapping,
        curation_lookup.description AS curation_description,
        drug_mechanism.mec_id
    FROM activities
    JOIN target_dictionary ON target_dictionary.tid = assays.tid
    JOIN assays ON activities.assay_id=assays.assay_id
    LEFT JOIN assay_type ON assay_type.assay_type=assays.assay_type
    LEFT JOIN bioassay_ontology on bioassay_ontology.bao_id = assays.bao_format
    LEFT JOIN relationship_type ON relationship_type.relationship_type = assays.relationship_type
    LEFT JOIN confidence_score_lookup ON confidence_score_lookup.confidence_score = assays.confidence_score
    LEFT JOIN curation_lookup ON curation_lookup.curated_by = assays.curated_by
    JOIN target_components ON (target_components.tid = target_dictionary.tid AND target_dictionary.target_type IN ('SINGLE PROTEIN'))
    JOIN component_sequences ON component_sequences.component_id = target_components.component_id
    LEFT JOIN drug_mechanism ON (drug_mechanism.molregno = activities.molregno AND drug_mechanism.tid = assays.tid AND activities.action_type = drug_mechanism.action_type)
    JOIN docs ON (activities.doc_id=docs.doc_id AND title != 'PubChem BioAssay data set')
    LEFT JOIN tissue_dictionary ON tissue_dictionary.tissue_id=assays.tissue_id
    WHERE (activity_comment = 'Active' OR activities.action_type IS NOT NULL) AND mec_id IS NULL
"""


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
CHEMBL_DOCUMENT_PREFIX = "CHEMBL.DOCUMENT:"
UNIPROT_PREFIX = "UniProtKB:"

REFERENCE_PREFIX_MAP = {'PMID':'PMID:', 'DOI':'doi:', 'ISBN': 'ISBN:', 'PubMed':'PMID:'}

AVAILABILITY_TYPES = {
    -2: "withdrawn",
    -1: None,
    0: "discontinued",
    1: "prescription only",
    2: "over the counter"
}

BIOLINK_DIRECTLY_INTERACTS_WITH = "biolink:directly_physically_interacts_with"

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
    # add default entry for action type ACTIVITY
    QUALIFIER_CONFIG["ACTIVITY"] = {
        "association": "ChemicalAffectsGeneAssociation",
        "predicate": "biolink:affects",
        "qualifiers": {}
    }

load_config()


# This function returns the latest version of the data source.
def get_latest_version() -> str:
    return LATEST_VERSION


def get_connection(koza: koza.KozaTransform) -> sqlite3.Connection:
    log = koza.log if hasattr(koza, 'log') else None
    version = get_latest_version()
    download_file = f"{koza.input_files_dir}/chembl_{version}_sqlite.tar.gz"
    database_path = f"{koza.input_files_dir}/chembl_{version}/chembl_{version}_sqlite/chembl_{version}.db"
    if log:
        log(f"Using ChEMBL database at {database_path}", level="INFO")
    # uncompress tar.gz file
    if not os.path.exists(database_path):
        if log:
            log(f"Extracting {download_file} to {database_path} ...", level="INFO")
        with tarfile.open(download_file, "r:gz") as tar:
            for member in tar.getmembers():
                if not member.name.startswith("chembl_"):
                    raise Exception("Unexpected file in ChEMBL tar archive: "+member.name)
            tar.extractall(path=koza.input_files_dir)
        if log:
            log("Extraction complete.", level="INFO")
    # create and return sqlite3 connection
    con = sqlite3.connect(database_path)
    con.row_factory = sqlite3.Row
    return con


def get_protein(chembl_id: str, name: str, record: dict[str, Any]) -> bm.Protein | None:
    if record["accession"] is None:
        return None
    if record["component_type"] != "PROTEIN":
        return None
    if record["db_source"] not in ("UNIPROT", "SWISS-PROT", "TREMBL"):
        return None
    
    uniprot_id = UNIPROT_PREFIX+record["accession"]
    synonym = record["description"]
    tax_id = TAX_ID_PREFIX+str(record["component_tax_id"]) if record["component_tax_id"] else None
    return bm.Protein(
        id=uniprot_id,
        name=name,
        synonym=[synonym] if synonym and synonym != name else None,
        xref=[chembl_id] if chembl_id else None,
        in_taxon = [tax_id]
    )


def get_all_proteins(koza: koza.KozaTransform) -> list[dict[str, Any]]:
    con = koza.state['chembl_db_connection']
    cur = con.cursor()
    cur.execute(COMPONENT_QUERY+WHERE_TARGET_TYPE_IS_SINGLE_PROTEIN)
    proteins = {}
    for record in cur.fetchall():
        chembl_id = CHEMBL_TARGET_PREFIX+record["target_chembl_id"]
        name = record["target_name"]
        protein = get_protein(chembl_id, name, record)
        if protein:
            proteins[chembl_id] = protein
            proteins[protein.id] = protein
    return proteins


def build_target_node(koza: koza.KozaTransform, record: dict[str, Any]):
    category = record["target_type"]
    if category not in TARGET_CLASS_MAP or TARGET_CLASS_MAP[category] is None:
        return None
    if category in ("PROTEIN", "SINGLE PROTEIN"):
        chembl_id = CHEMBL_TARGET_PREFIX+record["target_chembl_id"]
        return koza.state['chembl_proteins'].get(chembl_id)
    cls = getattr(bm, TARGET_CLASS_MAP[category])
    id = CHEMBL_TARGET_PREFIX+record["target_chembl_id"]
    name = record["target_name"]
    return cls(id=id, name=name)


def get_synonyms(koza: koza.KozaTransform, molregno: int) -> list[str] | None:
    cur = koza.state['chembl_db_connection'].cursor()
    cur.execute(SYNONYM_QUERY, (molregno,))
    synonyms = []
    for row in cur.fetchall():
        synonyms.append(row["synonyms"])
    if len(synonyms) > 0:
        return synonyms
    return None


def create_chemical_entity(koza: koza.KozaTransform, molregno: int, compound_name: str = None):
    cur = koza.state['chembl_db_connection'].cursor()
    cur.execute(MOLECULE_QUERY, (molregno,))
    record = cur.fetchone()
    if record:
        name = record["pref_name"]
        xref = []
        if record["standard_inchi_key"] is not None:
            xref.append("InChIKey:"+record["standard_inchi_key"])
        if record["canonical_smiles"] is not None:
            xref.append("SMILES:"+record["canonical_smiles"])
        routes_of_delivery = []
        if record["oral"] == 1:
            routes_of_delivery.append("oral")
        if record["parenteral"] == 1:
            routes_of_delivery.append("injection")
        if record["topical"] == 1:
            routes_of_delivery.append("absorption_through_the_skin")
        return ChemicalEntity(
            id=CHEMBL_COMPOUND_PREFIX+record["chembl_id"],
            name=name,
            xref=xref if len(xref) > 0 else None,
            synonym=get_synonyms(koza, molregno),
            # TODO: routes_of_delivery=routes_of_delivery if len(routes_of_delivery) > 0 else None,
            chembl_availability_type = AVAILABILITY_TYPES.get(record["availability_type"]),
            chembl_black_box_warning="True" if record["black_box_warning"] == 1 else None,
            chembl_natural_product=True if record["natural_product"] == 1 else None,
            chembl_prodrug=True if record["prodrug"] == 1 else None,
        )
    return None


def create_component_node(koza: koza.KozaTransform, record: dict[str, Any]):
    category = record["component_type"]
    if category == "PROTEIN" and record["accession"] is not None:
        uniprot_id = UNIPROT_PREFIX+record["accession"]
        if uniprot_id in koza.state['chembl_proteins']:
            return koza.state['chembl_proteins'][uniprot_id]
        return get_protein(None, record["description"], record)
    return None


def get_mutation_qualifier(record: dict[str, Any]):
    if record["mutation"] is not None:
        return "mutant_form"
    return None


def get_species_context_qualifier(record: dict[str, Any]):
    if record["organism_tax_id"] is not None:
        return TAX_ID_PREFIX+str(record["organism_tax_id"])
    return None


def get_enzyme_context_qualifier(koza: koza.KozaTransform, record: dict[str, Any]):
    if record["tid"] is None or record["target_chembl_id"] is None:
        return None
    chembl_id = CHEMBL_TARGET_PREFIX+str(record["target_chembl_id"])
    protein = koza.state['chembl_proteins'].get(chembl_id)
    return protein.id if protein else None


def get_publications(koza: koza.KozaTransform, record: dict[str, Any]):
    publication = None
    if record["pubmed_id"] is not None:
        publication = PUBMED_PREFIX+str(record["pubmed_id"])
    elif record["doi"] is not None:
        publication = DOI_PREFIX+str(record["doi"])
    elif record["document_chembl_id"] is not None:
        publication = CHEMBL_DOCUMENT_PREFIX+str(record["document_chembl_id"])

    publications = []
    if publication:
        publications.append(publication)
    if record['mec_id'] is not None:
        for ref in get_references(koza.state['chembl_db_connection'], 'mechanism_refs', 'mec_id', record['mec_id']):
            if ref != publication:
                publications.append(ref)
    if len(publications) == 0:
        publications = None
    return publications


def get_reference(ref_type: str, ref_id: str, ref_url: str) -> str:
    if ref_type in REFERENCE_PREFIX_MAP:
        prefix = REFERENCE_PREFIX_MAP[ref_type]
        return prefix + str(ref_id)
    if ref_url:
        return ref_url
    return None


def get_references(con: sqlite3.Connection, reference_table: str, reference_id_field: str, reference_id: Any) -> list[str]:
    cur = con.cursor()
    cur.execute(REFERENCE_QUERY.format(reference_table, reference_id_field), (reference_id,))
    references = []
    for row in cur.fetchall():
        reference = get_reference(row["ref_type"], row["ref_id"], row["ref_url"])
        if reference:
            references.append(reference)
    return references


def get_association_class(association_type: str):
    if association_type == "ChemicalAffectsGeneAssociation":
        return ChemicalAffectsGeneAssociation
    if association_type == "GeneAffectsChemicalAssociation":
        return GeneAffectsChemicalAssociation
    return None


def get_association(koza, record, action_type_map):
    nodes = []
    edges = []
    chemical = create_chemical_entity(koza, record['molregno'])
    target = build_target_node(koza, record)
    if target is not None and action_type_map is not None:
        predicate = action_type_map["predicate"]
        association_type = action_type_map["association"]
        qualifiers = action_type_map["qualifiers"]

        nodes.append(chemical)
        nodes.append(target)

            # add qualifiers if available
        mutation_qualifier = get_mutation_qualifier(record)
        species_context_qualifier = get_species_context_qualifier(record)

            # add publications if available
        publications = get_publications(koza, record)

        association_class = get_association_class(association_type)
        if association_class is None:
            koza.log(f" Unknown association class for action type {record['action_type']}", level="WARNING")
            return [], []
            # Create association
        association = association_class(
                id=entity_id(),
                subject=chemical.id,
                predicate=predicate,
                object=target.id,
                species_context_qualifier = species_context_qualifier,
                object_form_or_variant_qualifier = mutation_qualifier,
                sources=build_association_knowledge_sources(INFORES_CHEMBL),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                publications=publications,
                # add these fields when they are added to biolink model
                # TODO: binding_site_name = record["site_name"],
                # TODO: binding_site_comment = record["binding_site_comment"],
                # TODO: mechanism_of_action_description = record["mechanism_of_action"],
                # TODO: mechanism_of_action_comment = record["mechanism_comment"],
                # TODO: mutation = record["mutation"],
                # TODO: mutation_accession = record["mutation_accession"],
                # TODO: selectivity_comment = record["selectivity_comment"],
                **qualifiers
            )
        edges=[association]
    return nodes,edges


def get_activity_association(koza: koza.KozaTransform, chemical, target, action_type_map, record: dict[str, Any]) -> ChemicalAffectsGeneAssociation | GeneAffectsChemicalAssociation | None:
    species_context_qualifier = get_species_context_qualifier(record)
    anatomical_context_qualifier = record["uberon_id"]
    publications = get_publications(koza, record)
    predicate = action_type_map["predicate"]
    qualifiers = action_type_map["qualifiers"]
    association = ChemicalAffectsGeneAssociation(
        id=entity_id(),
        subject=chemical.id,
        predicate=predicate,
        object=target.id,
        species_context_qualifier = species_context_qualifier,
        anatomical_context_qualifier = [anatomical_context_qualifier] if anatomical_context_qualifier else None,
        sources=build_association_knowledge_sources(INFORES_CHEMBL),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.automated_agent if record["curated_by"] == "Autocuration" else AgentTypeEnum.manual_agent,
        publications=publications,
        has_confidence_score = record["confidence_score"],
        # TODO: assay_description = record["assay_description"],
        **qualifiers
    )
    return association


def create_chemical_association(koza: koza.KozaTransform, substrate, metabolite, record: dict[str, Any]) -> ChemicalEntityToChemicalEntityAssociation:
    species_context_qualifier = get_species_context_qualifier(record)
    context_qualifier = get_enzyme_context_qualifier(koza, record)
    context_qualifier = context_qualifier
    connection = koza.state['chembl_db_connection']
    references = get_references(connection, "metabolism_refs", "met_id", record["met_id"])
    association = ChemicalEntityToChemicalEntityAssociation(
        id=entity_id(),
        subject=substrate.id,
        predicate="biolink:has_metabolite",
        object=metabolite.id,
        species_context_qualifier = species_context_qualifier,
        # TODO: context_qualifier = context_qualifier,
        sources=build_association_knowledge_sources(INFORES_CHEMBL),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
        publications=references if len(references) > 0 else None,
        # TODO: metabolic_conversion = record["metabolic_conversion"],
        # TODO: metabolic_conversion_comment = record["metabolic_comment"],
    )
    return association


def get_has_part_association(koza: koza.KozaTransform, component, target, record: dict[str, Any]) -> AnatomicalEntityToAnatomicalEntityPartOfAssociation:
    species_context_qualifier = get_species_context_qualifier(record)
    species_context_qualifier = species_context_qualifier
    association = AnatomicalEntityToAnatomicalEntityPartOfAssociation(
        id=entity_id(),
        subject=target.id,
        predicate="biolink:has_part",
        object=component.id,
        # TODO: species_context_qualifier = species_context_qualifier,
        sources=build_association_knowledge_sources(INFORES_CHEMBL),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    return association


# The prepare function is responsible for any data download, decompression, or other preparation required
# before transformation. It should yield dictionaries, each representing a single record to be transformed.
@koza.prepare_data(tag="chembl_drug_mechanism_binding")
def prepare_bind(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:

    koza.log("ChEMBL drug mechanism binding data ...", level="INFO")
    con = get_connection(koza)
    koza.state['chembl_db_connection'] = con
    proteins = get_all_proteins(koza)
    koza.state['chembl_proteins'] = proteins
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
    con = get_connection(koza)
    koza.state['chembl_db_connection'] = con
    proteins = get_all_proteins(koza)
    koza.state['chembl_proteins'] = proteins
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


@koza.prepare_data(tag="chembl_metabolites")
def prepare_metabolites(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:
    koza.log("ChEMBL metabolites data ...", level="INFO")
    con = get_connection(koza)
    koza.state['chembl_db_connection'] = con
    proteins = get_all_proteins(koza)
    koza.state['chembl_proteins'] = proteins
    cur = con.cursor()
    cur.execute(METABOLITES_QUERY)
    records = cur.fetchall()
    for record in records:
         yield record
    con.close()


@koza.transform(tag="chembl_metabolites")
def transform_metabolites(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    for record in data:
        nodes = []
        edges = []
        substrate = create_chemical_entity(koza, record['substrate_molregno'])
        metabolite = create_chemical_entity(koza, record['metabolite_molregno'])
        if substrate and metabolite:
            nodes.append(substrate)
            nodes.append(metabolite)
            association = create_chemical_association(koza, substrate, metabolite, record)
            edges.append(association)
            if record['drug_molregno'] != record['substrate_molregno']:
                chemical = create_chemical_entity(koza, record['drug_molregno'])
                nodes.append(chemical)
                association = create_chemical_association(koza, chemical, metabolite, record)
                edges.append(association)
        yield KnowledgeGraph(nodes=nodes, edges=edges)


@koza.prepare_data(tag="chembl_complexes")
def prepare_complexes(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:
    koza.log("ChEMBL complexes data ...", level="INFO")
    con = get_connection(koza)
    koza.state['chembl_db_connection'] = con
    proteins = get_all_proteins(koza)
    koza.state['chembl_proteins'] = proteins
    cur = con.cursor()
    cur.execute(COMPONENT_QUERY + WHERE_TARGET_TYPE_IS_COMPLEX)
    records = cur.fetchall()
    for record in records:
         yield record
    con.close()


@koza.transform(tag="chembl_complexes")
def transform_complexes(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    for record in data:
        nodes = []
        edges = []
        target = build_target_node(koza, record)
        if target:
            component = create_component_node(koza, record)
            if component:
                nodes.append(component)
                nodes.append(target)    
                association = get_has_part_association(koza, component, target, record)
                edges.append(association)
        yield KnowledgeGraph(nodes=nodes, edges=edges)


@koza.prepare_data(tag="chembl_activities")
def prepare_activities(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:
    koza.log("ChEMBL activities data ...", level="INFO")
    con = get_connection(koza)
    koza.state['chembl_db_connection'] = con
    proteins = get_all_proteins(koza)
    koza.state['chembl_proteins'] = proteins
    koza.state['counter'] = 0
    cur = con.cursor()
    cur.execute(ACTIVITY_QUERY)
    records = cur.fetchall()
    for record in records:
         yield record
    con.close()


@koza.transform(tag="chembl_activities")
def transform_activities(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    processed_activities = set()
    for record in data:
        koza.state['counter'] += 1
        if koza.state['counter'] % 10000 == 0:
            koza.log(f" Processed {koza.state['counter']} activity records...", level="INFO")
        nodes = []
        edges = []
        chemical = create_chemical_entity(koza, record['molregno'])
        target = build_target_node(koza, record)
        action_type = record['action_type'] if record['action_type'] is not None else "ACTIVITY"
        if target is not None:
            activity_key = (chemical.id, target.id, action_type)
            if activity_key in processed_activities:
                continue
            processed_activities.add(activity_key)
            action_type_map = QUALIFIER_CONFIG.get(action_type)
            if action_type_map is None:
                koza.log(f" Unknown action type '{action_type}' in activities", level="WARNING")
                action_type_map = QUALIFIER_CONFIG.get("ACTIVITY")
            association = get_activity_association(koza, chemical, target, action_type_map, record)
            if association is not None:
                nodes.append(chemical)
                nodes.append(target)
                edges.append(association)
        yield KnowledgeGraph(nodes=nodes, edges=edges)
