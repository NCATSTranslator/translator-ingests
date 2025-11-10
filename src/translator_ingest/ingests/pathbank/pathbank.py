import uuid # generate unique identifiers
import koza # koza library
import os # for file operations
import time # for timing progress
import zipfile # for extracting zip files
import shutil # for copying files
from pathlib import Path # for file path operations
from typing import Any, Iterable # for type hints
import xmltodict # for XML parsing
from datetime import datetime # for date parsing

from koza.model.graphs import KnowledgeGraph # koza's knowledge graph object (nodes and edges)

from biolink_model.datamodel.pydanticmodel_v2 import (
    Pathway, NamedThing, Association,
    SmallMolecule, Protein, MacromolecularComplex, 
    MolecularActivity, NucleicAcidEntity, ChemicalEntity,
    KnowledgeLevelEnum, AgentTypeEnum
) # biolink model classes

from translator_ingest.util.biolink import INFORES_PATHBANK, entity_id, build_association_knowledge_sources # for source information
from translator_ingest.util.http_utils import get_modify_date # for getting file modification dates

def get_latest_version() -> str:
    """
    Returns the most recent modify date of the PathBank source files, with no spaces "%Y_%m_%d".
    Compares the modification dates of both the pathways CSV and PWML zip files and returns the most recent.
    """
    strformat = "%Y_%m_%d"
    
    # Get last-modified dates for each source data file
    pathways_modify_date = get_modify_date(
        "https://pathbank.org/downloads/pathbank_all_pathways.csv.zip",
        strformat
    )
    pwml_modify_date = get_modify_date(
        "https://pathbank.org/downloads/pathbank_all_pwml.zip",
        strformat
    )
    
    # Compare them and return the most recent date
    if datetime.strptime(pathways_modify_date, strformat) > datetime.strptime(pwml_modify_date, strformat):
        return pathways_modify_date
    else:
        return pwml_modify_date

@koza.prepare_data(tag="pathways")
def prepare_pathways_data(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    """Extract zip file and read CSV data for pathways."""
    import csv as csv_module
    
    source_data_dir = Path(koza.input_files_dir)
    
    # Extract pathbank_all_pathways.csv.zip if CSV doesn't exist
    pathways_zip = source_data_dir / "pathbank_all_pathways.csv.zip"
    csv_file = source_data_dir / "pathbank_pathways.csv"
    
    # If zip doesn't exist in current directory, check parent directory for other versions
    if not pathways_zip.exists():
        # Go up to data/pathbank level to search for v*/source_data directories
        pathbank_base_dir = source_data_dir.parent.parent
        # Check common version directories (v1, v2, etc.)
        for version_dir in pathbank_base_dir.glob("v*/source_data"):
            potential_zip = version_dir / "pathbank_all_pathways.csv.zip"
            if potential_zip.exists():
                koza.log(f"Found zip file in {version_dir}, copying to current directory...")
                # Ensure target directory exists
                pathways_zip.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(potential_zip, pathways_zip)
                break
    
    if pathways_zip.exists() and not csv_file.exists():
        koza.log(f"Extracting {pathways_zip.name}...")
        with zipfile.ZipFile(pathways_zip, "r") as zip_ref:
            zip_ref.extractall(source_data_dir)
        koza.log(f"Extracted {csv_file.name}")
    
    # Extract pathbank_all_pwml.zip if PWML directory doesn't exist or is empty
    pwml_zip = source_data_dir / "pathbank_all_pwml.zip"
    pwml_dir = source_data_dir / "pathbank_all_pwml"
    
    # If zip doesn't exist in current directory, check parent directory for other versions
    if not pwml_zip.exists():
        # Go up to data/pathbank level to search for v*/source_data directories
        pathbank_base_dir = source_data_dir.parent.parent
        # Check common version directories (v1, v2, etc.)
        for version_dir in pathbank_base_dir.glob("v*/source_data"):
            potential_zip = version_dir / "pathbank_all_pwml.zip"
            if potential_zip.exists():
                koza.log(f"Found PWML zip file in {version_dir}, copying to current directory...")
                # Ensure target directory exists
                pwml_zip.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(potential_zip, pwml_zip)
                break
    
    if pwml_zip.exists() and (not pwml_dir.exists() or not any(pwml_dir.glob("*.pwml"))):
        koza.log(f"Extracting {pwml_zip.name}...")
        with zipfile.ZipFile(pwml_zip, "r") as zip_ref:
            zip_ref.extractall(source_data_dir)
        koza.log(f"Extracted PWML files to {pwml_dir}")
    
    # Now read the CSV file and yield rows
    if csv_file.exists():
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv_module.DictReader(f, fieldnames=["SMPDB ID", "PW ID", "Name", "Subject", "Description"])
            # Skip header row if it exists
            first_row = next(reader, None)
            if first_row and first_row.get("SMPDB ID") == "SMPDB ID":
                # This was the header, skip it
                pass
            else:
                # First row was data, yield it
                yield first_row
            
            # Yield remaining rows
            for row in reader:
                yield row
    else:
        koza.log(f"CSV file {csv_file} not found after extraction attempt", level="WARNING")


@koza.on_data_begin(tag="pathways")
def on_data_begin(koza: koza.KozaTransform) -> None:
    # Initialize counters for missing fields
    koza.state["missing_fields"] = {
        "pathway_id": 0,
        "name": 0, 
        "description": 0,
        "subject": 0
    }
    koza.state["total_records"] = 0


def _create_compound_node_and_edges(compound: dict[str, Any], pathway_id: str) -> tuple[list[NamedThing], list[Association], dict[str, dict[str, str]]]:
    """Create SmallMolecule node and same_as edges for a compound.
    
    Returns:
        Tuple of (nodes, edges, translator_dict) where translator_dict maps compound_id to {equiv_id: prefix}
    """
    nodes = []
    edges = []
    
    # Extract compound data
    compound_id_raw = compound.get("id", {})
    if isinstance(compound_id_raw, dict):
        pwc_id = compound_id_raw.get("#text", "")
    else:
        pwc_id = str(compound_id_raw) if compound_id_raw else ""
    
    if not pwc_id:
        return [], [], {}
    
    name = _normalize_xml_value(compound.get("name"))
    description = _normalize_xml_value(compound.get("description"))
    chebi_id = _normalize_xml_value(compound.get("chebi-id"))
    drugbank_id = _normalize_xml_value(compound.get("drugbank-id"))
    kegg_id = _normalize_xml_value(compound.get("kegg-id"))
    synonyms_str = _normalize_xml_value(compound.get("synonyms"))
    
    # Create PathBank compound node
    pwc_curie = f"PathBank:Compound:{pwc_id}"
    compound_node = SmallMolecule(
        id=pwc_curie,
        name=name,
        description=description,
        synonym=[s.strip() for s in synonyms_str.split(";")] if synonyms_str and isinstance(synonyms_str, str) else None,
    )
    nodes.append(compound_node)
    
    # Build translator dictionary: {equiv_id: prefix}
    # equiv_id is just the ID part (not full CURIE), prefix is used to construct full CURIE
    compound_translator = {pwc_curie: "PathBank"}  # Store full CURIE for PathBank compound
    if chebi_id:
        compound_translator[chebi_id] = "CHEBI"  # Store just ID part for external IDs
    if drugbank_id:
        compound_translator[drugbank_id] = "DRUGBANK"
    if kegg_id:
        compound_translator[kegg_id] = "KEGG.COMPOUND"
    
    # Collect all equivalent IDs for same_as edges
    equivalent_ids = [pwc_curie]
    if chebi_id:
        equivalent_ids.append(f"CHEBI:{chebi_id}")
    if drugbank_id:
        equivalent_ids.append(f"DRUGBANK:{drugbank_id}")
    if kegg_id:
        equivalent_ids.append(f"KEGG.COMPOUND:{kegg_id}")
    
    # Create same_as edges between all equivalent IDs
    for i, subject_id in enumerate(equivalent_ids):
        for object_id in equivalent_ids[i+1:]:
            same_as_edge = Association(
                id=entity_id(),
                subject=subject_id,
                predicate="biolink:same_as",
                object=object_id,
                sources=build_association_knowledge_sources(
                    primary=INFORES_PATHBANK,
                    aggregating={"infores:biolink": [INFORES_PATHBANK]},
                ),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
            )
            edges.append(same_as_edge)
    
    # Create has_component edge from pathway to compound
    pathway_curie = f"PathBank:{pathway_id}"
    has_component_edge = Association(
        id=entity_id(),
        subject=pathway_curie,
        predicate="biolink:has_component",
        object=pwc_curie,
        sources=build_association_knowledge_sources(
            primary=INFORES_PATHBANK,
            aggregating={"infores:biolink": [INFORES_PATHBANK]},
        ),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    edges.append(has_component_edge)
    
    return nodes, edges, {pwc_id: compound_translator}


def _create_protein_node_and_edges(protein: dict[str, Any], pathway_id: str) -> tuple[list[NamedThing], list[Association], dict[str, dict[str, str]]]:
    """Create Protein node and same_as edges for a protein.
    
    Returns:
        Tuple of (nodes, edges, translator_dict) where translator_dict maps protein_id to {equiv_id: prefix}
    """
    nodes = []
    edges = []
    
    # Extract protein data
    protein_id_raw = protein.get("id", {})
    if isinstance(protein_id_raw, dict):
        pwp_id = protein_id_raw.get("#text", "")
    else:
        pwp_id = str(protein_id_raw) if protein_id_raw else ""
    
    if not pwp_id:
        return [], [], {}
    
    name = _normalize_xml_value(protein.get("name"))
    uniprot_id = _normalize_xml_value(protein.get("uniprot-id"))
    drugbank_id = _normalize_xml_value(protein.get("drugbank-id"))
    
    # Create PathBank protein node
    pwp_curie = f"PathBank:Protein:{pwp_id}"
    protein_node = Protein(
        id=pwp_curie,
        name=name,
    )
    nodes.append(protein_node)
    
    # Build translator dictionary: {equiv_id: prefix}
    protein_translator = {pwp_curie: "PathBank"}
    if uniprot_id:
        protein_translator[uniprot_id] = "UniProtKB"
    if drugbank_id:
        protein_translator[drugbank_id] = "DRUGBANK"
    
    # Collect all equivalent IDs for same_as edges
    equivalent_ids = [pwp_curie]
    if uniprot_id:
        equivalent_ids.append(f"UniProtKB:{uniprot_id}")
    if drugbank_id:
        equivalent_ids.append(f"DRUGBANK:{drugbank_id}")
    
    # Create same_as edges between all equivalent IDs
    for i, subject_id in enumerate(equivalent_ids):
        for object_id in equivalent_ids[i+1:]:
            same_as_edge = Association(
                id=entity_id(),
                subject=subject_id,
                predicate="biolink:same_as",
                object=object_id,
                sources=build_association_knowledge_sources(
                    primary=INFORES_PATHBANK,
                    aggregating={"infores:biolink": [INFORES_PATHBANK]},
                ),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
            )
            edges.append(same_as_edge)
    
    # Create has_component edge from pathway to protein
    pathway_curie = f"PathBank:{pathway_id}"
    has_component_edge = Association(
        id=entity_id(),
        subject=pathway_curie,
        predicate="biolink:has_component",
        object=pwp_curie,
        sources=build_association_knowledge_sources(
            primary=INFORES_PATHBANK,
            aggregating={"infores:biolink": [INFORES_PATHBANK]},
        ),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    edges.append(has_component_edge)
    
    return nodes, edges, {pwp_id: protein_translator}


def _create_protein_complex_node_and_edges(protein_complex: dict[str, Any], pathway_id: str, protein_translator: dict[str, dict[str, str]]) -> tuple[list[NamedThing], list[Association], dict[str, dict[str, str]]]:
    """Create MacromolecularComplex node and edges for a protein complex.
    
    Args:
        protein_complex: The protein complex data from PWML
        pathway_id: The pathway ID this complex belongs to
        protein_translator: Dictionary mapping protein PathBank IDs to their equivalent IDs (UniProt, DrugBank)
    """
    nodes = []
    edges = []
    
    # Extract protein complex data
    complex_id_raw = protein_complex.get("id", {})
    if isinstance(complex_id_raw, dict):
        pwp_id = complex_id_raw.get("#text", "")
    else:
        pwp_id = str(complex_id_raw) if complex_id_raw else ""
    
    if not pwp_id:
        return [], [], {}
    
    name = _normalize_xml_value(protein_complex.get("name"))
    
    # Create PathBank protein complex node
    pwp_curie = f"PathBank:ProteinComplex:{pwp_id}"
    complex_node = MacromolecularComplex(
        id=pwp_curie,
        name=name,
    )
    nodes.append(complex_node)
    
    # Build translator dictionary for the complex (just itself)
    complex_translator = {pwp_curie: "PathBank"}
    
    # Extract proteins in the complex
    complex_proteins = []
    if "protein_complex-proteins" in protein_complex:
        proteins_data = protein_complex["protein_complex-proteins"]
        protein_list = proteins_data.get("protein-complex-protein", [])
        if not isinstance(protein_list, list):
            protein_list = [protein_list]
        
        for protein_ref in protein_list:
            protein_id_raw = protein_ref.get("protein-id", {})
            if isinstance(protein_id_raw, dict):
                protein_pw_id = protein_id_raw.get("#text", "")
            else:
                protein_pw_id = str(protein_id_raw) if protein_id_raw else ""
            
            if protein_pw_id and protein_pw_id in protein_translator:
                # Get all equivalent IDs for this protein
                for equiv_id, prefix in protein_translator[protein_pw_id].items():
                    # If equiv_id already contains a colon, it's a full CURIE; otherwise construct it
                    if ":" in equiv_id:
                        complex_proteins.append(equiv_id)
                    else:
                        complex_proteins.append(f"{prefix}:{equiv_id}")
    
    # Create has_protein_in_complex edges
    for protein_id in complex_proteins:
        has_protein_edge = Association(
            id=entity_id(),
            subject=pwp_curie,
            predicate="biolink:has_part",
            object=protein_id,
            sources=build_association_knowledge_sources(
                primary=INFORES_PATHBANK,
                aggregating={"infores:biolink": [INFORES_PATHBANK]},
            ),
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        )
        edges.append(has_protein_edge)
    
    # Create has_component edge from pathway to complex
    pathway_curie = f"PathBank:{pathway_id}"
    has_component_edge = Association(
        id=entity_id(),
        subject=pathway_curie,
        predicate="biolink:has_component",
        object=pwp_curie,
        sources=build_association_knowledge_sources(
            primary=INFORES_PATHBANK,
            aggregating={"infores:biolink": [INFORES_PATHBANK]},
        ),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    edges.append(has_component_edge)
    
    return nodes, edges, {pwp_id: complex_translator}


def _create_nucleic_acid_node_and_edges(nucl_acid: dict[str, Any], pathway_id: str) -> tuple[list[NamedThing], list[Association], dict[str, dict[str, str]]]:
    """Create NucleicAcidEntity node and same_as edges for a nucleic acid."""
    nodes = []
    edges = []
    
    # Extract nucleic acid data
    na_id_raw = nucl_acid.get("id", {})
    if isinstance(na_id_raw, dict):
        pwna_id = na_id_raw.get("#text", "")
    else:
        pwna_id = str(na_id_raw) if na_id_raw else ""
    
    if not pwna_id:
        return [], [], {}
    
    name = _normalize_xml_value(nucl_acid.get("name"))
    chebi_id = _normalize_xml_value(nucl_acid.get("chebi-id"))
    
    # Create PathBank nucleic acid node
    pwna_curie = f"PathBank:NucleicAcid:{pwna_id}"
    na_node = NucleicAcidEntity(
        id=pwna_curie,
        name=name,
    )
    nodes.append(na_node)
    
    # Build translator dictionary: {equiv_id: prefix}
    na_translator = {pwna_curie: "PathBank"}
    if chebi_id:
        na_translator[chebi_id] = "CHEBI"
    
    # Collect all equivalent IDs for same_as edges
    equivalent_ids = [pwna_curie]
    if chebi_id:
        equivalent_ids.append(f"CHEBI:{chebi_id}")
    
    # Create same_as edges between all equivalent IDs
    for i, subject_id in enumerate(equivalent_ids):
        for object_id in equivalent_ids[i+1:]:
            same_as_edge = Association(
                id=entity_id(),
                subject=subject_id,
                predicate="biolink:same_as",
                object=object_id,
                sources=build_association_knowledge_sources(
                    primary=INFORES_PATHBANK,
                    aggregating={"infores:biolink": [INFORES_PATHBANK]},
                ),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
            )
            edges.append(same_as_edge)
    
    # Create has_component edge from pathway to nucleic acid
    pathway_curie = f"PathBank:{pathway_id}"
    has_component_edge = Association(
        id=entity_id(),
        subject=pathway_curie,
        predicate="biolink:has_component",
        object=pwna_curie,
        sources=build_association_knowledge_sources(
            primary=INFORES_PATHBANK,
            aggregating={"infores:biolink": [INFORES_PATHBANK]},
        ),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    edges.append(has_component_edge)
    
    return nodes, edges, {pwna_id: na_translator}


def _create_reaction_node_and_edges(reaction: dict[str, Any], pathway_id: str, data_translator: dict[str, dict[str, dict[str, str]]]) -> tuple[list[NamedThing], list[Association]]:
    """Create MolecularActivity node and edges for a reaction.
    
    Args:
        reaction: The reaction data from PWML
        pathway_id: The pathway ID this reaction belongs to
        data_translator: Dictionary mapping element types to their IDs to equivalent IDs
            Format: {"Compound": {compound_id: {equiv_id: prefix}}, "Protein": {...}, ...}
    """
    nodes = []
    edges = []
    
    # Extract reaction data
    reaction_id_raw = reaction.get("id", {})
    if isinstance(reaction_id_raw, dict):
        pwr_id = reaction_id_raw.get("#text", "")
    else:
        pwr_id = str(reaction_id_raw) if reaction_id_raw else ""
    
    if not pwr_id:
        return [], []
    
    # Create PathBank reaction node
    pwr_curie = f"PathBank:Reaction:{pwr_id}"
    reaction_node = MolecularActivity(
        id=pwr_curie,
    )
    nodes.append(reaction_node)
    
    # Extract left elements (reactants)
    left_elements_data = reaction.get("reaction-left-elements", {})
    left_elements = left_elements_data.get("reaction-left-element", []) if isinstance(left_elements_data, dict) else []
    if not isinstance(left_elements, list):
        left_elements = [left_elements] if left_elements else []
    
    # Extract right elements (products)
    right_elements_data = reaction.get("reaction-right-elements", {})
    right_elements = right_elements_data.get("reaction-right-element", []) if isinstance(right_elements_data, dict) else []
    if not isinstance(right_elements, list):
        right_elements = [right_elements] if right_elements else []
    
    # Extract enzymes (protein complexes)
    enzymes_data = reaction.get("reaction-enzymes", {})
    enzymes = enzymes_data.get("reaction-enzyme", []) if isinstance(enzymes_data, dict) else []
    if not isinstance(enzymes, list):
        enzymes = [enzymes] if enzymes else []
    
    # Create edges for left elements (reactants)
    for left_element in left_elements:
        element_id_raw = left_element.get("element-id", {})
        if isinstance(element_id_raw, dict):
            element_id = element_id_raw.get("#text", "")
        else:
            element_id = str(element_id_raw) if element_id_raw else ""
        
        element_type = left_element.get("element-type", "")
        
        if element_id and element_type in data_translator and element_id in data_translator[element_type]:
            # Get all equivalent IDs for this element
            equiv_ids = data_translator[element_type][element_id]
            for equiv_id, prefix in equiv_ids.items():
                # If equiv_id already contains a colon, it's a full CURIE; otherwise construct it
                if ":" in equiv_id:
                    subject_curie = equiv_id
                else:
                    subject_curie = f"{prefix}:{equiv_id}"
                reactant_edge = Association(
                    id=entity_id(),
                    subject=subject_curie,
                    predicate="biolink:participates_in",
                    object=pwr_curie,
                    sources=build_association_knowledge_sources(
                        primary=INFORES_PATHBANK,
                        aggregating={"infores:biolink": [INFORES_PATHBANK]},
                    ),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )
                edges.append(reactant_edge)
    
    # Create edges for right elements (products)
    for right_element in right_elements:
        element_id_raw = right_element.get("element-id", {})
        if isinstance(element_id_raw, dict):
            element_id = element_id_raw.get("#text", "")
        else:
            element_id = str(element_id_raw) if element_id_raw else ""
        
        element_type = right_element.get("element-type", "")
        
        if element_id and element_type in data_translator and element_id in data_translator[element_type]:
            # Get all equivalent IDs for this element
            equiv_ids = data_translator[element_type][element_id]
            for equiv_id, prefix in equiv_ids.items():
                # If equiv_id already contains a colon, it's a full CURIE; otherwise construct it
                if ":" in equiv_id:
                    object_curie = equiv_id
                else:
                    object_curie = f"{prefix}:{equiv_id}"
                product_edge = Association(
                    id=entity_id(),
                    subject=pwr_curie,
                    predicate="biolink:has_output",
                    object=object_curie,
                    sources=build_association_knowledge_sources(
                        primary=INFORES_PATHBANK,
                        aggregating={"infores:biolink": [INFORES_PATHBANK]},
                    ),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )
                edges.append(product_edge)
    
    # Create edges for enzymes (protein complexes)
    for enzyme in enzymes:
        enzyme_id_raw = enzyme.get("protein-complex-id", {})
        if isinstance(enzyme_id_raw, dict):
            enzyme_id = enzyme_id_raw.get("#text", "")
        else:
            enzyme_id = str(enzyme_id_raw) if enzyme_id_raw else ""
        
        if enzyme_id and "ProteinComplex" in data_translator and enzyme_id in data_translator["ProteinComplex"]:
            # Get all equivalent IDs for this protein complex
            equiv_ids = data_translator["ProteinComplex"][enzyme_id]
            for equiv_id, prefix in equiv_ids.items():
                # If equiv_id already contains a colon, it's a full CURIE; otherwise construct it
                if ":" in equiv_id:
                    subject_curie = equiv_id
                else:
                    subject_curie = f"{prefix}:{equiv_id}"
                enzyme_edge = Association(
                    id=entity_id(),
                    subject=subject_curie,
                    predicate="biolink:catalyzes",
                    object=pwr_curie,
                    sources=build_association_knowledge_sources(
                        primary=INFORES_PATHBANK,
                        aggregating={"infores:biolink": [INFORES_PATHBANK]},
                    ),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )
                edges.append(enzyme_edge)
    
    # Create has_component edge from pathway to reaction
    pathway_curie = f"PathBank:{pathway_id}"
    has_component_edge = Association(
        id=entity_id(),
        subject=pathway_curie,
        predicate="biolink:has_component",
        object=pwr_curie,
        sources=build_association_knowledge_sources(
            primary=INFORES_PATHBANK,
            aggregating={"infores:biolink": [INFORES_PATHBANK]},
        ),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    edges.append(has_component_edge)
    
    return nodes, edges


def _create_bound_node_and_edges(bound: dict[str, Any], pathway_id: str, data_translator: dict[str, dict[str, dict[str, str]]]) -> tuple[list[NamedThing], list[Association], dict[str, dict[str, str]]]:
    """Create ChemicalEntity node and edges for a bound."""
    nodes = []
    edges = []
    
    # Extract bound data
    bound_id_raw = bound.get("id", {})
    if isinstance(bound_id_raw, dict):
        pwb_id = bound_id_raw.get("#text", "")
    else:
        pwb_id = str(bound_id_raw) if bound_id_raw else ""
    
    if not pwb_id:
        return [], [], {}
    
    # Create PathBank bound node
    pwb_curie = f"PathBank:Bound:{pwb_id}"
    bound_node = ChemicalEntity(
        id=pwb_curie,
    )
    nodes.append(bound_node)
    
    # Build translator dictionary for the bound (just itself)
    bound_translator = {pwb_curie: "PathBank"}
    
    # Extract elements in the bound
    bound_elements_data = bound.get("bound-elements", {})
    bound_elements = bound_elements_data.get("bound-element", []) if isinstance(bound_elements_data, dict) else []
    if not isinstance(bound_elements, list):
        bound_elements = [bound_elements] if bound_elements else []
    
    # Create has_part edges from bound to elements
    for element in bound_elements:
        element_id_raw = element.get("element-id", {})
        if isinstance(element_id_raw, dict):
            element_id = element_id_raw.get("#text", "")
        else:
            element_id = str(element_id_raw) if element_id_raw else ""
        
        element_type = element.get("element-type", "")
        
        if element_id and element_type in data_translator and element_id in data_translator[element_type]:
            # Get all equivalent IDs for this element
            equiv_ids = data_translator[element_type][element_id]
            for equiv_id, prefix in equiv_ids.items():
                # If equiv_id already contains a colon, it's a full CURIE; otherwise construct it
                if ":" in equiv_id:
                    object_curie = equiv_id
                else:
                    object_curie = f"{prefix}:{equiv_id}"
                has_part_edge = Association(
                    id=entity_id(),
                    subject=pwb_curie,
                    predicate="biolink:has_part",
                    object=object_curie,
                    sources=build_association_knowledge_sources(
                        primary=INFORES_PATHBANK,
                        aggregating={"infores:biolink": [INFORES_PATHBANK]},
                    ),
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )
                edges.append(has_part_edge)
    
    # Create has_component edge from pathway to bound
    pathway_curie = f"PathBank:{pathway_id}"
    has_component_edge = Association(
        id=entity_id(),
        subject=pathway_curie,
        predicate="biolink:has_component",
        object=pwb_curie,
        sources=build_association_knowledge_sources(
            primary=INFORES_PATHBANK,
            aggregating={"infores:biolink": [INFORES_PATHBANK]},
        ),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    edges.append(has_component_edge)
    
    return nodes, edges, {pwb_id: bound_translator}


def _create_element_collection_node_and_edges(ec: dict[str, Any], pathway_id: str) -> tuple[list[NamedThing], list[Association], dict[str, dict[str, str]]]:
    """Create ChemicalEntity node and same_as edges for an element collection."""
    nodes = []
    edges = []
    
    # Extract element collection data
    ec_id_raw = ec.get("id", {})
    if isinstance(ec_id_raw, dict):
        pwec_id = ec_id_raw.get("#text", "")
    else:
        pwec_id = str(ec_id_raw) if ec_id_raw else ""
    
    if not pwec_id:
        return [], [], {}
    
    name = _normalize_xml_value(ec.get("name"))
    external_id_type = ec.get("external-id-type", "")
    external_id = _normalize_xml_value(ec.get("external-id"))
    
    # Create PathBank element collection node
    pwec_curie = f"PathBank:ElementCollection:{pwec_id}"
    ec_node = ChemicalEntity(
        id=pwec_curie,
        name=name,
    )
    nodes.append(ec_node)
    
    # Build translator dictionary: {equiv_id: prefix}
    ec_translator = {pwec_curie: "PathBank"}
    
    # Map external ID types to prefixes
    external_id_mapping = {
        "KEGG Compound": "KEGG.COMPOUND",
        "ChEBI": "CHEBI",
        "UniProt": "UniProtKB",
    }
    
    if external_id_type in external_id_mapping and external_id:
        prefix = external_id_mapping[external_id_type]
        ec_translator[external_id] = prefix
    
    # Collect all equivalent IDs for same_as edges
    equivalent_ids = [pwec_curie]
    if external_id_type in external_id_mapping and external_id:
        prefix = external_id_mapping[external_id_type]
        equivalent_ids.append(f"{prefix}:{external_id}")
    
    # Create same_as edges between all equivalent IDs
    for i, subject_id in enumerate(equivalent_ids):
        for object_id in equivalent_ids[i+1:]:
            same_as_edge = Association(
                id=entity_id(),
                subject=subject_id,
                predicate="biolink:same_as",
                object=object_id,
                sources=build_association_knowledge_sources(
                    primary=INFORES_PATHBANK,
                    aggregating={"infores:biolink": [INFORES_PATHBANK]},
                ),
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
            )
            edges.append(same_as_edge)
    
    # Create has_component edge from pathway to element collection
    pathway_curie = f"PathBank:{pathway_id}"
    has_component_edge = Association(
        id=entity_id(),
        subject=pathway_curie,
        predicate="biolink:has_component",
        object=pwec_curie,
        sources=build_association_knowledge_sources(
            primary=INFORES_PATHBANK,
            aggregating={"infores:biolink": [INFORES_PATHBANK]},
        ),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    edges.append(has_component_edge)
    
    return nodes, edges, {pwec_id: ec_translator}


@koza.transform(tag="pwml")
def transform_pwml(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    """Transform PWML data into Biolink nodes and edges."""
    record_count = 0
    for record in data:
        # Handle empty record (when no PWML files found)
        if not record:
            # Yield empty KnowledgeGraph to satisfy Koza's requirement for at least one result
            yield KnowledgeGraph(nodes=[], edges=[])
            return
        
        pathway_id = record.get("pathway_id", "")
        if not pathway_id:
            continue
        
        nodes = []
        edges = []
        
        # Build data_translator: maps element types to their IDs to equivalent IDs
        # Format: {"Compound": {compound_id: {equiv_id: prefix}}, ...}
        data_translator: dict[str, dict[str, dict[str, str]]] = {
            "Compound": {},
            "Protein": {},
            "NucleicAcid": {},
            "ProteinComplex": {},
            "ElementCollection": {},
            "Bound": {},
        }
        
        # Process compounds first (to build compound_translator)
        compounds = record.get("compounds", [])
        if isinstance(compounds, list):
            for compound in compounds:
                compound_nodes, compound_edges, compound_translator = _create_compound_node_and_edges(compound, pathway_id)
                nodes.extend(compound_nodes)
                edges.extend(compound_edges)
                # Merge translator into data_translator
                for compound_id, equiv_dict in compound_translator.items():
                    data_translator["Compound"][compound_id] = equiv_dict
        
        # Process proteins (to build protein_translator)
        proteins = record.get("proteins", [])
        protein_translator: dict[str, dict[str, str]] = {}
        if isinstance(proteins, list):
            for protein in proteins:
                protein_nodes, protein_edges, protein_translator_item = _create_protein_node_and_edges(protein, pathway_id)
                nodes.extend(protein_nodes)
                edges.extend(protein_edges)
                # Merge translator into data_translator and protein_translator
                for protein_id, equiv_dict in protein_translator_item.items():
                    data_translator["Protein"][protein_id] = equiv_dict
                    protein_translator[protein_id] = equiv_dict
        
        # Process nucleic acids (to build na_translator)
        nucleic_acids = record.get("nucleic-acids", [])
        if isinstance(nucleic_acids, list):
            for nucl_acid in nucleic_acids:
                na_nodes, na_edges, na_translator = _create_nucleic_acid_node_and_edges(nucl_acid, pathway_id)
                nodes.extend(na_nodes)
                edges.extend(na_edges)
                # Merge translator into data_translator
                for na_id, equiv_dict in na_translator.items():
                    data_translator["NucleicAcid"][na_id] = equiv_dict
        
        # Process protein complexes (needs protein_translator, builds complex_translator)
        protein_complexes = record.get("protein-complexes", [])
        if isinstance(protein_complexes, list):
            for protein_complex in protein_complexes:
                complex_nodes, complex_edges, complex_translator = _create_protein_complex_node_and_edges(protein_complex, pathway_id, protein_translator)
                nodes.extend(complex_nodes)
                edges.extend(complex_edges)
                # Merge translator into data_translator
                for complex_id, equiv_dict in complex_translator.items():
                    data_translator["ProteinComplex"][complex_id] = equiv_dict
        
        # Process element collections (to build ec_translator)
        element_collections = record.get("element-collections", [])
        if isinstance(element_collections, list):
            for ec in element_collections:
                ec_nodes, ec_edges, ec_translator = _create_element_collection_node_and_edges(ec, pathway_id)
                nodes.extend(ec_nodes)
                edges.extend(ec_edges)
                # Merge translator into data_translator
                for ec_id, equiv_dict in ec_translator.items():
                    data_translator["ElementCollection"][ec_id] = equiv_dict
        
        # Process bounds (needs data_translator, builds bound_translator)
        bounds = record.get("bounds", [])
        if isinstance(bounds, list):
            for bound in bounds:
                bound_nodes, bound_edges, bound_translator = _create_bound_node_and_edges(bound, pathway_id, data_translator)
                nodes.extend(bound_nodes)
                edges.extend(bound_edges)
                # Merge translator into data_translator
                for bound_id, equiv_dict in bound_translator.items():
                    data_translator["Bound"][bound_id] = equiv_dict
        
        # Process reactions (needs data_translator)
        reactions = record.get("reactions", [])
        if isinstance(reactions, list):
            for reaction in reactions:
                reaction_nodes, reaction_edges = _create_reaction_node_and_edges(reaction, pathway_id, data_translator)
                nodes.extend(reaction_nodes)
                edges.extend(reaction_edges)
        
        # Log progress for first few records
        if record_count < 3:
            compounds_count = len(compounds) if isinstance(compounds, list) else 0
            proteins_count = len(proteins) if isinstance(proteins, list) else 0
            complexes_count = len(protein_complexes) if isinstance(protein_complexes, list) else 0
            reactions_count = len(reactions) if isinstance(reactions, list) else 0
            koza.log(f"PWML record {record_count + 1} for pathway {pathway_id}: {compounds_count} compounds, {proteins_count} proteins, {complexes_count} complexes, {reactions_count} reactions")
        
        record_count += 1
        
        # Yield KnowledgeGraph if we have nodes or edges
        if nodes or edges:
            yield KnowledgeGraph(nodes=nodes, edges=edges)
    
    koza.log(f"Processed {record_count} PWML records in transform function")

@koza.transform_record(tag="pathways")
def transform_record(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    # Extract data from CSV columns
    pathway_id = record.get("SMPDB ID") or record.get("PW ID")
    name = record.get("Name")
    description = record.get("Description")
    subject = record.get("Subject")

    # Track missing fields
    koza.state["total_records"] += 1
    if not pathway_id:
        koza.state["missing_fields"]["pathway_id"] += 1
    if not name:
        koza.state["missing_fields"]["name"] += 1
    if not description:
        koza.state["missing_fields"]["description"] += 1
    if not subject:
        koza.state["missing_fields"]["subject"] += 1

    # Only require pathway_id, others are optional
    if not pathway_id:
        return None

    # Create Biolink Pathway node
    node_id = f"PathBank:{pathway_id}"
    pathway = Pathway(id=node_id, name=name, description=description)

    return KnowledgeGraph(nodes=[pathway], edges=[])

@koza.on_data_end(tag="pathways")
def on_data_end(koza: koza.KozaTransform):
    # Log final statistics
    koza.log(f"Processed {koza.state['total_records']} total records")
    koza.log(f"Missing pathway_id: {koza.state['missing_fields']['pathway_id']}")
    koza.log(f"Missing name: {koza.state['missing_fields']['name']}")
    koza.log(f"Missing description: {koza.state['missing_fields']['description']}")
    koza.log(f"Missing subject: {koza.state['missing_fields']['subject']}")

def _normalize_to_list(value: Any) -> list:
    # xmltodict returns a dict for single elements, list for multiple
    # Normalize to always return a list
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return [value]
    return []


def _normalize_xml_value(value: Any) -> str | None:
    """Normalize XML values, handling nil/null values from xmltodict.
    
    xmltodict can represent nil/null values as {'@nil': 'true'} dictionaries.
    This function converts those to None, and ensures strings are returned.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        # Check if it's a nil value
        if value.get("@nil") == "true" or value.get("@nil") == True:
            return None
        # If it's a dict with #text, extract the text
        if "#text" in value:
            return value["#text"]
        # Otherwise, return None for unexpected dicts
        return None
    if isinstance(value, str):
        return value if value else None
    # Convert other types to string
    return str(value) if value else None

@koza.on_data_begin(tag="pwml")
def on_data_begin_pwml(koza: koza.KozaTransform) -> None:
    # Initialize counters for PWML processing
    koza.state["pwml_files_processed"] = 0
    koza.state["pwml_files_failed"] = 0
    koza.state["pwml_start_time"] = time.time()
    koza.state["pwml_last_log_time"] = time.time()

@koza.prepare_data(tag="pwml")
def prepare_pwml_data(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    # Parse PWML XML files and extract structured data
    # We discover files directly since Koza doesn't support glob patterns
    
    source_data_dir = Path(koza.input_files_dir)
    pwml_dir = source_data_dir / "pathbank_all_pwml"
    
    # Extract pathbank_all_pwml.zip if PWML directory doesn't exist or is empty
    pwml_zip = source_data_dir / "pathbank_all_pwml.zip"
    
    # If zip doesn't exist in current directory, check parent directory for other versions
    if not pwml_zip.exists():
        # Go up to data/pathbank level to search for v*/source_data directories
        pathbank_base_dir = source_data_dir.parent.parent
        # Check common version directories (v1, v2, etc.)
        for version_dir in pathbank_base_dir.glob("v*/source_data"):
            potential_zip = version_dir / "pathbank_all_pwml.zip"
            if potential_zip.exists():
                koza.log(f"Found PWML zip file in {version_dir}, copying to current directory...")
                # Ensure target directory exists
                pwml_zip.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(potential_zip, pwml_zip)
                break
    
    if pwml_zip.exists() and (not pwml_dir.exists() or not any(pwml_dir.glob("*.pwml"))):
        koza.log(f"Extracting {pwml_zip.name}...")
        with zipfile.ZipFile(pwml_zip, "r") as zip_ref:
            zip_ref.extractall(source_data_dir)
        koza.log(f"Extracted PWML files to {pwml_dir}")
    
    # Find all PWML files in the directory directly (ignore Koza's data iterator)
    pwml_files = list(pwml_dir.glob("*.pwml")) if pwml_dir.exists() else []
    total_files = len(pwml_files)
    koza.log(f"Found {total_files} PWML files to process")
    
    # If no files found, yield an empty dict to satisfy Koza's requirement for at least one result
    # The transform function will handle this gracefully
    if total_files == 0:
        koza.log("No PWML files found, skipping PWML processing", level="WARNING")
        yield {}  # Yield empty dict so transform function is called and can handle gracefully
        return
    
    # Process each PWML file
    for file_index, pwml_file in enumerate(pwml_files, start=1):
        try:
            with open(pwml_file, "rb") as f:
                pw = xmltodict.parse(f.read())
            
            # Extract pathway ID from the file
            pathway_id = None
            if "super-pathway-visualization" in pw:
                pathway_id = pw["super-pathway-visualization"].get("pw-id")
            
            # If no pathway ID in XML, extract from filename (e.g., PW000001.pwml -> PW000001)
            if not pathway_id:
                pathway_id = pwml_file.stem
            
            # Extract pathway visualization contexts
            if "super-pathway-visualization" not in pw:
                koza.log(f"Skipping {pwml_file.name}: missing super-pathway-visualization", level="WARNING")
                koza.state["pwml_files_failed"] += 1
                continue
            
            sv = pw["super-pathway-visualization"]
            
            # Handle both single context (dict) and multiple contexts (list)
            contexts = []
            if "pathway-visualization-contexts" in sv:
                pvc = sv["pathway-visualization-contexts"]
                if "pathway-visualization-context" in pvc:
                    pvc_item = pvc["pathway-visualization-context"]
                    if isinstance(pvc_item, list):
                        # Multiple contexts
                        contexts = [item["pathway-visualization"] for item in pvc_item if "pathway-visualization" in item]
                    elif isinstance(pvc_item, dict):
                        # Single context
                        if "pathway-visualization" in pvc_item:
                            contexts = [pvc_item["pathway-visualization"]]
            
            # If no contexts found, skip this file
            if not contexts:
                koza.log(f"Skipping {pwml_file.name}: no pathway-visualization-context found", level="WARNING")
                koza.state["pwml_files_failed"] += 1
                continue
            
            # Process each context (most files have one, but some have multiple)
            for context in contexts:
                # Extract pathway information from context
                pathway_data = context.get("pathway", {}) if isinstance(context.get("pathway"), dict) else {}
                
                # Extract compounds, proteins, etc. - normalize to lists (xmltodict returns dict for single, list for multiple)
                compounds_data = context.get("compounds", {})
                proteins_data = context.get("proteins", {})
                protein_complexes_data = context.get("protein-complexes", {})
                nucleic_acids_data = context.get("nucleic-acids", {})
                reactions_data = context.get("reactions", {})
                bounds_data = context.get("bounds", {})
                element_collections_data = context.get("element-collections", {})
                references_data = pathway_data.get("references", {})
                
                # Yield structured data for this pathway context
                yield {
                    "pathway_id": pathway_id,
                    "pathway_data": pathway_data,
                    "compounds": _normalize_to_list(compounds_data.get("compound") if isinstance(compounds_data, dict) else None),
                    "proteins": _normalize_to_list(proteins_data.get("protein") if isinstance(proteins_data, dict) else None),
                    "protein-complexes": _normalize_to_list(protein_complexes_data.get("protein-complex") if isinstance(protein_complexes_data, dict) else None),
                    "nucleic-acids": _normalize_to_list(nucleic_acids_data.get("nucleic-acid") if isinstance(nucleic_acids_data, dict) else None),
                    "reactions": _normalize_to_list(reactions_data.get("reaction") if isinstance(reactions_data, dict) else None),
                    "bounds": _normalize_to_list(bounds_data.get("bound") if isinstance(bounds_data, dict) else None),
                    "element-collections": _normalize_to_list(element_collections_data.get("element-collection") if isinstance(element_collections_data, dict) else None),
                    "references": _normalize_to_list(references_data.get("reference") if isinstance(references_data, dict) else None),
                    "_file_path": str(pwml_file),
                }
            
            koza.state["pwml_files_processed"] += 1
            
            # Log progress every 1000 files or every 30 seconds
            current_time = time.time()
            should_log = (
                file_index % 1000 == 0 or  # Every 1000 files
                current_time - koza.state["pwml_last_log_time"] >= 30  # Every 30 seconds
            )
            
            if should_log:
                processed = koza.state["pwml_files_processed"]
                failed = koza.state["pwml_files_failed"]
                elapsed = current_time - koza.state["pwml_start_time"]
                percent = (file_index / total_files) * 100
                
                if processed > 0:
                    rate = processed / elapsed  # files per second
                    remaining = (total_files - file_index) / rate if rate > 0 else 0
                    koza.log(
                        f"Progress: {file_index}/{total_files} files ({percent:.1f}%) | "
                        f"Processed: {processed} | Failed: {failed} | "
                        f"Rate: {rate:.1f} files/sec | ETA: {remaining:.0f}s"
                    )
                else:
                    koza.log(
                        f"Progress: {file_index}/{total_files} files ({percent:.1f}%) | "
                        f"Processed: {processed} | Failed: {failed}"
                    )
                
                koza.state["pwml_last_log_time"] = current_time
            
        except Exception as e:
            koza.log(f"Error processing PWML file {pwml_file.name}: {e}", level="WARNING")
            koza.state["pwml_files_failed"] += 1
            continue

@koza.on_data_end(tag="pwml")
def on_data_end_pwml(koza: koza.KozaTransform):
    # Log final statistics
    koza.log(f"Processed {koza.state.get('pwml_files_processed', 0)} PWML files successfully")
    koza.log(f"Failed to process {koza.state.get('pwml_files_failed', 0)} PWML files")