"""
HMDB ingest utils adapted from the Orion HMDB parsing code base.
"""
from typing import Tuple, Iterable, Literal

import koza

import xml.etree.cElementTree as E_Tree

from biolink_model.datamodel.pydanticmodel_v2 import (
    Protein,
    Disease,
    Pathway,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    GeneAffectsChemicalAssociation,
    DiseaseAssociatedWithResponseToChemicalEntityAssociation,
    ChemicalEntityToPathwayAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum
)

from bmt.pydantic import (
    entity_id,
    build_association_knowledge_sources
)

from koza.model.graphs import KnowledgeGraph


def read_xml_file(
        koza_transform: koza.KozaTransform,
        fp,
        element
) -> Iterable[str]:
    """
    Read the XML file and capture the metabolite elements.

    TODO filter out more items that arent used

    """
    # create the target xml fragment search tag
    start_tag: str = f'<{element}>'
    end_tag: str = f'</{element}>'

    # flag to indicate we have identified a new xml fragment
    tag_found: bool = False

    # init the xml text to be captured
    xml_string: str = ''

    # init a record counter
    counter: int = 0

    # for every line in the file
    for line in fp:

        # convert to string and remove the unprintable characters
        line = line.decode('utf-8')

        # xml elements span multiple lines - are we starting a relevant one?
        if start_tag in line:
            tag_found = True
            counter += 1
            if counter % 25000 == 0:
                koza_transform.log(
                    msg=f'Loaded {counter} metabolites...',
                    level="DEBUG"
                )

        # concatenate the relevant lines
        if tag_found:
            xml_string += line

        # did we read the end of the element?
        if end_tag in line:
            # save the element in the list
            yield xml_string

            # reset the start flag
            tag_found = False

            # reset the xml string
            xml_string = ''

    koza_transform.log(
        msg=f'Loaded a total of {counter} metabolites.',
        level="DEBUG"
    )


def smpdb_to_curie(smp_id: str) -> str:
    """
    returns a valid smpdb curie from what is passed in ('SMP00123')

    :param smp_id: the smp id
    :return: the corrected curie
    """
    # init the return
    ret_val: str = ''

    # get the integer part
    smp_numeric: str = smp_id.lstrip('SMP')

    # was there an integer?
    if smp_numeric.isdigit():
        ret_val = 'SMPDB:SMP' + '0' * (7 - len(smp_numeric)) + smp_numeric

    # return to the caller
    return ret_val

G2C_PREDICATE = Literal[
    "biolink:affects",
    "biolink:ameliorates_condition",
    "biolink:disrupts",
    "biolink:exacerbates_condition",
    "biolink:has_adverse_event",
    "biolink:has_side_effect",
    "biolink:regulates"
]

def get_genes(
        koza_transform,
        el,
        metabolite_id
) -> list[Tuple[Protein, GeneAffectsChemicalAssociation]]:
    """
    This method creates the gene nodes and gene-to-metabolite edges.

    Note that there are 2 potential edge directions (legacy records shown):
         It is unknown (to me) why these would have different
         provided_by's, as the subject/object types are the same.

      - Metabolite to enzyme
        "provided_by": "hmdb.metabolite_to_enzyme",
        "subject": "CHEBI:16040", (chemical compound, i.e., metabolite)
        "object": "NCBIGene:29974", (protein, i.e., gene)
        "predicate": "RO:0002434",
        "publications": []

      - Enzyme to metabolite
        "provided_by": "hmdb.enzyme_to_metabolite",
        "subject": "CHEBI:84764", (chemical compound, i.e., metabolite)
        "object": "NCBIGene:53947", (protein, i.e., gene)
        "predicate": "RO:0002434",
        "publications": []

    :param koza_transform: The koza transform ingest context object.
    :param el: The root of this XML fragment
    :param metabolite_id: the metabolite id
    :return: list[Tuple[Protein, GeneAffectsChemicalAssociation]]
    """
    gene_list: list[Tuple[Protein, GeneAffectsChemicalAssociation]] = []

    # get all the proteins
    proteins: list = el.find('protein_associations').findall('protein')

    # did we get any records?
    if len(proteins) > 0:
        # for all the proteins listed
        for p in proteins:
            # get the protein id (gene)
            protein: E_Tree.Element = p.find('uniprot_id')

            if protein is not None and protein.text is not None:
                # get the type of protein (gene type)
                protein_type: E_Tree.Element = p.find('protein_type')

                # was the protein type found?
                if protein_type is not None and protein_type.text is not None:

                    el_name: E_Tree.Element = p.find('name')

                    # was the name found (optional)?
                    if el_name is not None and el_name.text is not None:
                        name: str = el_name.text.encode('ascii',errors='ignore').decode(encoding="utf-8")
                    else:
                        name: str = ''

                    protein_id = f"UniProtKB:{protein.text}"

                    predicate: G2C_PREDICATE

                    # what type of protein is this?
                    if protein_type.text.startswith('Enzyme'):
                        # Enzymes affect the rate of reactions that
                        # either produce or consume metabolites.
                        predicate = "biolink:regulates"  # "CTD:affects_abundance_of"
                        object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.abundance
                    elif protein_type.text.startswith('Transport'):
                        # else it must be a transport protein?
                        predicate = "biolink:regulates"  # "CTD:increases_transport_of"
                        object_aspect_qualifier = GeneOrGeneProductOrChemicalEntityAspectEnum.transport
                    else:
                        # this is a protein type of unknown function
                        predicate = "biolink:affects"
                        object_aspect_qualifier = None

                    protein_node = Protein(id=protein_id, name=name)

                    edge = GeneAffectsChemicalAssociation(
                        id=entity_id(),
                        subject=protein_id,
                        predicate=predicate,
                        object=metabolite_id,
                        object_aspect_qualifier=object_aspect_qualifier,
                        sources=build_association_knowledge_sources(primary="infores:hmdb"),
                        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                        agent_type=AgentTypeEnum.manual_agent
                    )

                    gene_list.append( (protein_node, edge) )
                else:
                    koza_transform.log(
                        msg=f'no protein type for {metabolite_id}',
                        level="DEBUG"
                    )
            else:
                koza_transform.log(
                    msg=f'no proteins for {metabolite_id}',
                    level="DEBUG"
                )
    else:
        koza_transform.log(
            msg=f'No proteins for {metabolite_id}',
            level="DEBUG"
        )

    return gene_list

D2C_PREDICATE = Literal[
    "biolink:associated_with_resistance_to",
    "biolink:associated_with_response_to",
    "biolink:associated_with_sensitivity_to"
]

def get_diseases(
        koza_transform,
        el,
        metabolite_id
) -> list[Tuple[Disease, DiseaseAssociatedWithResponseToChemicalEntityAssociation]]:
    """
    This method creates disease nodes and disease to metabolite edges.

    note: that there are 2 potential edge directions (modified legacy records shown below)
          It is unknown (to me) why these would have different
           provided_by's as the subject/object types are the same.

     - hmdb.metabolite_to_disease
          "provided_by": "hmdb.metabolite_to_disease",
          "subject": "CHEBI:16742", (chemical compound, i.e., the metabolite)
          "object": "UMLS:C4324375", (disease, i.e., the OMIM value)
          "predicate": "SEMMEDDB:ASSOCIATED_WITH",
          "publications": []

     - disease_to_hmdb.metabolite
          "provided_by": "hmdb.disease_to_metabolite",
          "subject": "CHEBI:16742", (chemical compound, i.e., the metabolite)
          "object": "MONDO:0005335", (disease, i.e., the OMIM value)
          "predicate": "SEMMEDDB:ASSOCIATED_WITH",
          "publications": []

    :param koza_transform: The koza transform context
    :param el: the root of this XML fragment
    :param metabolite_id: the metabolite id (edge subject)
    :return: list[Tuple[Disease, DiseaseAssociatedWithResponseToChemicalEntityAssociation]]
    """

    disease_list: list[Tuple[Disease, DiseaseAssociatedWithResponseToChemicalEntityAssociation]] = []

    # get all the diseases
    diseases: list = el.find('diseases').findall('disease')

    # did we get any diseases?
    if len(diseases) > 0:
        # for each disease
        for d in diseases:
            # get the omim id
            object_id: E_Tree.Element = d.find('omim_id')

            # did we get a value?
            if object_id is not None and object_id.text is not None:

                # did we get the name?
                name: E_Tree.Element = d.find('name')

                # was the name found (optional)?
                if name is not None and name.text is not None:
                    name: str = name.text.encode('ascii',errors='ignore').decode(encoding="utf-8")
                else:
                    name: str = ''

                # get all the pubmed ids
                references: list = d.find('references').findall('reference')

                # did we get some good data?
                if references is not None and len(references) > 0:
                    # storage for the pubmed ids
                    pmids: list = []

                    # for each reference get the pubmed id
                    for r in references:
                        pmid: E_Tree.Element = r.find('pubmed_id')
                        if pmid is not None and pmid.text is not None:
                            pmids.append('PMID:' + pmid.text)

                    disease_id = f"OMIM:{object_id.text}"

                    disease_node = Disease(id=disease_id, name=name)

                    edge = DiseaseAssociatedWithResponseToChemicalEntityAssociation(
                        id=entity_id(),
                        subject=disease_id,

                        # replaces original 'RO:0002610' - correlated_with
                        predicate="biolink:associated_with_response_to",

                        object=metabolite_id,
                        publications=pmids if len(pmids) > 0 else None,
                        sources=build_association_knowledge_sources(primary="infores:hmdb"),
                        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                        agent_type=AgentTypeEnum.manual_agent
                    )

                    disease_list.append( (disease_node, edge) )
            else:
                koza_transform.log(
                    msg=f'No OMIM id for {metabolite_id}',
                    level="DEBUG"
                )
    else:
        koza_transform.log(
            msg=f'No diseases for {metabolite_id}',
            level="DEBUG"
        )

    return disease_list


def get_pathways(
        koza_transform,
        el,
        metabolite_id
) -> list[Tuple[Pathway, ChemicalEntityToPathwayAssociation]]:
    """
    This method creates pathway nodes and pathway to metabolite edges.

    Note that there is one edge direction (modified legacy record shown below):
          "provided_by": "hmdb.metabolite_to_pathway",
          "subject": "CHEBI:80603", (chemical compound, i.e., the metabolite)
          "object": "SMPDB:SMP0000627", (SMP pathway)
          "predicate": "RO:0000056",
          "publications": []

    :param koza_transform: The koza transform context
    :param el: the root of this XML fragment
    :param metabolite_id: the metabolite id (edge subject)
    :return: list[Tuple[Pathway, ChemicalEntityToPathwayAssociation]]
    """
    pathway_list: list[Tuple[Pathway, ChemicalEntityToPathwayAssociation]] = []

    # get the pathways
    pathways: list = el.find('biological_properties').find('pathways').findall('pathway')

    # did we find any pathways?
    if len(pathways) > 0:
        # for each pathway
        for p in pathways:
            # get the pathway id
            smpdb_id: E_Tree.Element = p.find('smpdb_id')

            # did we get a good value?
            if id is not None and smpdb_id.text is not None:
                # get the pathway curie ID
                pathway_id: str = smpdb_to_curie(smpdb_id.text)

                # did we get an id. a valid curie here is 16 characters long (SMPDB:SMP1234567)
                if len(pathway_id) == 16:

                    # get the name
                    name_el: E_Tree.Element = p.find('name')

                    # did we get a good value (optional)
                    if name_el is not None and name_el.text is not None:
                        name: str = name_el.text.encode('ascii',errors='ignore').decode(encoding="utf-8")
                    else:
                        name: str = ''

                    pathway_node = Pathway(id=pathway_id, name=name)

                    edge = ChemicalEntityToPathwayAssociation(
                        id=entity_id(),
                        subject=pathway_id,

                        # replaces original 'RO:0000056' - correlated_with
                        predicate="biolink:participates_in",

                        object=metabolite_id,
                        sources=build_association_knowledge_sources(primary="infores:hmdb"),
                        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                        agent_type=AgentTypeEnum.manual_agent
                    )

                    pathway_list.append( (pathway_node, edge) )
                else:
                    koza_transform.log(
                        msg=f"invalid smpdb for {metabolite_id}",
                        level="DEBUG"
                    )
            else:
                koza_transform.log(
                    msg=f'no smpdb for {metabolite_id}',
                    level="DEBUG"
                )
    else:
        koza_transform.log(
            msg=f'No pathways for {metabolite_id}',
            level="DEBUG"
        )

    return pathway_list
