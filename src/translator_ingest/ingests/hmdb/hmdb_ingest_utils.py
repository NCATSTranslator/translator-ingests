"""
HMDB ingest utils adapted from the Orion code base.
"""
from typing import Iterable
import koza

import xml.etree.cElementTree as E_Tree

#
# from orion.kgxmodel import kgxnode, kgxedge
# class kgxnode:
#     def __init__(self,
#                  identifier,
#                  name='',
#                  categories=None,
#                  nodeprops=None):
#         self.identifier = identifier
#         self.name = name
#         self.categories = categories if categories else [NAMED_THING]
#         self.properties = nodeprops if nodeprops else {}
#
# class kgxedge:
#     def __init__(self,
#                  subject_id,
#                  object_id,
#                  predicate=None,
#                  primary_knowledge_source=None,
#                  aggregator_knowledge_sources: list = None,
#                  edgeprops=None):
#         self.subjectid = subject_id
#         self.objectid = object_id
#         self.predicate = predicate
#         self.primary_knowledge_source = primary_knowledge_source
#         self.aggregator_knowledge_sources = aggregator_knowledge_sources
#         if edgeprops:
#             self.properties = edgeprops
#         else:
#             self.properties = {}

def read_xml_file(
        koza_transform: koza.KozaTransform,
        fp,
        element
) -> Iterable[str]:
    """
    Read the xml file and capture the metabolite elements.

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


def get_genes(koza_transform, el, metabolite_id) -> bool:
    """
    This method creates the gene nodes and gene-to-metabolite edges.

    Note that there are 2 potential edge directions (legacy records shown):
         It is unknown (to me) why these would have different provided_by's as the subject/object types are the same.
      - metabolite to enzyme
        "provided_by": "hmdb.metabolite_to_enzyme",
        "subject": "CHEBI:16040", (chemical compound, ie. metabolite)
        "object": "NCBIGene:29974", (protein, ie. gene)
        "predicate": "RO:0002434",
        "publications": []

      - enzyme to metabolite
        "provided_by": "hmdb.enzyme_to_metabolite",
        "subject": "CHEBI:84764", (chemical compound, i.e., metabolite)
        "object": "NCBIGene:53947", (protein, i.e., gene)
        "predicate": "RO:0002434",
        "publications": []

    :param el: the root of this xml fragment
    :param metabolite_id: the metabolite id
    :return: found flag
    """
    # init the return
    ret_val: bool = False

    # get all the proteins
    proteins: list = el.find('protein_associations').findall('protein')

    # did we get any records?
    if len(proteins) > 0:
        # for all the proteins listed
        for p in proteins:
            # get the protein id (gene)
            protein: E_Tree.Element = p.find('uniprot_id')

            # did we get a value
            if protein is not None and protein.text is not None:
                # get the type of protein (gene type)
                protein_type: E_Tree.Element = p.find('protein_type')

                # was the protein type found
                if protein_type is not None and protein_type.text is not None:
                    # we got at least something
                    ret_val = True

                    # create the gene id
                    protein_id = UNIPROTKB + ':' + protein.text

                    # what type of protein is this
                    if protein_type.text.startswith('Enzyme'):
                        #Enzymes affect the rate of reactions that either produce or consume metabolites.
                        # create the edge data
                        subject_id: str = protein_id
                        object_id: str = metabolite_id
                        predicate: str = 'CTD:affects_abundance_of'
                    # else it must be a transport?
                    elif protein_type.text.startswith('Transport'):
                        # create the edge data
                        subject_id: str = protein_id
                        object_id: str = metabolite_id
                        predicate: str = 'CTD:increases_transport_of'
                    else: # this should be a protein type of Unknown
                        # create the edge data
                        subject_id: str = metabolite_id
                        object_id: str = protein_id
                        predicate: str = 'CTD:related_to'

                    # get the name element
                    el_name: E_Tree.Element = p.find('name')

                    # was the name found (optional)
                    if el_name is not None and el_name.text is not None:
                        name: str = el_name.text.encode('ascii',errors='ignore').decode(encoding="utf-8")
                    else:
                        name: str = ''

                    # create a node and add it to the list
                    new_node = kgxnode(protein_id, name=name)
                    self.output_file_writer.write_kgx_node(new_node)

                    edge_props = {KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                                  AGENT_TYPE: MANUAL_AGENT}
                    # create an edge and add it to the list
                    new_edge = kgxedge(subject_id,
                                       object_id,
                                       predicate=predicate,
                                       primary_knowledge_source=self.provenance_id,
                                       edgeprops=edge_props)
                    self.output_file_writer.write_kgx_edge(new_edge)

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

    # return pass or fail
    return ret_val

def get_diseases(koza_transform, el, metabolite_id) -> bool:
    """
    This method creates disease nodes and disease to metabolite edges.

    note: that there are 2 potential edge directions (modified legacy records shown below)
          It is unknown (to me) why these would have different provided_by's as the subject/object types are the same.

     - hmdb.metabolite_to_disease
          "provided_by": "hmdb.metabolite_to_disease",
          "subject": "CHEBI:16742", (chemical compound, ie. the metabolite)
          "object": "UMLS:C4324375", (disease, ie. the OMIM value)
          "predicate": "SEMMEDDB:ASSOCIATED_WITH",
          "publications": []

     - disease_to_hmdb.metabolite
          "provided_by": "hmdb.disease_to_metabolite",
          "subject": "CHEBI:16742", (chemical compound, ie. the metabolite)
          "object": "MONDO:0005335", (disease, ie. the OMIM value)
          "predicate": "SEMMEDDB:ASSOCIATED_WITH",
          "publications": []

    :param el: the root of this xml fragment
    :param metabolite_id: the metabolite id (edge subject)
    :return: found flag
    """
    # init the return
    ret_val: bool = False

    # get all the diseases
    diseases: list = el.find('diseases').findall('disease')

    # did we get any diseases?
    if len(diseases) > 0:
        # for each disease
        for d in diseases:
            # get the omim id
            object_id: E_Tree.Element = d.find('omim_id')

            # did we get a value
            if object_id is not None and object_id.text is not None:

                # get the name
                name: E_Tree.Element = d.find('name')

                # was the name found (optional)
                if name is not None and name.text is not None:
                    name: str = name.text.encode('ascii',errors='ignore').decode(encoding="utf-8")
                else:
                    name: str = ''

                # get all the pubmed ids
                references: list = d.find('references').findall('reference')

                # did we get some good data
                if references is not None and len(references) > 0:
                    # storage for the pubmed ids
                    pmids: list = []

                    # for each reference get the pubmed id
                    for r in references:
                        # get the pubmed id
                        pmid: E_Tree.Element = r.find('pubmed_id')

                        # was it found
                        if pmid is not None and pmid.text is not None:
                            # save it in the list
                            pmids.append('PMID:' + pmid.text)

                    # create the edge property data
                    edge_props = {KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                                  AGENT_TYPE: MANUAL_AGENT}

                    # if we found any pubmed ids add them to the properties (optional)
                    if len(pmids) > 0:
                        edge_props[PUBLICATIONS] = pmids

                    disease_id = f'{OMIM}:{object_id.text}'

                    # create a node and add it to the list
                    new_node = kgxnode(disease_id, name=name)
                    self.output_file_writer.write_kgx_node(new_node)

                    # create an edge and add it to the list
                    new_edge = kgxedge(metabolite_id,
                                       disease_id,
                                       predicate='RO:0002610',
                                       primary_knowledge_source=self.provenance_id,
                                       edgeprops=edge_props)
                    self.output_file_writer.write_kgx_edge(new_edge)
                    ret_val = True
            else:
                koza_transform.log(
                    msg=f'no omim id for {metabolite_id}',
                    level="DEBUG"
                )
    else:
        koza_transform.log(
            msg=f'No diseases for {metabolite_id}',
            level="DEBUG"
        )

    # return pass or fail
    return ret_val

def get_pathways(koza_transform, el, metabolite_id) -> bool:
    """
    This method creates pathway nodes and pathway to metabolite edges.

    Note that there is one edge direction (modified legacy record shown below):
          "provided_by": "hmdb.metabolite_to_pathway",
          "subject": "CHEBI:80603", (chemical compound, ie. the metabolite)
          "object": "SMPDB:SMP0000627", (SMP pathway)
          "predicate": "RO:0000056",
          "publications": []

    :param el: the root of this xml fragment
    :param metabolite_id: the metabolite id (edge subject)
    :return: found flag
    """
    # init the return
    ret_val: bool = False
    
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
                object_id: str = smpdb_to_curie(smpdb_id.text)

                # did we get an id. a valid curie here is 16 characters long (SMPDB:SMP1234567)
                if len(object_id) == 16:

                    # get the name
                    name_el: E_Tree.Element = p.find('name')

                    # did we get a good value (optional)
                    if name_el is not None and name_el.text is not None:
                        name: str = name_el.text.encode('ascii',errors='ignore').decode(encoding="utf-8")
                    else:
                        name: str = ''

                    # # create a node and add it to the list
                    # new_node = kgxnode(object_id, name=name)
                    # self.output_file_writer.write_kgx_node(new_node)
                    #
                    # edge_props = {KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
                    #               AGENT_TYPE: MANUAL_AGENT}
                    #
                    # # create an edge and add it to the list
                    # new_edge = kgxedge(metabolite_id,
                    #                    object_id,
                    #                    predicate='RO:0000056',
                    #                    primary_knowledge_source=self.provenance_id,
                    #                    edgeprops=edge_props)
                    # self.output_file_writer.write_kgx_edge(new_edge)
                else:
                    koza_transform.log(
                        msg='invalid smpdb for {metabolite_id}',
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

    # return pass or fail
    return ret_val
