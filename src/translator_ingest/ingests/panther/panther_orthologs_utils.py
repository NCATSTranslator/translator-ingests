"""
Utility functions for Panther Orthology data processing
"""
# RMB: 29-Sept-2025: NCBI gene lookup is not used for now
# import gzip
# from collections import Counter
# NCBI_MAP_FILE_PATH = "./data/panther/gene_info.gz"

# These names should match pantherdb shorthand names for each species
# Example... https://www.pantherdb.org/genomes/genome.jsp?taxonId=9606 --> Short Name: HUMAN
panther_taxon_map = {
    "HUMAN": "9606",
    "MOUSE": "10090",
    "RAT": "10116",

    # The September 2025 implementation of the
    # Translator Phase 3 Panther data ingestion
    # only contains data for the above 3 species.
    #
    # "CANLF": "9615",   # Canis lupus familiaris - domestic dog
    # "BOVIN": "9913",   # Bos taurus - cow
    # "PIG": "9823",     # Sus scrofa - pig
    # "CHICK": "9031",
    # "XENTR": "8364",   # Xenopus tropicalis - tropical clawed frog
    # "DANRE": "7955",
    # "DROME": "7227",
    # "CAEEL": "6239",
    # "DICDI": "44689",
    # "EMENI": "227321",  # Emericella nidulans (strain FGSC A4 etc.) (Aspergillus nidulans)
    # "SCHPO": "4896",
    # "YEAST": "4932"
    # Additional species for future here...
    # "FELCA": "9685",  # Felis catus - domestic cat
}


# RMB: 29-Sept-2025: NCBI gene lookup is not used for now
# # Full list of NCBI columns is:
# relevant_ncbi_cols = [
#     '#tax_id',
#     'GeneID',
#     'Symbol',
#     'LocusTag',
#     'Synonyms',
#     'dbXrefs',
#     'Symbol_from_nomenclature_authority',
#     'Full_name_from_nomenclature_authority',
#     'Other_designations'
# ]

# Entries with Gene/Orthology identifier namespaces that need modifying
# to match our CURIEs.  Keys are the pantherdb namespace, and values
# are the CURIE namespace (Note: many key/value pairs are the same
# for simplicity in downstream processing)
db_to_curie_map = {
    "HGNC":"HGNC",
    "MGI":"MGI",
    "RGD":"RGD",
    "Ensembl": "ENSEMBL",

    # These identifier namespaces for non-target species are ignored for now.
    # We assume that they don't slip by the gauntlet of the taxonomic filter in the code.
    # "SGD":"SGD",
    # "ZFIN":"ZFIN",
    # "dictyBase":"dictyBase",
    # "PomBase":"PomBase",
    # "Xenbase":"Xenbase",
    # "FlyBase":"FB",
    # "WormBase":"WB",

    ## For future reference... Genes with this prefix (EnsembleGenome)
    # appear to be in the symbol name space...
    ## Rather than ENSEMBL gene name space (i.e., ENS0000123..))
    ## So we simply use the gene name as is and attempt
    # to map back to ncbi gene id, and uniprot as fallback
    ##"EnsemblGenome": "ENSEMBL"
}


# RMB: 29-Sept-2025: NCBI gene lookup is not used for now
# # Used in make_ncbi_taxon_gene_map function to filter for only species we are interested in
# relevant_ncbi_taxons = {v:'' for v in panther_taxon_map.values()}

# RMB: 29-Sept-2025: NCBI gene lookup is not used for now
# def make_ncbi_taxon_gene_map(gene_info_file: str, relevant_columns: list, taxon_catalog: dict):
#
#     # Ensure relevant columns has #tx_id as the first entry
#     if relevant_columns[0] != "#tax_id":
#         raise RuntimeError("- '#tax_id' must be first element present in relevant_columns arg... Exiting")
#
#     # We don't want entries equivalent to this from this file
#     exclude_terms = {"-":''}
#
#     # Don't want to use pandas here (for memory and other reasons relating to speed)
#     taxa_gene_map = {tx_id:{} for tx_id in taxon_catalog}   # Many-->1 mapping dictionary
#     taxa_remove_map = {tx_id:Counter() for tx_id in taxon_catalog} # Removes unreliable mapping keys from taxa_gene_map
#
#     with gzip.open(gene_info_file, 'rt') as infile:
#
#         # Read the header line into memory to index relevant column fields
#         hinfo = {hfield:i for i,hfield in enumerate(infile.readline().strip('\r').strip('\n').split('\t'))}
#
#         ccc = 0
#         # Now loop through each line and create a map back to taxon / NCBIGene:xyz ...
#         for line in infile:
#             cols = line.strip('\r').strip('\n').split('\t')
#             rel_data = [str(cols[hinfo[r]]) for r in relevant_columns]
#             tx_id = rel_data[0]
#
#             # Only consume species we are interested in
#             if tx_id not in taxon_catalog:
#                 continue
#
#             ncbi_gene_id = cols[hinfo["GeneID"]]
#
#             # Find reliable mapping keys to this NCBI gene id
#             # We take the set() of relevant mapping keys here... that if the same_id is reported on the same line
#             # This removes the possibility of removing an id that is reported twice on the same line
#             removed = []
#             for map_key in set(rel_data[1:]):
#
#                 # Some columns like dbxref contain this character,
#                 # which separates common names from each other. So we loop through them
#                 # (a minority, not a majority, have this in them)
#                 mk_cols = map_key.split("|")
#                 for key_to_ncbi in mk_cols:
#
#                     # Deal with entries like MGI:MGI:95886, where we want to remove one of the MGI: prefix
#                     key_split = key_to_ncbi.split(":")
#                     if len(key_split) >= 2:
#                         if key_split[0] == key_split[1]:
#                             key_to_ncbi = "{}:{}".format(key_split[0], key_split[-1])
#
#                     if key_to_ncbi not in taxa_gene_map[tx_id]:
#                         taxa_gene_map[tx_id].update({key_to_ncbi:ncbi_gene_id})
#                     else:
#                         taxa_remove_map[tx_id][key_to_ncbi] += 1
#
#
#     # Remove unreliable mapping keys to this NCBI gene id
#     for tx_id in taxa_remove_map:
#         for remove_key, rcount in taxa_remove_map[tx_id].items():
#             ##print("- Taxon {} | Removing {} | Count {}".format(tx_id, remove_key, rcount))
#             del taxa_gene_map[tx_id][remove_key]
#
#     # Return cleaned-up mapping to a ncbi gene id
#     # that can be normalized later down the road
#     return taxa_gene_map


def parse_gene_info(
        gene_info,
        taxon_map,
        curie_map,
        # fallback_map - we don't use this NCBI Gene lookup at the moment, but we keep it here for now for reference'
):
    """
    This function takes a panther gene information string and returns the species name and gene identifier in a
    standardized format. This is done by converting to CURIEs based on a predefined mapping in a table and using
    uniprotkb id as a fallback. We also remove ensemble version/transcript ids from the tail end of ensembl ids,
    and we also filter out species that are not in our taxon map. Below are examples of the transformation process

    HUMAN|HGNC=16435|UniProtKB=Q8TCT9 --> HUMAN, HGNC:16435
    SCHPO|PomBase=SPBP23A10.09|UniProtKB=Q9P7X6 --> SCHPO, PomBase:SPBP23A10.09
    CANLF|Ensembl=ENSCAFG00845009646.1|UniProtKB=A0A8I3N1X7 --> CANLF, Ensembl:ENSCAFG00845009646

    :param gene_info: This is a string of the format species|gene|uniprotkb_id
    :param taxon_map: This is a dictionary of the Panther standard species name to NCBI taxon id
    :param curie_map: This is a dictionary of the gene  CURIE prefix mappings
    :return:
    """
    
    cols = gene_info.split("|") # species|gene|uniprotkb_id
    species = cols[0]

    # Exit condition (saves compute when there are many rows to process...)
    if species not in taxon_map:
        return None, None
    
    # Now assign our gene to its "rightful" prefix...
    # If no reasonable prefix exists (HGNC, MGI, etc.),
    # then we use the UniprotKB ID prefix as a fallback.
    # Connections can be rescued through
    # a normalization process, via UniProtKB protein ids
    
    # Our preferred order is Organism specific (HGNC, PomBase, ZFIN)
    # RMB: 29-Sept-2025: taxon_id only used for NCBI gene lookup, which is itself unused for now
    # taxon_id = taxon_map[species]
    gene_split = cols[1].split("=")

    # TODO: not sure what's the point of these counters here... not reported anywhere?
    # matched = 0
    # fback = 0
    # unikb = 0
    
    # Check if gene id can be mapped directly to kg build preferred gene ids
    if gene_split[0] in curie_map:
        # We use -1 here to avoid things like MGI=MGI=95886
        gene = "{}:{}".format(curie_map[gene_split[0]], gene_split[-1])
        # matched = 1

    # TODO: RMB 29-Sept-2025: This is commented out for now:
    #       The NCBIGene resolution is skipped to simply
    #       and speed up the ingestion process for now.
    # # If the gene identifier is already in the 'curie_map'
    # # then we use that as the gene identifier.
    # elif gene_split[1] in curie_map:
    #     gene = "{}:{}".format(curie_map[gene_split[1]], gene_split[-1])
    # # Otherwise, the gene identifiers for the target species
    # # are not the ones in the 'curie_map' so we
    # # fall back onto NCBI Gene map, if possible.
    # elif gene_split[1] in fallback_map[taxon_id]:
    #     g_id = fallback_map[taxon_id][gene_split[1]]
    #     gene = "NCBIGene:{}".format(g_id)
    #     # fback = 1
    
    # Use uniprotkb id as a last resort and format e.g. UniProtKB=Q8TCT9 => "UniProtKB:Q8TCT9"
    else:
        gene = "{}".format(cols[-1].replace("=", ":"))
        # unikb += 1
        
    # Lastly we need to strip version numbers off from ENSEMBL IDs,
    # (e.g. ENSG00000123456.1 => ENSG00000123456)
    if gene.startswith("ENSEMBL:") and (":ENS" in gene):
        gene = gene.split(".")[0]
    
    return species, gene,


## ORION code for Panther parser (to be adapted)
# from https://github.com/RobokopU24/ORION/blob/master/parsers/panther/src/loadPanther.py
# import os
# import csv
# import argparse
# import re
#
# import requests
#
# from bs4 import BeautifulSoup
# from Common.biolink_constants import *
# from Common.utils import GetData
# from Common.loader_interface import SourceDataLoader
# from Common.kgxmodel import kgxnode, kgxedge
# from functools import partial
# from typing import NamedTuple
#
# class LabeledID(NamedTuple):
#     """
#     Labeled Thing Object
#     ---
#     """
#     identifier: str
#     label: str = ''
# #############
# Class: PANTHER loader, Protein ANalysis THrough Evolutionary Relationships
#
# By: Phil Owen
# Date: 4/5/2021
# Desc: Class that loads/parses the PANTHER data.
# #############
# class PLoader(SourceDataLoader):
#
#     source_id: str = 'PANTHER'
#     source_db: str = 'Protein ANalysis THrough Evolutionary Relationships'
#     provenance_id: str = 'infores:panther'
#     parsing_version: str = '1.2'
#
#     def __init__(self, test_mode: bool = False, source_data_dir: str = None):
#         """
#         :param test_mode - sets the run into test mode
#         :param source_data_dir - the specific storage directory to save files in
#         """
#         super().__init__(test_mode=test_mode, source_data_dir=source_data_dir)
#
#         self.data_file: str = None  # data file name changes based on version, will be set below
#         self.data_version: str = None
#         self.get_latest_source_version()
#
#         # the list of columns in the data
#         self.sequence_file_columns = ['gene_identifier', 'protein_id', 'gene_name', 'panther_sf_id', 'panther_family_name',
#                                       'panther_subfamily_name', 'panther_molecular_func', 'panther_biological_process',
#                                       'cellular_components', 'protein_class', 'pathway']
#
#         self.split_mapping = {
#             'gene_identifier': partial(self.split_with, splitter='|', keys=['organism', 'gene_id', 'protein_id'], ignore_length_mismatch=True),
#             'panther_molecular_func': partial(self.split_with, splitter=';'),
#             'panther_biological_process': partial(self.split_with, splitter=';'),
#             'cellular_components': partial(self.split_with, splitter=';'),
#             'pathway': partial(self.split_with, splitter=';')
#         }
#
#         self.__gene_family_data__ = None
#
#     def get_latest_source_version(self) -> str:
#
#         if self.data_version:
#             return self.data_version
#
#         # init the return
#         ret_val: str = 'Not found'
#
#         # load the web page for CTD
#         html_page: requests.Response = requests.get('http://data.pantherdb.org/ftp/sequence_classifications/current_release/PANTHER_Sequence_Classification_files/')
#
#         # get the html into a parsable object
#         resp: BeautifulSoup = BeautifulSoup(html_page.content, 'html.parser')
#
#         # set the search text
#         search_text = 'PTHR*'
#
#         # find the version tag
#         a_tag: BeautifulSoup.Tag = resp.find('a', string=re.compile(search_text))
#
#         # was the tag found
#         if a_tag is not None:
#             # strip off the search text
#             val = a_tag.text.split(search_text[:-1])[1].strip()
#
#             # get the actual version number
#             ret_val = val.split('_')[0]
#
#             # save the version for data gathering later
#             self.data_version = ret_val
#
#             # make the data file name correct
#             self.data_file = f'PTHR{self.data_version}_human'
#
#         # return to the caller
#         return ret_val
#
#     def get_data(self) -> int:
#         """
#         Gets the Panther data.
#
#         """
#         # get a reference to the data gathering class
#         gd: GetData = GetData(self.logger.level)
#
#         # do the real thing if we arent in debug mode
#         if not self.test_mode:
#             # get the complete data set
#             #TODO make these class level variables.
#             file_count: int = gd.pull_via_ftp(
#                'ftp.pantherdb.org',
#                f'/sequence_classifications/{self.data_version}/PANTHER_Sequence_Classification_files/',
#                [self.data_file],
#                self.data_path
#             )
#         else:
#             file_count: int = 1
#
#         # return the file count to the caller
#         return file_count
#
#     @staticmethod
#     def split_with(input_str, splitter, keys=[], ignore_length_mismatch=False):
#         """
#         Splits a string based on splitter. If keys is provided it will return a dictionary where the keys of the dictionary map to
#         the split values.
#         """
#         split = input_str.split(splitter)
#
#         if not keys:
#             return split
#
#         if not ignore_length_mismatch and len(split) != len(keys):
#             raise Exception("Length of keys provided doesn't match split result")
#
#         return {keys[index]: value for index, value in enumerate(split[:len(keys)])}
#
#     @property
#     def gene_family_data(self):
#         """
#         Property that restructures raw csv values into dictionary organized by family and subfamilies of genes.
#         """
#         rows = []
#
#         # if we have already retrieved the data return it
#         if self.__gene_family_data__:
#             return self.__gene_family_data__
#
#         # open up the file
#         with open(os.path.join(self.data_path, self.data_file), 'r', encoding="utf-8") as fp:
#             # get a handle on the input data
#             data = csv.DictReader(fp, delimiter='\t', fieldnames=self.sequence_file_columns)
#
#             for item in data:
#                 rows.append(item)
#
#         with_columns = [{self.sequence_file_columns[index]: value for index, value in enumerate(row)} for row in rows]
#
#         # second pass transform into sub dictionaries for relevant ones
#         for row in with_columns:
#             for key in self.split_mapping:
#                 functor = self.split_mapping[key]
#                 row[key] = functor(row[key])
#
#         # reorganize them to 'family-key'-'sub-family'
#         self.__gene_family_data__ = {}
#
#         for row in rows:
#             fam_id, sub_id = row['panther_sf_id'].split(':')
#             family_name = row['panther_family_name']
#             sub_family_name = row['panther_subfamily_name']
#
#             if fam_id not in self.__gene_family_data__:
#                 self.__gene_family_data__[fam_id] = {
#                     'family_name': family_name
#                 }
#
#             if sub_id not in self.__gene_family_data__[fam_id]:
#                 self.__gene_family_data__[fam_id][sub_id] = {
#                     'sub_family_name': sub_family_name,
#                     'rows': []
#                 }
#
#             self.__gene_family_data__[fam_id][sub_id]['rows'].append(row)
#
#         return self.__gene_family_data__
#
#     def parse_data(self) -> dict:
#         """
#         Parses the data file for graph nodes/edges and writes them to the KGX csv files.
#
#         note: this is a port from robo-commons/greent/services
#
#         :return: ret_val: record counts
#         """
#
#         # init the record counters
#         record_counter: int = 0
#         skipped_record_counter: int = 0
#
#         gene_fam_data = self.gene_family_data
#
#         gene_families: list = []
#
#         for key in gene_fam_data:
#             name = gene_fam_data[key]['family_name']
#
#             gene_families.append(LabeledID(f'PANTHER.FAMILY:{key}', name))
#
#             sub_keys = [k for k in gene_fam_data[key].keys() if k != 'family_name']
#
#             for k in sub_keys:
#                 name = gene_fam_data[key][k]['sub_family_name']
#
#                 gene_families.append(LabeledID(f'PANTHER.FAMILY:{key}:{k}', name))
#
#         # for each family
#         for family in gene_families:
#             self.get_gene_family_by_gene_family(family)
#             self.get_gene_by_gene_family(family)
#             self.get_cellular_component_by_gene_family(family)
#             self.get_pathway_by_gene_family(family)
#             self.get_molecular_function_by_gene_family(family)
#             self.get_biological_process_or_activity_by_gene_family(family)
#
#         self.logger.debug(f'Parsing data file complete.')
#
#         # load up the metadata
#         load_metadata: dict = {
#             'num_source_lines': record_counter,
#             'unusable_source_lines': skipped_record_counter
#         }
#
#         # return to the caller
#         return load_metadata
#
#     def get_gene_family_by_gene_family(self, family):
#         # get the family and sub family info
#         fam_id, sub_fam_id = self.get_family_sub_family_ids_from_curie(family.identifier)
#
#         # is no sub ids search the list
#         if sub_fam_id == None:
#             # we are looking for subfamilies
#             sub_id_keys = [y for y in self.gene_family_data[fam_id] if y != 'family_name']
#
#             # if we got some sub ids for this family
#             if len(sub_id_keys) > 0:
#                 # create the gene family node
#                 gene_family_node = kgxnode(family.identifier, name=family.label)
#                 self.final_node_list.append(gene_family_node)
#
#                 for sub_id in sub_id_keys:
#                     # logger.debug(f'GENE _ FAMILY DATA: { self.gene_family_data[fam_id]}')
#                     sub_family = self.gene_family_data[fam_id][sub_id]
#
#                     # create the gene sub-family node
#                     g_sub_fam_id = f'{family.identifier}:{sub_id}'
#                     gene_sub_family_node = kgxnode(g_sub_fam_id, name=sub_family['sub_family_name'])
#                     self.final_node_list.append(gene_sub_family_node)
#
#                     # create the edge
#                     edge_properties = {KNOWLEDGE_LEVEL: NOT_PROVIDED,
#                                        AGENT_TYPE: NOT_PROVIDED}
#                     new_edge = kgxedge(subject_id=g_sub_fam_id,
#                                        predicate='BFO:0000050',
#                                        object_id=family.identifier,
#                                        primary_knowledge_source=self.provenance_id,
#                                        edgeprops=edge_properties)
#                     self.final_edge_list.append(new_edge)
#
#     def get_gene_by_gene_family(self, family):
#         # get the data rows for this family
#         rows = self.get_rows_using_curie(family.identifier)
#
#         # look at all the family records and get the gene nodes
#         for gene_family_data in rows:
#
#             gene_id = self.get_gene_id_from_row(gene_family_data)
#
#             # if the gene id was found
#             if gene_id is not None:
#                 # get the gene name
#                 gene_name = gene_family_data['gene_name'] if gene_family_data['gene_name'] and len(gene_family_data['gene_name']) > 1 else gene_id
#
#                 # create the gene node
#                 gene_node = kgxnode(gene_id, name=gene_name)
#                 self.final_node_list.append(gene_node)
#
#                 # create the edge
#                 edge_properties = {KNOWLEDGE_LEVEL: NOT_PROVIDED,
#                                    AGENT_TYPE: NOT_PROVIDED}
#                 gene_family_edge = kgxedge(subject_id=gene_id,
#                                            predicate='BFO:0000050',
#                                            object_id=family.identifier,
#                                            primary_knowledge_source=self.provenance_id,
#                                            edgeprops=edge_properties)
#                 self.final_edge_list.append(gene_family_edge)
#
#     def get_biological_process_or_activity_by_gene_family(self, family):
#         # get the data rows for this family
#         rows = self.get_rows_using_curie(family.identifier)
#
#         # look at all the family records
#         for gene_family_data in rows:
#             # for each family record get the biological process nodes
#             for bio_process in gene_family_data['panther_biological_process'].split(';'):
#                 # was there a molecular function
#                 if len(bio_process) > 0:
#                     # get the pathway pieces
#                     name, bio_p_id = bio_process.split('#')
#
#                     # create the node
#                     new_node = kgxnode(bio_p_id, name=name)
#                     self.final_node_list.append(new_node)
#
#                     # create the gene_family-biological_process_or_activity edge
#                     edge_properties = {KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
#                                        AGENT_TYPE: MANUAL_AGENT}
#                     new_edge = kgxedge(subject_id=family.identifier,
#                                        predicate='RO:0002331',
#                                        object_id=bio_p_id,
#                                        primary_knowledge_source=self.provenance_id,
#                                        edgeprops=edge_properties)
#                     self.final_edge_list.append(new_edge)
#
#     def get_molecular_function_by_gene_family(self, family):
#         # get the data rows for this family
#         rows = self.get_rows_using_curie(family.identifier)
#
#         # look at all the family records
#         for gene_family_data in rows:
#             # for each family record get the molecular functions
#             for mole_func in gene_family_data['panther_molecular_func'].split(';'):
#                 # was there a molecular function
#                 if len(mole_func) > 0:
#                     # get the molecular function pieces
#                     name, mole_func_id = mole_func.split('#')
#
#                     # create the node
#                     new_node = kgxnode(mole_func_id, name=name)
#                     self.final_node_list.append(new_node)
#
#                     # create the gene_family-molecular function edge
#                     edge_properties = {KNOWLEDGE_LEVEL: KNOWLEDGE_ASSERTION,
#                                        AGENT_TYPE: MANUAL_AGENT}
#                     new_edge = kgxedge(subject_id=family.identifier,
#                                        predicate='RO:0002327',
#                                        object_id=mole_func_id,
#                                        primary_knowledge_source=self.provenance_id,
#                                        edgeprops=edge_properties)
#                     self.final_edge_list.append(new_edge)
#
#     def get_cellular_component_by_gene_family(self, family):
#         # get the data rows for this family
#         rows = self.get_rows_using_curie(family.identifier)
#
#         # look at all the family records
#         for gene_family_data in rows:
#             # for each family record get the cellular component nodes
#             for item in gene_family_data['cellular_components'].split(';'):
#                 # was there a cellular component
#                 if len(item) > 0:
#                     # get the pieces
#                     name, cellular_component_id = item.split('#')
#
#                     # create the gene sub-family node
#                     new_node = kgxnode(cellular_component_id, name=name)
#                     self.final_node_list.append(new_node)
#
#                     # create the gene_family-cellular_component edge
#                     edge_properties = {KNOWLEDGE_LEVEL: NOT_PROVIDED,
#                                        AGENT_TYPE: NOT_PROVIDED}
#                     new_edge = kgxedge(subject_id=family.identifier,
#                                        predicate='RO:0001025',
#                                        object_id=cellular_component_id,
#                                        primary_knowledge_source=self.provenance_id,
#                                        edgeprops=edge_properties)
#                     self.final_edge_list.append(new_edge)
#
#     def get_pathway_by_gene_family(self, family):
#         # get the data rows for this family
#         rows = self.get_rows_using_curie(family.identifier)
#
#         # look at all the family records
#         for gene_family_data in rows:
#             # for each family record find the pathway
#             pathway = gene_family_data['pathway'].split('>')
#
#             # was there a pathway
#             if len(pathway) > 0 and len(pathway[0]) > 0:
#                 # get the pathway pieces
#                 pathway_name, pathway_access = pathway[0].split('#')
#
#                 # create the pathway node
#                 panther_pathway_id = f'PANTHER.PATHWAY:{pathway_access}'
#                 new_node = kgxnode(panther_pathway_id, name=pathway_name)
#                 self.final_node_list.append(new_node)
#
#                 # create the gene_family-pathway edge
#                 edge_properties = {KNOWLEDGE_LEVEL: NOT_PROVIDED,
#                                    AGENT_TYPE: NOT_PROVIDED}
#                 new_edge = kgxedge(subject_id=panther_pathway_id,
#                                    predicate='RO:0000057',
#                                    object_id=family.identifier,
#                                    primary_knowledge_source=self.provenance_id,
#                                    edgeprops=edge_properties)
#                 self.final_edge_list.append(new_edge)
#
#     def get_gene_id_from_row(self, row):
#         gene_data = row['gene_identifier']
#         gene_data = gene_data.split('|')
#         gene_field = gene_data[1]
#         # these are not useful IDs
#         if "Gene" not in gene_field:
#             gene_id = gene_field.replace('=', ':').upper()
#             return gene_id
#         return None
#
#     @staticmethod
#     def un_curie (text):
#         return ':'.join(text.split (':', 1)[1:]) if ':' in text else text
#
#     def get_rows_using_curie(self, curie):
#         """
#         Get all information from the Panther.gene_family_data using a panther identifier.
#         """
#         fam_id, sub_fam_id = self.get_family_sub_family_ids_from_curie(curie)
#         if sub_fam_id == None:
#             rows = []
#             sub_ids = [y for y in list(self.gene_family_data[fam_id].keys()) if y != 'family_name']
#             for sub_id in sub_ids:
#                 rows += [x for x in self.gene_family_data[fam_id][sub_id]['rows'] if x not in rows]
#             return rows
#         return self.gene_family_data[fam_id][sub_fam_id]['rows']
#
#     def get_family_sub_family_ids_from_curie(self, curie):
#         """
#         Splits a panther curie into family id and sub family id
#         whenever possible.
#         """
#         if 'PANTHER.FAMILY' in curie:
#             curie = self.un_curie(curie)
#
#         splitted = curie.split(':')
#
#         if len(splitted) == 1:
#             return (splitted[0], None)
#
#         return (splitted)
