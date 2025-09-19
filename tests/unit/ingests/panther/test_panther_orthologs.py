"""
An example test file for the transform script.

It uses pytest fixtures to define the input data and the mock koza transform.
The test_example function then tests the output of the transform script.

See the Koza documentation for more information on testing transforms:
https://koza.monarchinitiative.org/Usage/testing/

Unit tests for Panther Gene Orthology relationships ingest
"""

import pytest
from biolink_model.datamodel.pydanticmodel_v2 import GeneToGeneHomologyAssociation, KnowledgeLevelEnum, AgentTypeEnum

from translator_ingest.ingests.panther.panther_orthologs_utils import (
    make_ncbi_taxon_gene_map,
    parse_gene_info,
    panther_taxon_map,
    relevant_ncbi_cols,
    relevant_ncbi_taxons,
    db_to_curie_map
)


################################################################################
### Fixtures referencing code and basic parameters reused in the tests below ###
@pytest.fixture
def source_name():
    """
    :return: string name of the data ingest source found within transform yaml
    """
    return "panther_genome_orthologs"


@pytest.fixture
def script():
    """
    :return: string path to Panther Gene Orthology relationships ingest script
    """
    return "./src/pantherdb_orthologs_ingest/transform.py"


@pytest.fixture()
def relevant_association_test_keys():
    """
    :return: list of keys that are relevant to the association test
    """
    return ["subject",
            "object",
            "predicate",
            "has_evidence",
            "aggregator_knowledge_source",
            "primary_knowledge_source",
            "knowledge_level",
            "agent_type"]


@pytest.fixture
def ncbi_gene_info_path():
    """
    :return: string of path to gaf-eco-mappings.txt file
    """
    return "./tests/test_ncbi_gene_info.txt.gz"


@pytest.fixture
def ncbi_gene_info_cols():
    return relevant_ncbi_cols


@pytest.fixture
def ncbi_taxons():
    return relevant_ncbi_taxons


@pytest.fixture
def map_cache(ncbi_gene_info_path, ncbi_gene_info_cols, ncbi_taxons):
    map_cache = make_ncbi_taxon_gene_map(gene_info_file=ncbi_gene_info_path,
                                         relevant_columns=ncbi_gene_info_cols,
                                         taxon_catalog=ncbi_taxons)
    return map_cache


#############################################################################
### Fixture for panther rows/records to test for proper koza associations ###
@pytest.fixture
def panther_rows():

    data = [# Human and rat ortholog row test
            ({"Gene": "HUMAN|HGNC=11477|UniProtKB=Q6GZX4",  # species1|DB=id1|protdb=pdbid1
              "Ortholog": "RAT|RGD=1564893|UniProtKB=Q6GZX2",  # species2|DB=id2|protdb=pdbid2
              "Type of ortholog": "LDO",  # [LDO, O, P, X ,LDX]  see: localtt
              "Common ancestor for the orthologs": "Euarchontoglires",  # unused
              "Panther Ortholog ID": "PTHR12434"},  # panther_id
              
              {"subject": "HGNC:11477",
                "object": "RGD:1564893",
                "predicate": "biolink:orthologous_to",
                "has_evidence": ["PANTHER.FAMILY:PTHR12434"],
                "aggregator_knowledge_source": ["infores:monarchinitiative"],
                "primary_knowledge_source": "infores:panther",
                "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
                "agent_type": AgentTypeEnum.not_provided}),
            
            # Mouse and Schizosaccharomyces pombe ortholog row test
            ({"Gene":"MOUSE|MGI=MGI=2147627|UniProtKB=Q91WQ3",
              "Ortholog": "SCHPO|PomBase=SPAC30C2.04|UniProtKB=Q9P6K7",
              "Type of ortholog": "LDO", 
              "Common ancestor for the orthologs": "Opisthokonts",
              "Panther Ortholog ID": "PTHR11586"},
             
             {"subject": "MGI:2147627",
              "object": "PomBase:SPAC30C2.04",
              "predicate": "biolink:orthologous_to",
              "has_evidence": ["PANTHER.FAMILY:PTHR11586"],
              "aggregator_knowledge_source": ["infores:monarchinitiative"],
              "primary_knowledge_source": "infores:panther",
              "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
              "agent_type": AgentTypeEnum.not_provided}),

            # Xenopus tropicalis and ceravisea ("yeast") ortholog row test
            ({"Gene": "XENTR|Xenbase=XB-GENE-957143|UniProtKB=Q6P335",
              "Ortholog": "YEAST|SGD=S000004439|UniProtKB=P32366",
              "Type of ortholog": "O", 
              "Common ancestor for the orthologs": "Opisthokonts",
              "Panther Ortholog ID": "PTHR11028"},
    
            {"subject": "Xenbase:XB-GENE-957143",
             "object": "SGD:S000004439",
             "predicate": "biolink:orthologous_to",
             "has_evidence": ["PANTHER.FAMILY:PTHR11028"],
             "aggregator_knowledge_source": ["infores:monarchinitiative"],
             "primary_knowledge_source": "infores:panther",
             "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
             "agent_type": AgentTypeEnum.not_provided}),

            # Zebrafish and fly ortholog row test
            ({"Gene": "DANRE|ZFIN=ZDB-GENE-050417-421|UniProtKB=Q567X8",
              "Ortholog": "DROME|FlyBase=FBgn0002773|UniProtKB=P18432",
              "Type of ortholog": "O", 
              "Common ancestor for the orthologs": "Bilateria",
              "Panther Ortholog ID": "PTHR23049"},
    
             {"subject": "ZFIN:ZDB-GENE-050417-421",
              "object": "FB:FBgn0002773",
              "predicate": "biolink:orthologous_to",
              "has_evidence": ["PANTHER.FAMILY:PTHR23049"],
              "aggregator_knowledge_source": ["infores:monarchinitiative"],
              "primary_knowledge_source": "infores:panther",
              "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
              "agent_type": AgentTypeEnum.not_provided}),
            
            # C. elegans and Dictyostelium discoideum ortholog row test
            ({"Gene": "CAEEL|WormBase=WBGene00009059|UniProtKB=Q19739",
              "Ortholog": "DICDI|dictyBase=DDB_G0269178|UniProtKB=Q9GPS0",
              "Type of ortholog": "O", 
              "Common ancestor for the orthologs": "Unikonts",
              "Panther Ortholog ID": "PTHR24072"},
    
             {"subject": "WB:WBGene00009059",
              "object": "dictyBase:DDB_G0269178",
              "predicate": "biolink:orthologous_to",
              "has_evidence": ["PANTHER.FAMILY:PTHR24072"],
              "aggregator_knowledge_source": ["infores:monarchinitiative"],
              "primary_knowledge_source": "infores:panther",
              "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
              "agent_type": AgentTypeEnum.not_provided}),
            
            # Human Ensembl gene and mouse ortholog row test
            ({"Gene": "HUMAN|Ensembl=ENSG00000275949.5|UniProtKB=A0A0G2JMH3",
              "Ortholog": "MOUSE|MGI=MGI=99431|UniProtKB=P84078",
              "Type of ortholog": "O", 
              "Common ancestor for the orthologs": "Euarchontoglires",
              "Panther Ortholog ID": "PTHR11711"},
    
             {"subject": "ENSEMBL:ENSG00000275949",
              "object": "MGI:99431",
              "predicate": "biolink:orthologous_to",
              "has_evidence": ["PANTHER.FAMILY:PTHR11711"],
              "aggregator_knowledge_source": ["infores:monarchinitiative"],
              "primary_knowledge_source": "infores:panther",
              "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
              "agent_type": AgentTypeEnum.not_provided})]

    return data


#################################################
### Fixtures to test parse_gene_info function ###
@pytest.fixture
def panther_species_genes():
    species_genes = [["HUMAN|Ensembl=ENSG00000275949.5|UniProtKB=A0A0G2JMH3", {"ENSEMBL:ENSG00000275949":''}],
                     ["HUMAN|HGNC=11477|UniProtKB=Q6GZX4", {"HGNC:11477":''}],
                     ["MOUSE|MGI=MGI=99431|UniProtKB=P84078", {"MGI:99431":''}],
                     ["CANLF|Ensembl=ENSCAFG00845004769.1", {"ENSEMBL:ENSCAFG00845004769":''}],
                     ["BOVIN|Ensembl=ENSBTAG00000048390.1|UniProtKB=A0A3Q1NMJ3", {"ENSEMBL:ENSBTAG00000048390":''}],
                     ["PIG|Ensembl=ENSSSCG00000033574.3|UniProtKB=A0A8W4FPJ3", {"ENSEMBL:ENSSSCG00000033574":''}],
                     ["RAT|Ensembl=ENSRNOG00000066524.1|UniProtKB=A0A8I5ZRQ5", {"ENSEMBL:ENSRNOG00000066524":''}],
                     ["CHICK|Ensembl=ENSGALG00000014680|UniProtKB=F1NB96", {"ENSEMBL:ENSGALG00000014680":''}],
                     ["XENTR|Ensembl=ENSXETG00000030579|UniProtKB=A0A6I8PUG3", {"ENSEMBL:ENSXETG00000030579":''}],
                     ["DANRE|ZFIN=ZDB-GENE-080205-1|UniProtKB=A8WFS6", {"ZFIN:ZDB-GENE-080205-1":''}],
                     ["DROME|FlyBase=FBgn0010348|UniProtKB=P61209", {"FB:FBgn0010348":''}],
                     ["CAEEL|WormBase=WBGene00000446|UniProtKB=P34663", {"WB:WBGene00000446":''}],
                     ["DICDI|dictyBase=DDB_G0274381|UniProtKB=P54642", {"dictyBase:DDB_G0274381":''}],
                     
                     # Aspergillus will be our test cases to ensure mapping back to NCBIGene is done properly
                     # and the scenario where we use UniProtKB identifier instead
                     ["EMENI|Gene_ORFName=AN0062|UniProtKB=Q5BHB8", {"NCBIGene:ANIA_00062":'',
                                                                     "UniProtKB:Q5BHB8":''}],

                     ["EMENI|EnsemblGenome=ANIA_08553|UniProtKB=Q5AT27", {"NCBIGene:2868830":'', 
                                                                          "UniProtKB:Q5AT27":''}],
                     
                     ["SCHPO|PomBase=SPAC13G7.02c|UniProtKB=Q10265", {"PomBase:SPAC13G7.02c":''}],
                     ["YEAST|SGD=S000003465|UniProtKB=P17442", {"SGD:S000003465":''}]]

    return species_genes


@pytest.fixture
def exclude_species_genes():
    exclude_species_genes = ["FAKE_SPECIES_13|Ensembl=ENSFAKE00000000001|UniProtKB=FAKE1234"]
    return exclude_species_genes


###########################################################
### Perform our actual tests here on the fixtures above ###
def test_panther_rows(panther_rows, source_name, script, mock_koza, map_cache, relevant_association_test_keys):

    # Upack our mock koza transform and expected info
    rows_to_test, expected_res = [v[0] for v in panther_rows], [v[1] for v in panther_rows]

    # Call upon mock_koza just once here, to process all our test rows at once
    koza_associations = mock_koza(name=source_name, data=rows_to_test, transform_code=script, map_cache=map_cache)

    # Now check our koza generated associations against expected results
    for koza_association, expected_info in zip(koza_associations, expected_res):
    
        # Ensure the association type is correct (we are dealing with GeneToGeneHomologyAssociation s here)
        assert isinstance(koza_association, GeneToGeneHomologyAssociation)

        # Test that mock koza processing of pantherdb row produces expected results
        for key in relevant_association_test_keys:
            assert key in expected_info
            assert getattr(koza_association, key) == expected_info[key]


def test_species_parse_gene(panther_species_genes, map_cache):
    for gene_info, expected in panther_species_genes:
        species, gene_id = parse_gene_info(gene_info, 
                                           panther_taxon_map, 
                                           db_to_curie_map,
                                           map_cache)
                                           
        # Assert that the parsed species and gene ID match the expected values
        assert species in panther_taxon_map
        assert gene_id in expected    # Allows for multiple mapping values to be present
        

def test_exclude_species_parse_gene(exclude_species_genes, map_cache):
    for gene_info in exclude_species_genes:
        species, gene_id = parse_gene_info(gene_info, 
                                           panther_taxon_map, 
                                           db_to_curie_map,
                                           map_cache)

        # Assert that the species and gene ID are empty for excluded species
        assert not species
        assert not gene_id
