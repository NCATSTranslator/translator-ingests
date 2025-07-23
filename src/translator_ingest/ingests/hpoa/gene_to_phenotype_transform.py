"""
The [Human Phenotype Ontology](https://hpo.jax.org/) group
curates and assembles over 115,000 annotations to hereditary diseases
using the HPO ontology. Here we create Biolink associations
between genes and associated phenotypes.
"""
import uuid
from typing import Dict, Iterable

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    Association,
    Gene,
    PhenotypicFeature,
    GeneToPhenotypicFeatureAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum
)

from phenotype_ingest_utils import Frequency, phenotype_frequency_to_hpo_term

# All HPOA ingest submodules share one
# simplistic ingest versioning (for now)
from . import get_latest_version


"""
def prepare(records: Iterator[Dict] = None) -> Iterator[Dict] | None:
    # prepare is just a function that gets run before transform or transform_record ie to seed a database
    # return an iterator of dicts if that makes sense,
    # or we could use env vars to just provide access to the data/db in transform()
    return records
"""

#
##### ORIGINAL Koza-centric ingest code
#
# # Initiate koza app and mondo map from sssom file
# koza_app = get_koza_app("hpoa_gene_to_phenotype")
# mondo_map = koza_app.get_map('mondo_map')
#
# while (row := koza_app.get_row()) is not None:
#     gene_id = "NCBIGene:" + row["ncbi_gene_id"]
#     phenotype_id = row["hpo_id"]
#
#     # No frequency data provided
#     if row["frequency"] == "-":
#         frequency = Frequency()
#     else:
#         # Raw frequencies - HPO term curies, ratios, percentages - normalized to HPO terms
#         frequency: Frequency = phenotype_frequency_to_hpo_term(row["frequency"])
#
#     # Convert to mondo id if possible, otherwise leave as is
#     org_id = row["disease_id"].replace("ORPHA:", "Orphanet:")
#     dis_id = org_id
#     if dis_id in mondo_map:
#         dis_id = mondo_map[dis_id]['subject_id']
#
#
#     publications = [pub.strip() for pub in row["publications"].split(";")] if row["publications"] else []
#
#     association = GeneToPhenotypicFeatureAssociation(id="uuid:" + str(uuid.uuid1()),
#                                                      subject=gene_id,
#                                                      predicate="biolink:has_phenotype",
#                                                      object=phenotype_id,
#                                                      primary_knowledge_source="infores:hpo-annotations",
#                                                      knowledge_level=KnowledgeLevelEnum.logical_entailment,
#                                                      agent_type=AgentTypeEnum.automated_agent,
#                                                      frequency_qualifier=frequency.frequency_qualifier if frequency.frequency_qualifier else None,
#                                                      has_percentage=frequency.has_percentage,
#                                                      has_quotient=frequency.has_quotient,
#                                                      has_count=frequency.has_count,
#                                                      has_total=frequency.has_total,
#                                                      disease_context_qualifier=dis_id,
#                                                      publications=publications)
#
#     koza_app.write(association)
#

# TODO: Initialize MONDO map from sssom file;
#       alternately, don't worry about this since
#       the subsequent Normalization step might fix this?
# mondo_map = koza_app.get_map('mondo_map')

def transform_record(record: Dict) -> (Iterable[NamedThing], Iterable[Association]):

    gene_id = "NCBIGene:" + record["ncbi_gene_id"]
    gene = Gene(id=gene_id, name=record["gene_symbol"],**{})

    hpo_id = record["hpo_id"]
    assert hpo_id, "HPOA Disease to Phenotype has missing HP ontology ('HPO_ID') field identifier?"
    phenotype = PhenotypicFeature(id=hpo_id, **{})

    # No frequency data provided
    if record["frequency"] == "-":
        frequency = Frequency()
    else:
        # Raw frequencies - HPO term curies, ratios, percentages - normalized to HPO terms
        frequency: Frequency = phenotype_frequency_to_hpo_term(record["frequency"])

    # Convert to mondo id if possible, otherwise leave as is
    dis_id = record["disease_id"].replace("ORPHA:", "Orphanet:")

    # TODO: Need to uncomment this once the MONDO map access
    #       is sorted out here (see above comment). This
    #       "normalization" of the disease context could be sorted out in a later pipeline step?
    # if dis_id in mondo_map:
    #     dis_id = mondo_map[dis_id]['subject_id']

    publications = [pub.strip() for pub in record["publications"].split(";")] if record["publications"] else []

    association = GeneToPhenotypicFeatureAssociation(
        id="uuid:" + str(uuid.uuid1()),
        subject=gene_id,
        predicate="biolink:has_phenotype",
        object=hpo_id,
        frequency_qualifier=frequency.frequency_qualifier if frequency.frequency_qualifier else None,
        has_percentage=frequency.has_percentage,
        has_quotient=frequency.has_quotient,
        has_count=frequency.has_count,
        has_total=frequency.has_total,
        disease_context_qualifier=dis_id,
        publications=publications,
        primary_knowledge_source="infores:hpo-annotations",
        knowledge_level=KnowledgeLevelEnum.logical_entailment,
        agent_type=AgentTypeEnum.automated_agent,
        **{}
    )

    return [gene, phenotype],[association]


"""
this is just an example of the interface, using transform() offers the opportunity to do something more efficient
def transform(records: Iterator[Dict]) -> Iterable[tuple[Iterable[NamedThing], Iterable[Association]]]:
    for record in records:
        gene_id = "NCBIGene:" + record["ncbi_gene_id"]
        gene = Gene(id=gene_id, name=record["gene_symbol"],**{})
    
        hpo_id = record["hpo_id"]
        assert hpo_id, "HPOA Disease to Phenotype has missing HP ontology ('HPO_ID') field identifier?"
        phenotype = PhenotypicFeature(id=hpo_id, **{})
    
        # No frequency data provided
        if record["frequency"] == "-":
            frequency = Frequency()
        else:
            # Raw frequencies - HPO term curies, ratios, percentages - normalized to HPO terms
            frequency: Frequency = phenotype_frequency_to_hpo_term(record["frequency"])
    
        # Convert to mondo id if possible, otherwise leave as is
        dis_id = record["disease_id"].replace("ORPHA:", "Orphanet:")
    
        # TODO: Need to uncomment this once the MONDO map access
        #       is sorted out here (see above comment). This
        #       "normalization" of the disease context could be sorted out in a later pipeline step?
        # if dis_id in mondo_map:
        #     dis_id = mondo_map[dis_id]['subject_id']
    
        publications = [pub.strip() for pub in record["publications"].split(";")] if record["publications"] else []
    
        association = GeneToPhenotypicFeatureAssociation(
            id="uuid:" + str(uuid.uuid1()),
            subject=gene_id,
            predicate="biolink:has_phenotype",
            object=hpo_id,
            frequency_qualifier=frequency.frequency_qualifier if frequency.frequency_qualifier else None,
            has_percentage=frequency.has_percentage,
            has_quotient=frequency.has_quotient,
            has_count=frequency.has_count,
            has_total=frequency.has_total,
            disease_context_qualifier=dis_id,
            publications=publications,
            primary_knowledge_source="infores:hpo-annotations",
            knowledge_level=KnowledgeLevelEnum.logical_entailment,
            agent_type=AgentTypeEnum.automated_agent,
            **{}
        )
    
        yield [gene, phenotype],[association]
"""
