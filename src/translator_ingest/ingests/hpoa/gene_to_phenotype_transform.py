import uuid

from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    GeneToPhenotypicFeatureAssociation,
    KnowledgeLevelEnum,
)

# from koza.cli_utils import get_koza_app
from koza.runner import KozaTransform

from phenotype_ingest_utils import Frequency, phenotype_frequency_to_hpo_term

# TO DO: Once biolink is updated with the disease_context_qualifier slot we need to update the association we make
# https://github.com/biolink/biolink-model/pull/1524


# Initiate koza app and mondo map from sssom file
koza_app = get_koza_app("hpoa_gene_to_phenotype")
mondo_map = koza_app.get_map('mondo_map')


while (row := koza_app.get_row()) is not None:
    gene_id = "NCBIGene:" + row["ncbi_gene_id"]
    phenotype_id = row["hpo_id"]

    # No frequency data provided
    if row["frequency"] == "-":
        frequency = Frequency()
    else:
        # Raw frequencies - HPO term curies, ratios, percentages - normalized to HPO terms
        frequency: Frequency = phenotype_frequency_to_hpo_term(row["frequency"])
    
    # Convert to mondo id if possible, otherwise leave as is
    org_id = row["disease_id"].replace("ORPHA:", "Orphanet:")
    dis_id = org_id
    if dis_id in mondo_map:
        dis_id = mondo_map[dis_id]['subject_id']


    publications = [pub.strip() for pub in row["publications"].split(";")] if row["publications"] else []

    association = GeneToPhenotypicFeatureAssociation(id="uuid:" + str(uuid.uuid1()),
                                                     subject=gene_id,
                                                     predicate="biolink:has_phenotype",
                                                     object=phenotype_id,
                                                     aggregator_knowledge_source=["infores:monarchinitiative"],
                                                     primary_knowledge_source="infores:hpo-annotations",
                                                     knowledge_level=KnowledgeLevelEnum.logical_entailment,
                                                     agent_type=AgentTypeEnum.automated_agent,
                                                     frequency_qualifier=frequency.frequency_qualifier if frequency.frequency_qualifier else None,
                                                     has_percentage=frequency.has_percentage,
                                                     has_quotient=frequency.has_quotient,
                                                     has_count=frequency.has_count,
                                                     has_total=frequency.has_total,
                                                     disease_context_qualifier=dis_id,
                                                     publications=publications)

    koza_app.write(association)
