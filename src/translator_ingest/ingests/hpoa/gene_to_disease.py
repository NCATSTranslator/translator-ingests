import uuid

from biolink_model.datamodel.pydanticmodel_v2 import GeneToDiseaseAssociation, CausalGeneToDiseaseAssociation, \
    CorrelatedGeneToDiseaseAssociation

# from koza.cli_runner import get_koza_app
from koza.runner import KozaTransform

from src.translator_ingest.util.monarch.constants import INFORES_MONARCHINITIATIVE, BIOLINK_CAUSES
from src.translator_ingest.ingests.hpoa.hpoa_utils import get_knowledge_sources, get_predicate

koza_app = get_koza_app("hpoa_gene_to_disease")


while (row := koza_app.get_row()) is not None:
    gene_id = row["ncbi_gene_id"]
    disease_id = row["disease_id"].replace("ORPHA:", "Orphanet:")

    predicate = get_predicate(row["association_type"])

    primary_knowledge_source, aggregator_knowledge_source = get_knowledge_sources(row["source"], INFORES_MONARCHINITIATIVE)

    if predicate == BIOLINK_CAUSES:
        association_class = CausalGeneToDiseaseAssociation
    else:
        association_class = CorrelatedGeneToDiseaseAssociation

    association = association_class(
        id="uuid:" + str(uuid.uuid1()),
        subject=gene_id,
        predicate=predicate,
        object=disease_id,
        primary_knowledge_source=primary_knowledge_source,
        aggregator_knowledge_source=aggregator_knowledge_source
    )

    koza_app.write(association)
