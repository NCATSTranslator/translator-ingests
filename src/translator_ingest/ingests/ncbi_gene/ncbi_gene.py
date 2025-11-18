import logging
from typing import Any

from biolink_model.datamodel.pydanticmodel_v2 import Gene
from koza.model.graphs import KnowledgeGraph
import koza

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_taxon_name(taxon_id: str) -> str:
    """
    Get the scientific name for human, mouse, and rat taxon IDs.
    """
    taxon_names = {
        "9606": "Homo sapiens",
        "10090": "Mus musculus", 
        "10116": "Rattus norvegicus"
    }
    
    return taxon_names.get(taxon_id, "")


def get_latest_version() -> str:
    return "2024-12-01"


@koza.transform_record()
def transform_record(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    """
    Transform NCBI Gene record into biolink:Gene node.
    """

    # Get taxon label if not already cached
    if "object_taxon_label" not in record or not record["object_taxon_label"]:
        in_taxon_label = get_taxon_name(record["tax_id"])
        record["object_taxon_label"] = in_taxon_label

    gene = Gene(
        id='NCBIGene:' + record["GeneID"],
        symbol=record["Symbol"],
        name=record["Symbol"],
        full_name=record["Full_name_from_nomenclature_authority"],
        description=record["description"],
        in_taxon=['NCBITaxon:' + record["tax_id"]],
        provided_by=["infores:ncbi-gene"],
        in_taxon_label=record["object_taxon_label"]
    )

    return KnowledgeGraph(nodes=[gene])
