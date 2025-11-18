import logging
import os
from typing import Any, Tuple
from functools import lru_cache

import requests
from dotenv import load_dotenv
from biolink_model.datamodel.pydanticmodel_v2 import Gene
from koza.model.graphs import KnowledgeGraph
import koza

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@lru_cache(maxsize=128)
def get_taxon_name(taxon_id: str) -> str:
    """
    Fetch the scientific name for a given NCBI taxon ID via E-utilities.
    """
    ncbi_info = get_ncbi_access_data()
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
    params = {
        "db": "taxonomy",
        "id": taxon_id,
        "retmode": "json",
        "api_key": ncbi_info[0],
        "email": ncbi_info[1]
    }
    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()
    result = data.get("result", {})
    taxon_info = result.get(taxon_id)

    if taxon_info and "scientificname" in taxon_info:
        name = taxon_info["scientificname"]
        return name
    else:
        logger.warning(f"No scientific name found for taxon ID {taxon_id}")
        return ""


def get_ncbi_access_data() -> Tuple[str, str]:
    load_dotenv()
    api_key = os.getenv("NCBI_API_KEY")
    mail = os.getenv("NCBI_MAIL")

    if not api_key:
        logger.debug("No NCBI_API_KEY provided. Will use none.")

    if not mail:
        logger.debug("No NCBI_MAIL provided. Will use none.")

    return api_key, mail


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
