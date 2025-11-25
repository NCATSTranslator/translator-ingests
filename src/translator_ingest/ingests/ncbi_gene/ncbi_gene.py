from typing import Any

from biolink_model.datamodel.pydanticmodel_v2 import Gene
from koza.model.graphs import KnowledgeGraph
import koza

from translator_ingest.util.http_utils import get_modify_date


def get_latest_version() -> str:
    return get_modify_date("https://ftp.ncbi.nlm.nih.gov/gene/DATA/gene_info.gz")


@koza.on_data_begin()
def on_begin_ncbi_gene(koza_app: koza.KozaTransform) -> None:
    """Track processing statistics"""
    koza_app.state["total_records_processed"] = 0
    koza_app.state["genes_created"] = 0
    koza_app.state["genes_by_taxon"] = {"9606": 0, "10090": 0, "10116": 0}
    koza_app.state["filtered_records"] = 0


@koza.on_data_end()
def on_end_ncbi_gene(koza_app: koza.KozaTransform) -> None:
    """Log processing statistics"""
    koza_app.log("NCBI Gene processing complete:", level="INFO")
    koza_app.log(f"  Total records processed: {koza_app.state['total_records_processed']}", level="INFO")
    koza_app.log(f"  Records filtered out: {koza_app.state['filtered_records']}", level="INFO")
    koza_app.log(f"  Genes created: {koza_app.state['genes_created']}", level="INFO")
    koza_app.log(f"  Human genes: {koza_app.state['genes_by_taxon']['9606']}", level="INFO")
    koza_app.log(f"  Mouse genes: {koza_app.state['genes_by_taxon']['10090']}", level="INFO")
    koza_app.log(f"  Rat genes: {koza_app.state['genes_by_taxon']['10116']}", level="INFO")


@koza.transform_record()
def transform_record(koza_app: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    """
    Transform NCBI Gene record into biolink:Gene node.
    """

    koza_app.state["total_records_processed"] += 1

    # Double-check the filter - only process allowed taxon IDs
    allowed_taxons = ["9606", "10090", "10116"]
    if record["tax_id"] not in allowed_taxons:
        koza_app.state["filtered_records"] += 1
        return None

    gene = Gene(
        id=f'NCBIGene:{record["GeneID"]}',
        symbol=record["Symbol"],
        name=record["Symbol"],
        full_name=record["Full_name_from_nomenclature_authority"],
        description=record["description"],
        taxon=f'NCBITaxon:{record["tax_id"]}',
        category=["biolink:Gene"]
    )

    # Track genes by taxon and increment created count
    koza_app.state["genes_created"] += 1
    koza_app.state["genes_by_taxon"][record["tax_id"]] += 1

    return KnowledgeGraph(nodes=[gene], edges=[])
