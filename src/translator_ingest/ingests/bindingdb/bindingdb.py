from typing import Optional, Any, Iterable
from datetime import datetime
from pathlib import Path
import polars as pl

import koza
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalGeneInteractionAssociation,
    Protein,
    KnowledgeLevelEnum,
    AgentTypeEnum
)
from bmt.pydantic import entity_id, build_association_knowledge_sources
from koza.model.graphs import KnowledgeGraph

from translator_ingest.ingests.bindingdb.bindingdb_util import (
    extract_bindingdb_columns_polars,
    process_publications,

    CURATION_DATA_SOURCE_TO_INFORES_MAPPING,
    LINK_TO_LIGAND_TARGET_PAIR,
    MONOMER_ID,
    TARGET_NAME,
    SOURCE_ORGANISM,
    CURATION_DATASOURCE,
    PUBCHEM_CID,
    UNIPROT_ID,
    PUBLICATION,
    SUPPORTING_DATA_ID,
    REACTANT_SET_ID,
    ARTICLE_DOI,
    PMID,
    PATENT_NUMBER
)


BINDINGDB_COLUMNS = (
    REACTANT_SET_ID,
    MONOMER_ID,
    PUBCHEM_CID,
    TARGET_NAME,
    SOURCE_ORGANISM,
    UNIPROT_ID,
    CURATION_DATASOURCE,
    ARTICLE_DOI,
    PMID,
    PATENT_NUMBER
)

SOURCE_ORGANISM_TO_TAXON_ID_MAPPING = {
    "Homo sapiens": "9606",
    "Mus musculus": "10090",
    "Rattus norvegicus": "10116",
    "Bos taurus": "9913",   # cattle
    "Sus scrofa": "9823",     # swine
    "Xenopus laevis": "8355",   # Xenopus laevis (African clawed frog)
    "Xenopus tropicalis": "8364",   # Xenopus tropicalis - tropical clawed frog
    "Danio rerio": "7955",
    "Drosophila melanogaster": "7227",
    "Caenorhabditis elegans": "6239",
    "Schizosaccharomyces pombe": "4896",
    "Saccharomyces cerevisiae": "4932"
}


def get_latest_version() -> str:
    # According to the BindingDb team,
    # a fresh year+month date-stamped release
    # of BindingDb data is made at the start of each month,
    # so we use the heuristic of a date function to return
    # this candidate 'latest release' value.
    return datetime.today().strftime("%Y%m")


@koza.on_data_begin()
def on_begin_ingest_by_record(koza_transform: koza.KozaTransform) -> None:
    pass


@koza.on_data_end()
def on_end_ingest_by_record(koza_transform: koza.KozaTransform) -> None:
    for tag, value in koza_transform.transform_metadata.items():
        koza_transform.log(
            msg=f"Exception {str(tag)} encountered for records: {',\n'.join(value)}.",
            level="WARNING"
        )

@koza.prepare_data()
def prepare_bindingdb_data(
        koza_transform: koza.KozaTransform,
        data: Iterable[dict[str, Any]]
) -> Iterable[dict[str, Any]] | None:
    """
    Given the large size and complex structure of the BindingDB data file,
    this method bypasses the Koza input reader mechanism to directly preprocess
    the original data file downloaded by the download.yaml specification.

    BindingDb records typically come in groups of identical ligand-target interactions
    characterized across more than one assay. We are therefore going to leverage the
    @koza.prepare_data decorated method to perform a simple consolidation of such
    edges across rows, returning a single edge per unique ligand-target pair.

    This polars-based consolidates duplicate assay records for the same
    ligand-target pair using polars' efficient grouping and aggregation.

    Possible performance improvements over the original implementation:
    - 5-20x faster for grouping operations
    - More explicit and maintainable logic
    - Better memory efficiency

    :param koza_transform: The koza.KozaTransform context of the data processing.
    :param data: Iterable[dict[str, Any]], @koza.prepare_data() specified input data
                 parameter is not set by the pipeline, hence ignored.

    :return: Iterable[dict[str, Any]], Consolidation of related assay records
             for each unique ligand-target pair, with possible aggregation
             of distinct annotation encountered across the original set of assays.
    """
    try:
        # As of December 2025, the BindingDB input file is
        # assumed to be a Zipfile archive with a single file inside
        data_archive_path: Path = koza_transform.input_files_dir / "BindingDB.zip"

        # Directly read and extract useful columns from the original
        # downloaded bindingdb data file, using the 'polars' library.
        df = extract_bindingdb_columns_polars(
            data_archive_path,
            columns=BINDINGDB_COLUMNS,
            target_taxa=tuple(SOURCE_ORGANISM_TO_TAXON_ID_MAPPING.keys())
        )

        # Process publications
        df = process_publications(koza_transform, df)

        # Map curation knowledge sources to supporting_data_ids
        lookup = pl.DataFrame(
            {
                CURATION_DATASOURCE: CURATION_DATA_SOURCE_TO_INFORES_MAPPING.keys(),
                SUPPORTING_DATA_ID: CURATION_DATA_SOURCE_TO_INFORES_MAPPING.values()
            }
        )
        df = df.join(lookup, on=CURATION_DATASOURCE, how="left")

        # Group by unique ligand-target-publication combinations
        # This consolidates duplicate assay records
        # TODO: this operation does yet not aggregate disparate annotation across assays;
        #       Such annotation might need to be so aggregated in future ingest iterations?
        df = df.unique(
            subset=[PUBLICATION, PUBCHEM_CID, UNIPROT_ID],
            keep="last"  # Keep the last occurrence (matches current behavior)
        )

        return df.to_dicts()

    except Exception as e:
        exception_tag = f"{str(type(e))}: {str(e)}"
        koza_transform.transform_metadata[exception_tag] = "Failed to load BindingDb data."
        return None

@koza.transform_record()
def transform_bindingdb_by_record(
        koza_transform: koza.KozaTransform,
        record: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Transform a BindingDb record into a KnowledgeGraph object.
    A basic assumption here is that each record being processed
    represents a single ligand-target interaction (duplicate assays
    having already been consolidated in a "prepare" method step).

    :param koza_transform: The koza.KozaTransform context of the data processing.
    :param record: Individual BindingDb records to be processed.
    :return: KnowledgeGraph object containing nodes and edges for the record.
    """
    try:
        # Nodes

        # TODO: All ligands will be treated as ChemicalEntity, for now,
        #       as a first approximation but we may want to consider
        #       using more specialized classes if suitable discrimination
        #       can eventually be made in between chemical types
        chemical = ChemicalEntity(id="CID:" + record[PUBCHEM_CID])

        # Taxon of protein target
        taxon_label = record[SOURCE_ORGANISM]
        taxon_id = SOURCE_ORGANISM_TO_TAXON_ID_MAPPING.get(taxon_label, None)

        # Unless otherwise advised, all BindingDb targets
        # are assumed to be (UniProt registered) proteins.
        target_name = record[TARGET_NAME]
        protein = Protein(
            id="UniProtKB:" + record[UNIPROT_ID],
            name=target_name,
            in_taxon=[f"NCBITaxon:{taxon_id}"] if taxon_id else None,
            in_taxon_label=taxon_label
        )

        # Publications
        publications = [record[PUBLICATION]]

        # Sources
        supporting_data_id = record[SUPPORTING_DATA_ID]
        supporting_data: Optional[list[str]] = [supporting_data_id] if supporting_data_id else None
        sources = build_association_knowledge_sources(
            primary=(
                "infores:bindingdb",
                [LINK_TO_LIGAND_TARGET_PAIR.format(monomerid=record[MONOMER_ID], enzyme=target_name)]
            ),
            supporting=supporting_data
        )

        # Edge
        association = ChemicalGeneInteractionAssociation(
            id=entity_id(),
            subject=chemical.id,
            predicate="biolink:directly_physically_interacts_with",
            object=protein.id,
            publications=publications,
            sources=sources,
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        )

        return KnowledgeGraph(nodes=[chemical, protein], edges=[association])

    except Exception as e:
        # Tally errors here
        exception_tag = f"{str(type(e))}: {str(e)}"
        rec_id = (f"Ligand:{record.get(PUBCHEM_CID, "Unknown")}->"
                  f"Target:{record.get(UNIPROT_ID, 'Unknown')}"
                  f"[NCBITaxon:{record.get(SOURCE_ORGANISM, 'Unknown')}]")
        if exception_tag not in koza_transform.transform_metadata:
            koza_transform.transform_metadata[exception_tag] = [rec_id]
        else:
            koza_transform.transform_metadata[exception_tag].append(rec_id)
        return None
