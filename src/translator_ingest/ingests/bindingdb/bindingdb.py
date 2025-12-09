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
    get_bindingdb_input_file,
    extract_bindingdb_columns_polars,
    DATASOURCE_TO_IDENTIFIER_MAPPING,
    LINK_TO_LIGAND_TARGET_PAIR,
    MONOMER_ID, LIGAND_NAME, TARGET_NAME,
    CURATION_DATASOURCE, ARTICLE_DOI, PMID, PATENT_ID,
    PUBMED_CID, UNIPROT_ID, PUBLICATION, SUPPORTING_DATA_ID
)

# This is the local filename of the BindingDb data file
# as specified in the ingest download.yaml
BINDINGDB_INPUT_FILE = "BindingDB_All_current_tsv.zip"


def get_latest_version() -> str:
    # According to the BindingDb team,
    # a fresh year+month date-stamped release
    # of BindingDb data is made at the start of each month,
    # so we use the heuristic of a date function to return
    # this candidate 'latest release' value.
    return datetime.today().strftime("%Y%m")

#
# Code now embedded in the prepare_bindingdb_data method below.
#
# @koza.on_data_begin()
# def on_bindingdb_data_begin(koza_transform: koza.KozaTransform) -> None:
#     koza_transform.transform_metadata["ingest_by_record"] = {"rows_missing_publications": 0}
#
#
# Utility function used in the original prepare_bindingdb_data method below.
# 
# def _get_publication(koza_transform: koza.KozaTransform, data: dict[str, Any]) -> Optional[str]:
#     """
#     Export the best record publication here, based on PMID > Patent ID > Article DOI
# 
#     :param koza_transform: The koza.KozaTransform context of the data processing.
#     :param data: Iterable[dict[str, Any]], Original BindingDb records
#     :return: Best publication CURIE or None if not available
#     """
#     publication: Optional[str] = None
#     if data:
#         # Precedence is PMID > Patent ID > Article DOI
#         if data[PMID]:
#             publication = f"PMID:{data[PMID]}"
#         elif data[PATENT_ID]:
#             publication = f"uspto-patent:{data[PATENT_ID].replace('US', "")}"
#         elif data[ARTICLE_DOI]:
#             publication = f"doi:{data[ARTICLE_DOI]}"
#         else:
#             koza_transform.log(f"No publication found for {data[BINDING_ENTRY_ID]}")
#             koza_transform.transform_metadata["ingest_by_record"]["rows_missing_publications"] += 1
# 
#     return publication

@koza.prepare_data()
def prepare_bindingdb_data(
        koza_transform: koza.KozaTransform,
        data: Iterable[dict[str, Any]]
) -> Iterable[dict[str, Any]] | None:
    """
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
    :param data: Iterable[dict[str, Any]], Original BindingDb records

    :return: Iterable[dict[str, Any]], Consolidation of related assay records
             for each unique ligand-target pair, with possible aggregation
             of distinct annotation encountered across the original set of assays.
    """
    #
    # Original
    # output_for_publication: Optional[dict[str, Any]] = None
    # current_output: Optional[dict[str, Any]] = None
    #
    # it = iter(data)
    #
    # while True:
    #     current_record = next(it, None)
    #     if current_record is not None:
    #         # We have a new record to process...
    #         current_record[PUBLICATION] = _get_publication(koza_transform, current_record)
    #         if not current_record[PUBLICATION]:
    #             # We can't publish this record without
    #             # a publication CURIE, so we skip it
    #             continue
    #
    #         current_record[SUPPORTING_DATA_ID] = \
    #             DATASOURCE_TO_IDENTIFIER_MAPPING.get(current_record[DATASOURCE], None)
    #
    #         if (    # we check our boundary conditions for publishing the
    #                 # last aggregated record, then starting a new collection
    #                 not current_output
    #                 or current_record[PUBLICATION] != current_output[PUBLICATION]
    #                 or current_record[PUBMED_CID] != current_output[PUBMED_CID]
    #                 or current_record[UNIPROT_ID] != current_output[UNIPROT_ID]
    #         ):
    #             # The "current_output" could still be None if we are just starting out.
    #             output_for_publication = current_output
    #             current_output = current_record.copy()
    #         else:
    #             # This is a bit of a dodgy shortcut
    #             # to collect 'new' edge annotations, simply because
    #             # some of the data collected from earlier matching
    #             # SPO+publications records is likely being overwritten.
    #             # Note: probably need to devise a more robust approach to aggregate multiple records
    #             current_output.update(current_record)
    #
    #     else:
    #         # No more records. If there were no data in the first place,
    #         # then the "current_output" could still be None here, but just in case
    #         # we did collect something already, then we should publish it.
    #         output_for_publication = current_output
    #
    #     # Publish the latest content seen...
    #     if output_for_publication is not None:
    #         yield output_for_publication
    #         output_for_publication = None
    #
    #     if not current_record:
    #         # we've reached the end of the data stream
    #         # with all data published, so we can stop processing
    #         return
    #

    koza_transform.transform_metadata["ingest_by_record"] = {"rows_missing_publications": 0}

    # Directly read and extract useful columns from the original
    # downloaded bindingdb data file, using the 'polars' library.
    bindingdb_data_path: Path = koza_transform.input_files_dir / get_bindingdb_input_file()
    df = extract_bindingdb_columns_polars(file_path=str(bindingdb_data_path))

    # Add the publication column (same logic as the current implementation)
    df = df.with_columns([
        pl.when(pl.col(PMID).is_not_null())
        .then(pl.concat_str([pl.lit("PMID:"), pl.col(PMID)]))
        .when(pl.col(PATENT_ID).is_not_null())
        .then(
            pl.concat_str([
                pl.lit("uspto-patent:"),
                pl.col(PATENT_ID).str.replace("US", "")
            ])
        )
        .when(pl.col(ARTICLE_DOI).is_not_null())
        .then(pl.concat_str([pl.lit("doi:"), pl.col(ARTICLE_DOI)]))
        .otherwise(None)
        .alias(PUBLICATION)
    ])

    # Count rows without publications
    rows_missing_pubs = df.filter(pl.col(PUBLICATION).is_null()).height
    koza_transform.transform_metadata["ingest_by_record"]["rows_missing_publications"] = rows_missing_pubs

    # Filter out rows without publications
    df = df.filter(pl.col(PUBLICATION).is_not_null())

    df = df.with_columns([
        pl.col(CURATION_DATASOURCE)
        .replace(DATASOURCE_TO_IDENTIFIER_MAPPING, default=None)
        .alias(SUPPORTING_DATA_ID)
    ])

    # Group by unique ligand-target-publication combinations
    # This consolidates duplicate assay records
    df = df.unique(
        subset=[PUBLICATION, PUBMED_CID, UNIPROT_ID],
        keep="last"  # Keep the last occurrence (matches current behavior)
    )

    return df.to_dicts()


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
    publications = [record[PUBLICATION]]

    # TODO: All ligands will be treated as ChemicalEntity, for now,
    #       as a first approximation but we may want to consider
    #       using more specialized classes if suitable discrimination
    #       can eventually be made in between chemical types
    chemical = ChemicalEntity(id="CID:" + record[PUBMED_CID], name=record[LIGAND_NAME])

    # Unless otherwise advised, all BindingDb targets are assumed to be (UniProt registered) proteins.
    target_name = record[TARGET_NAME]
    protein = Protein(id="UniProtKB:" + record[UNIPROT_ID], name=target_name)

    supporting_data_id = record[SUPPORTING_DATA_ID]
    supporting_data: Optional[list[str]] = [supporting_data_id] if supporting_data_id else None
    sources = build_association_knowledge_sources(
        primary=(
            "infores:bindingdb",
            [LINK_TO_LIGAND_TARGET_PAIR.format(monomerid=record[MONOMER_ID], enzyme=target_name)]
        ),
        supporting=supporting_data
    )
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
