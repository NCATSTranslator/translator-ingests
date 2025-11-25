from typing import Optional, Any, Iterable
from datetime import datetime

import koza
from anyio import current_time
from biolink_model.datamodel.model import UNIPROT_ISOFORM
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalGeneInteractionAssociation,
    Protein,
    KnowledgeLevelEnum,
    AgentTypeEnum
)
from translator_ingest.util.biolink import entity_id, build_association_knowledge_sources
from koza.model.graphs import KnowledgeGraph

#
# Core BindingDb Record Field Name Keys - currently ignored fields commented out
#
BINDING_ENTRY_ID = "BindingDB Reactant_set_id"
# "Ligand SMILES" = "OC(=O)C[C@H](NC(=O)c1ccc(CNS(=O)(=O)c2ccc(O)c(c2)C(O)=O)cc1)C=O",
# "Ligand InChI" = "InChI=1S/C19H18N2O9S/c22-10-13(7-17(24)25)21-18(26)12-3-1-11(2-4-12)9-20-31(29,30)14-5-6-16(23)15(8-14)19(27)28/h1-6,8,10,13,20,23H,7,9H2,(H,21,26)(H,24,25)(H,27,28)",
# "Ligand InChI Key" = "FMTZTJFXNZOBIQ-UHFFFAOYSA-N",
MONOMER_ID = "BindingDB MonomerID"
LIGAND_NAME = "BindingDB Ligand Name"
TARGET_NAME = "Target Name"
# "Target Source Organism According to Curator or DataSource" = "Homo sapiens",
# KI = "Ki (nM)"
# IC50 = "IC50 (nM)"
# KD = "Kd (nM)"
# EC50 = "EC50 (nM)"
# KON = "kon (M-1-s-1)"
# KOFF = "koff (s-1)"
# "pH" = "7.4",
# "Temp (C)" = "25.00",
DATASOURCE = "Curation/DataSource"
ARTICLE_DOI = "Article DOI"
# "BindingDB Entry DOI" = "10.7270/Q2B56GW5",
PMID = "PMID"
PATENT_ID = "Patent Number"
PUBMED_CID = "PubChem CID"
# "PubChem SID" = "8030145",
# "ChEBI ID of Ligand" = "",
# "ChEMBL ID of Ligand" = "",
UNIPROT_ID = "UniProt (SwissProt) Primary ID of Target Chain 1"
# "UniProt (SwissProt) Recommended Name of Target Chain 1" = "Caspase-1",

# We don't need these yet...
# BASE_LINK_TO_MONOMER: str = "http://www.bindingdb.org/bind/chemsearch/marvin/MolStructure.jsp?monomerid={monomerid}"
# BASE_LINK_TO_TARGET: str = ("http://www.bindingdb.org/rwd/jsp/dbsearch/PrimarySearch_ki.jsp"
#                             "?energyterm=kJ/mole"
#                             "&tag=com"
#                             "&complexid=56"
#                             "&target={target}"
#                             "&column=ki&startPg=0&Increment=50&submit=Search")

# ...but would like to use this to publish the source_record_urls for the
#    BindindDb primary_knowledge_source RetrievalSource provenance metadata.
LINK_TO_LIGAND_TARGET_PAIR: str = (
    "http://www.bindingdb.org/rwd/jsp/dbsearch/PrimarySearch_ki.jsp"
    "?energyterm=kJ/mole&tag=r21&monomerid={monomerid}"
    "&enzyme={enzyme}"
    "&column=ki&startPg=0&Increment=50&submit=Search"
)


def get_latest_version() -> str:
    # According to the BindingDb team,
    # a fresh year+month date-stamped release
    # of BindingDb data is made at the start of each month,
    # so we use the heuristic of a date function to return
    # this candidate 'latest release' value.
    return datetime.today().strftime("%Y%m")

@koza.on_data_begin()
def on_bindingdb_data_begin(koza_transform: koza.KozaTransform) -> None:
    koza_transform.transform_metadata["ingest_by_record"] = {"rows_missing_publications": 0}

DATASOURCE_TO_IDENTIFIER_MAPPING = {
    "CSAR": "infores:community-sar",
    "ChEMBL": "infores:chembl",
    "D3R": "infores:drug-design",
    "PDSP Ki": "infores:ki-database",
    "PubChem": "infores:pubchem",
    "Taylor Research Group, UCSD": "infores:taylor-research-group-ucsd",
    "US Patent": "infores:uspto-patent"
}

def _get_publication(koza_transform: koza.KozaTransform, data: dict[str, Any]) -> Optional[str]:
    """
    Export the best record publication here, based on PMID > Patent ID > Article DOI

    :param koza_transform: The koza.KozaTransform context of the data processing.
    :param data: Iterable[dict[str, Any]], Original BindingDb records
    :return: Best publication CURIE or None if not available
    """
    publication: Optional[str] = None
    if data:
        if data[PMID]:
            publication = f"PMID:{data[PMID]}"
        elif data[PATENT_ID]:
            publication = f"uspto-patent:{data[PATENT_ID].replace('US', "")}"
        elif data[ARTICLE_DOI]:
            publication = f"doi:{data[ARTICLE_DOI]}"
        else:
            koza_transform.log(f"No publication found for {data[BINDING_ENTRY_ID]}")
            koza_transform.transform_metadata["ingest_by_record"]["rows_missing_publications"] += 1

    return publication

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

    For the time being, we blissfully make an assumption that the data from the iterable
    is aggregated by in blocks from a single experimental project (i.e., publication),
    then grouped by sets of assays for the same ligand-target pair.

    :param koza_transform: The koza.KozaTransform context of the data processing.
    :param data: Iterable[dict[str, Any]], Original BindingDb records

    :return: Iterable[dict[str, Any]], Consolidation of related assay records
             for each unique ligand-target pair, with possible aggregation
             of distinct annotation encountered across the original set of assays.
    """
    output_for_publication: Optional[dict[str, Any]] = None
    current_output: Optional[dict[str, Any]] = None

    it = iter(data)

    while True:
        current_record = next(it, None)
        if current_record is not None:
            # We have a new record to process...
            current_record["publication"] = _get_publication(koza_transform, current_record)
            if not current_record["publication"]:
                # We can't publish this record without
                # a publication CURIE, so we skip it
                continue

            current_record["supporting_data_id"] = \
                DATASOURCE_TO_IDENTIFIER_MAPPING.get(current_record[DATASOURCE], None)

            if (    # we check our boundary conditions for publishing the
                    # last aggregated record, then starting a new collection
                    not current_output
                    or current_record["publication"] != current_output["publication"]
                    or current_record[PUBMED_CID] != current_output[PUBMED_CID]
                    or current_record[UNIPROT_ID] != current_output[UNIPROT_ID]
            ):
                # The "current_output" could still be None if we are just starting out.
                output_for_publication = current_output
                current_output = current_record.copy()
            else:
                # This is a bit of a dodgy shortcut
                # to collect 'new' edge annotations, simply because
                # some of the data collected from earlier matching
                # SPO+publications records is likely being overwritten.
                # TODO: devise a more robust approach to aggregate multiple records
                current_output.update(current_record)

        else:
            # No more records. If there were no data in the first place,
            # then the "current_output" could still be None here, but just in case
            # we did collect something already, then we should publish it.
            output_for_publication = current_output

        # Publish the latest content seen...
        if output_for_publication is not None:
            yield output_for_publication
            output_for_publication = None

        if not current_record:
            # we've reached the end of the data stream
            # with all data published, so we can stop processing
            return


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
    publications = [record['publication']]

    # TODO: All ligands will be treated as ChemicalEntity, for now,
    #       as a first approximation but we may want to consider
    #       using more specialized classes if suitable discrimination
    #       can eventually be made in between chemical types
    chemical = ChemicalEntity(id="CID:" + record[PUBMED_CID], name=record[LIGAND_NAME])

    # Unless otherwise advised, all BindingDb targets are assumed to be (UniProt registered) proteins.
    target_name = record[TARGET_NAME]
    protein = Protein(id="UniProtKB:" + record[UNIPROT_ID], name=target_name)

    supporting_data_id = record["supporting_data_id"]
    supporting_data: Optional[list[str]] = [supporting_data_id] if supporting_data_id else None
    sources = build_association_knowledge_sources(
        primary="infores:bindingdb",

        # TODO: source_record_urls needs implementation in "build_association_knowledge_sources()"
        # source_record_urls=LINK_TO_LIGAND_TARGET_PAIR.format(monomerid=record[MONOMER_ID], enzyme=target_name),

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
