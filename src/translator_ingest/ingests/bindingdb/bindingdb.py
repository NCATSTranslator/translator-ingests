from typing import Any, Iterable
from datetime import datetime

import koza
# from biolink_model.datamodel.pydanticmodel_v2 import (
#     NamedThing,
#     Association,
#     ChemicalEntity,
#     ChemicalToDiseaseOrPhenotypicFeatureAssociation,
#     Disease,
#     KnowledgeLevelEnum,
#     AgentTypeEnum
# )
# from translator_ingest.util.biolink import entity_id, build_association_knowledge_sources
from koza.model.graphs import KnowledgeGraph

BASE_LINK_TO_MONOMER: str = "http://www.bindingdb.org/bind/chemsearch/marvin/MolStructure.jsp?monomerid={monomerid}"
BASE_LINK_TO_TARGET: str = ("http://www.bindingdb.org/rwd/jsp/dbsearch/PrimarySearch_ki.jsp"
                            "?energyterm=kJ/mole"
                            "&tag=com"
                            "&complexid=56"
                            "&target={target}"
                            "&column=ki&startPg=0&Increment=50&submit=Search")

LINK_TO_LIGAND_TARGET_PAIR: str = (
    "http://www.bindingdb.org/rwd/jsp/dbsearch/PrimarySearch_ki.jsp"
    "?energyterm=kJ/mole&tag=r21&monomerid={monomerid}"
    "&enzyme={enzyme}"
    "&column=ki&startPg=0&Increment=50&submit=Search"
)


def get_latest_version() -> str:
    # According to the BindingDb team, a fresh year+month date-stamped release
    # of BindingDb data is made at the start of each month,
    # so we use the heuristic of a date function to return
    # this candidate 'latest release' value.
    return datetime.today().strftime("%Y%m")


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
    is grouped by identical ligand-target pairs from a single experimental project (i.e., publication)

    :param koza_transform: The koza.KozaTransform context of the data processing.
    :param data: Iterable[dict[str, Any]], Original BindingDb records
    :return: Iterable[dict[str, Any]], Consolidation of related assay records
                                       for each unique ligand-target pair, with possible aggregation
                                       of distinct annotation encountered across the original set of assays.
    """
    # do database stuff:
    # import sqlite3
    # con = sqlite3.connect("example.db")
    # con.row_factory = sqlite3.Row
    # cur = con.cursor()
    # cur.execute("SELECT * FROM example_table")
    # records = cursor.fetchall()
    # for record in records:
    #     yield record
    # con.close()
    # TODO: stub NOOP, implementation to be fixed
    return data


@koza.transform_record()
def transform_ingest_by_record(
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
    :return: KnowledgeGraph object containing nodes and edges for the record
             ('None' if the record parsing was unsuccessful for some reason).
    """

    # # here is an example of skipping a record based off of some condition
    # publications = [f"PMID:{p}" for p in record["PubMedIDs"].split("|")] if record["PubMedIDs"] else None
    # if not publications:
    #     koza_transform.log(f"No pubmed IDs found for {record['PubMedIDs']}")
    #     koza_transform.state["example_counter"] += 1
    #     return None
    # else:
    #     koza_transform.log(f" pubmed IDs found for {record['PubMedIDs']}")
    #
    # chemical = ChemicalEntity(id="MESH:" + record["ChemicalID"], name=record["ChemicalName"])
    # disease = Disease(id=record["DiseaseID"], name=record["DiseaseName"])
    # association = ChemicalToDiseaseOrPhenotypicFeatureAssociation(
    #     id=entity_id(),
    #     subject=chemical.id,
    #     predicate="biolink:related_to",
    #     object=disease.id,
    #     publications=publications,
    #     sources=build_association_knowledge_sources(primary=INFORES_CTD),
    #     knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
    #     agent_type=AgentTypeEnum.manual_agent,
    # )
    # return KnowledgeGraph(nodes=[chemical, disease], edges=[association])
    # TODO: stub NOOP, implementation to be fixed
    return KnowledgeGraph()
