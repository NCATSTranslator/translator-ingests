"""
Open Health Data - Carolina ingest parser
(adapted a bit from COHD and HMBD ingests)
"""
from typing import Any, Iterable
from pathlib import Path

from zipfile import ZipFile
import csv

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    Study,
    Association,
    CorrelatedGeneToDiseaseAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum
)

import koza
from translator_ingest.util.biolink import build_association_knowledge_sources
from translator_ingest.util.transform_utils import entity_id
from koza.model.graphs import KnowledgeGraph

from translator_ingest.util.biolink import (
    get_biolink_model_toolkit
)

bmt = get_biolink_model_toolkit()


def get_latest_version() -> str:
    return "2026-06-29"  # Temporary placeholder version

# Subject and object nodes will be duplicated so we cache them
_ohdc_nodes: dict[str, NamedThing] = {}

def _resolve_node(identifier: str, name: str) -> NamedThing:
    if identifier in _ohdc_nodes:
        return _ohdc_nodes[identifier]
    else:
        # TODO: need to be more clever here to resolve node category to something more useful
        node_found = NamedThing(id=identifier, name=name)
        _ohdc_nodes[identifier] = node_found
        return node_found

def _wrap_study(record: dict[str, Any]) -> Study:
    # TODO: Stub implementation
    chi_squared_p_value = record["chi_squared_p_value"],
    log_odds_ratio = record["log_odds_ratio"],
    log_odds_ratio_95_ci = record["log_odds_ratio_95_ci"],
    score = record["score"],
    total_sample_size = record["total_sample_size"]
    return Study(id="infores:openhealthdata-carolina", **{})


@koza.transform()
def transform_ohdc_ingest(
        koza_transform: koza.KozaTransform,
        data: Iterable[dict[str, Any]]
) -> Iterable[KnowledgeGraph]:
    """
    Given that OHD@Carolina is a zip archive wrapping a csv file,
    that we process as a streaming knowledge source design pattern.
    """
    # We actually ignore the input data Iterable,
    # assuming that Koza didn't bother pre-processing
    # the downloaded OHD@Carolina file, leaving it to us here
    if koza_transform.input_files_dir is None:
        raise ValueError("input_files_dir must be set for OHD@Carolina ingest")

    ohdc_data_archive_path: Path = koza_transform.input_files_dir / "unc_omop_2018_2022_kg.zip"

    with ZipFile(ohdc_data_archive_path) as zf:
        # open the OHD@Carolina CSV file
        with open('unc_omop_2018_2022_kg.csv', newline="") as fp:
            reader = csv.DictReader(fp)
            for record in reader:
                subject_node = _resolve_node(record["subject"], record["subject_name"])
                object_node = _resolve_node(record["object"], record["object_name"])

                predicate = record["predicate"]

                study: Study = _wrap_study(record)

                if "gene or gene product" in bmt.get_ancestors(subject_node.category[0]) and \
                        object_node.category[0] == "biolink:Disease":
                    association = CorrelatedGeneToDiseaseAssociation
                else:
                    association = Association

                association = association(
                    id=entity_id(),
                    subject=subject_node.id,
                    predicate=predicate,
                    object=object_node.id,
                    has_supporting_studies={"": study},
                    sources=build_association_knowledge_sources(primary="infores:openhealthdata-carolina"),
                    knowledge_level=KnowledgeLevelEnum.statistical_association,
                    agent_type=AgentTypeEnum.data_analysis_pipeline,
                    **{}
                )

                yield KnowledgeGraph(nodes=[subject_node,object_node], edges=[association])
