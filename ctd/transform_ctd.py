#!/usr/bin/env python3.12

import argparse
import itertools
import pandas
import ingest_utils as iu
from biolink_model.datamodel.pydanticmodel_v2 import (ChemicalEntity,
                                                      Disease,
                                                      ChemicalToDiseaseOrPhenotypicFeatureAssociation,
                                                      KnowledgeLevelEnum,
                                                      AgentTypeEnum)

BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT = "biolink:treats_or_applied_or_studied_to_treat"
INFORES_CTD = 'infores:ctd'

# C-style typedefs here
DataFrame = pandas.DataFrame
TextFileReader = pandas.io.parsers.readers.TextFileReader


def get_args() -> argparse.Namespace:
    arg_parser = argparse.ArgumentParser(description='ingest parser for CTD chemicals-diseases TSV file')
    arg_parser.add_argument('source_url',
                            default="https://ctdbase.org/reports/CTD_chemicals_diseases.tsv.gz")
    arg_parser.add_argument('output_nodes_filename',
                            default='ctd-output-nodes.jsonl')
    arg_parser.add_argument('output_edges_filename',
                            default='ctd-output-edges.jsonl')
    return arg_parser.parse_args()


def read_and_filter_ctd(source_url: str) -> pandas.DataFrame:
    return (pandas.read_csv(source_url,
                            header=27,
                            delimiter="\t")
            .pipe(iu.make_fix_comment_in_column_name('#'))
            .pipe(iu.make_mutator_filter("DirectEvidence", "therapeutic")))


def get_nodes(ctd_df: pandas.DataFrame) -> tuple[ChemicalEntity | Disease]:
    return tuple(itertools.chain.from_iterable(map(lambda args: iu.make_nodes(ctd_df, *args),
                                                   (("ChemicalID", "ChemicalName", ChemicalEntity),
                                                    ("DiseaseID", "DiseaseName", Disease)))))


def get_edges(ctd_df: pandas.DataFrame) -> tuple[ChemicalToDiseaseOrPhenotypicFeatureAssociation]:
    return iu.make_assocs(ctd_df,
                          "ChemicalID",
                          "DiseaseID",
                          "PubMedIDs",
                          {'predicate': BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT,
                           'primary_knowledge_source': INFORES_CTD,
                           'knowledge_level': KnowledgeLevelEnum.knowledge_assertion,
                           'agent_type': AgentTypeEnum.manual_agent},
                          (iu.make_tx_col_func("PubMedIDs",
                                               lambda ps: tuple(map(lambda p: 'PMID:' + p, ps.split("|")))),),
                          ChemicalToDiseaseOrPhenotypicFeatureAssociation)


def transform_and_write_jsonl(ctd_df: pandas.DataFrame,
                              output_nodes_filename: str,
                              output_edges_filename: str):
    map(lambda f, fn: iu.save_to_jsonl(f(ctd_df)),
        ((get_nodes, output_nodes_filename),
         (get_edges, output_edges_filename)))

    
def main(source_url: str,
         output_nodes_filename: str,
         output_edges_filename: str):
    
    transform_and_write_jsonl(read_and_filter_ctd(source_url),
                              output_nodes_filename,
                              output_edges_filename)



if __name__ == "__main__":
    main(**iu.namespace_to_dict(get_args()))
    
