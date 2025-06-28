#!/usr/bin/env python3.12

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


ctd_df = (pandas.read_csv("https://ctdbase.org/reports/CTD_chemicals_diseases.tsv.gz",
                          header=27,
                          delimiter="\t")
          .pipe(iu.make_fix_comment_in_column_name('#'))
          .pipe(iu.make_mutator_filter("DirectEvidence", "therapeutic")))

nodes = sum(map(lambda args: iu.make_nodes(ctd_df, *args),
                (("ChemicalID", "ChemicalName", ChemicalEntity),
                 ("DiseaseID", "DiseaseName", Disease))), ())

iu.save_to_jsonl(nodes,
                 'ctd-output-nodes.jsonl')

associations = iu.make_assocs(ctd_df,
                              "ChemicalID",
                              "DiseaseID",
                              "PubMedIDs",
                              {'predicate': BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT,
                               'primary_knowledge_source': INFORES_CTD,
                               'knowledge_level': KnowledgeLevelEnum.knowledge_assertion,
                               'agent_type': AgentTypeEnum.manual_agent},
                              (iu.make_tx_col_func("PubMedIDs",
                                                   lambda p: tuple(p.split("|"))),),
                              ChemicalToDiseaseOrPhenotypicFeatureAssociation)

iu.save_to_jsonl(associations,
                 'ctd-output-edges.jsonl')
