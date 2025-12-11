## FROM template, modified for this ingest
import koza
import pandas as pd
from typing import Any, Iterable
from koza.model.graphs import KnowledgeGraph
from bmt.pydantic import entity_id, build_association_knowledge_sources
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    Gene,    ## because terms/IDs dgidb gives are for genes
    ChemicalAffectsGeneAssociation,    ## ONLY for affects
    ChemicalGeneInteractionAssociation,    ## ONLY for interacts_with
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
## import from mapping file
from translator_ingest.ingests.dgidb.mappings import (
    supporting_data_sources,
    publications,
    int_type_mapping,
)
from translator_ingest.util.biolink import INFORES_DGIDB
from translator_ingest.util.http_utils import get_modify_date


## HARD-CODED VALUES, see mapping.py for more
## drug ID namespaces in data that NodeNorm currently doesn't recognize
PREFIXES_TO_DROP = [
    ## . probably will be treated as "all match"...unless escaped
    "ncit",   ## not recognized as chemicals by NodeNorm
    "iuphar\\.ligand",
    "wikidata",
    "hemonc",
    "drugsatfda\\.nda",
    "chemidplus",
]
## interaction_types that map to plain "interacts_with" edge (no qualifiers, extra edge)
## "~NULL" is a placeholder for NA, see prepare_data for details
plain_interact_types = {"other/unknown", "~NULL"}
BIOLINK_INTERACTS = "biolink:interacts_with"
## columns for drug-gene pair
DRUG_GENE_COLS = ["drug_concept_id", "gene_concept_id"]


## PIPELINE MAIN FUNCTIONS

def get_latest_version() -> str:
    ## Needs to be manually updated when we update what file we're using
    ## ...unless we can read the downloaded file during this step. Then we can get the version info from the first few lines (header)
    return get_modify_date("https://dgidb.org/data/2024-Dec/interactions.tsv")


@koza.prepare_data()
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:
    df = pd.DataFrame.from_records(data)
    ## data was loaded with empty values = "". Just in case, replace these empty strings with None so na methods will work
    df.replace(to_replace="", value=None, inplace=True)
    ## for debugging
    # print(df[df["gene_concept_id"].notna()].shape)
    # print(df[df["drug_concept_id"].notna()].shape)
    # print(df[df["interaction_source_db_name"].isna()].shape)

    ## log, drop rows with NA (no value) in gene ID OR drug ID (default dropna behavior)
    n_have_values = df.dropna(subset=["gene_concept_id", "drug_concept_id"]).shape[0]
    koza.log(f"{n_have_values} rows ({n_have_values / df.shape[0]:.1%}) kept (have both entity IDs)")
    df.dropna(subset=["gene_concept_id", "drug_concept_id"], ignore_index=True, inplace=True)

    ## remove rows with drug IDs from namespaces that NodeNorm currently doesn't recognize
    n_before = df.shape[0]    ## save for log: calculating change
    df["drug_prefix"] = [i.split(":")[0] for i in df["drug_concept_id"]]
    ## set case=False so it isn't case-sensitive on matches!
    df = df[~df.drug_prefix.str.contains('|'.join(PREFIXES_TO_DROP), case=False)].copy()
    koza.log(f"{df.shape[0]} rows ({df.shape[0] / n_before:.1%}) after filtering out drug namespaces that can't be NodeNormed")
    ## remove prefix column, not needed anymore
    df.drop("drug_prefix", axis=1, inplace=True)

    ## change ID prefixes to Translator standard: mostly making all upper-case
    df["drug_concept_id"] = df["drug_concept_id"].str.upper()
    df["gene_concept_id"] = df["gene_concept_id"].str.upper()
    ## special handling for some prefixes: CHEMBL, NCBIGENE
    df["drug_concept_id"] = df["drug_concept_id"].str.replace("CHEMBL:", "CHEMBL.COMPOUND:")
    df["gene_concept_id"] = df["gene_concept_id"].str.replace("NCBIGENE:", "NCBIGene:")

    ## clean up interaction_type values
    ## first replace NA with "~NULL" (will be at end alphabetically). then can use string methods on column
    df["interaction_types"] = df["interaction_types"].fillna("~NULL")
    ## some values are "|"-delimited. Want to split into separate rows
    df["interaction_types"] = df["interaction_types"].str.split("|")
    df = df.explode("interaction_types", ignore_index=True)
    koza.log(f"{df.shape[0]} rows after expanding rows with multiple interaction_type values")
    ## make new relationship-type column: mod_type
    ## where interaction_types values with the same data-modeling are set to the same value
    ##   currently, multiple values map to plain "interacts_with" edge modeling
    df["mod_type"] = ["~PLAIN_INTERACTS" if i in plain_interact_types else i for i in df["interaction_types"]]
    ## (keeping original column interaction_types for trouble-shooting, maybe future use (original predicates?))
    ## take int_type_mapping and add this value
    int_type_mapping.update({
        "~PLAIN_INTERACTS": {
            "predicate": BIOLINK_INTERACTS,
            ## create empty qualifier dict, so assigning to association later is easier. Will error if the ** is set to None
            "qualifiers": {},
        }
    })

    ## group-by/merge rows by unique drug ID, gene ID, mod_type combo
    ## then each row == 1 Translator edge
    COLS_DEFINE_EDGE = ["drug_concept_id", "gene_concept_id", "mod_type"]
    df = df.groupby(by=COLS_DEFINE_EDGE).agg(
        {
            "interaction_types": set, 
            "interaction_source_db_name": set,
            "interaction_score": "first",
            "evidence_score": "first",
            "drug_name": "first",
            "gene_name": "first",

        }
    ).reset_index().copy()
    koza.log(f"{df.shape[0]} rows after merging by unique drug ID, gene ID, mod_type combo")
    koza.log(f"{df[df["mod_type"] == "~PLAIN_INTERACTS"].shape[0]} rows that map to plain 'interacts_with' edges")

    ## SPECIAL sources logic: for plain "interacts_with" edges, include ALL sources for drug-gene pair
    ## first create a mapping df: group-by drug-gene pair -> get set of all sources
    drug_gene_sources = df.copy()
    ## need to merge sets!
    ## and keep multi-index, annoying but seems easier to retrieve values later
    drug_gene_sources = drug_gene_sources.groupby(by=DRUG_GENE_COLS).agg(
        {"interaction_source_db_name": lambda x: set.union(*x)}
    )
    ## currently overwrite sources column
    df["interaction_source_db_name"] = [
        drug_gene_sources.loc[x.drug_concept_id, x.gene_concept_id].to_list()[0] if x.mod_type == "~PLAIN_INTERACTS"
        else x.interaction_source_db_name
        for x in df[["drug_concept_id", "gene_concept_id", "mod_type", "interaction_source_db_name"]].itertuples()
    ]

    ## SPECIAL scores logic: remove scores from rows that aren't plain "interacts_with" IF there's > 1 row (mod_type) for a drug-gene pair
    grp = df.groupby(by=DRUG_GENE_COLS)
    for name, group in grp:
        if group.shape[0] > 1:
            for idx,row in group.iterrows():
                if row.mod_type != "~PLAIN_INTERACTS":
                    df.at[idx, "interaction_score"] = pd.NA
                    df.at[idx, "evidence_score"] = pd.NA
    koza.log(f"Removed {df["interaction_score"].isna().sum()} scores from dataframe after special logic")

    ## return updated dataset
    return df.to_dict(orient="records")


@koza.transform_record()
def transform_row(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    ## processing source values (already deduplicated, since it's a set)
    record_support_infores = list()
    record_pubs = list()
    for i in record["interaction_source_db_name"]:
        if i in supporting_data_sources.keys():
            record_support_infores.append(supporting_data_sources[i])
        elif i in publications.keys():
            record_pubs.append(publications[i])
    ## if publications is an empty list, make it None so pipeline will remove from properties (it handles empty supporting list okay) 
    if len(record_pubs) == 0:
        record_pubs = None

    ## Nodes
    chemical = ChemicalEntity(id=record["drug_concept_id"])
    gene = Gene(id=record["gene_concept_id"])

    ## diff Association type depending on predicate
    data_modeling = int_type_mapping[record["mod_type"]]

    if "interacts_with" in data_modeling["predicate"]:
    ## covers "interacts_with" and descendants with substring
    ## ASSUMING no special logic, so only 1 edge made
    ## NOTE: sometimes supporting sources, publications, scores will be empty (None, empty list). Then don't want them present in output
        association = ChemicalGeneInteractionAssociation(
            id=entity_id(),
            subject=chemical.id,
            object=gene.id,
            ## KL/AT is for dgidb
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.automated_agent,
            sources=build_association_knowledge_sources(primary=INFORES_DGIDB, supporting=record_support_infores),
            publications=record_pubs,
            dgidb_interaction_score=record["interaction_score"],
            ## currently, becomes int without issues
            dgidb_evidence_score=record["evidence_score"],
            predicate=data_modeling["predicate"],
            **data_modeling.get("qualifiers")
        )
        return KnowledgeGraph(nodes=[chemical, gene], edges=[association])
    elif "affects" in data_modeling["predicate"]:
    ## currently covers all other cases
    ## NOTE: sometimes supporting sources, publications, scores will be empty (None, empty list). Then don't want them present in output
        ## MAIN EDGE
        association = ChemicalAffectsGeneAssociation(
            id=entity_id(),
            subject=chemical.id,
            object=gene.id,
            ## KL/AT is for dgidb
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.automated_agent,
            sources=build_association_knowledge_sources(primary=INFORES_DGIDB, supporting=record_support_infores),
            publications=record_pubs,
            dgidb_interaction_score=record["interaction_score"],
            ## currently, becomes int without issues
            dgidb_evidence_score=record["evidence_score"],
            predicate=data_modeling["predicate"],
            ## currently, there are always qualifiers
            **data_modeling.get("qualifiers")
        )
        ## if there's an extra edge field
        if data_modeling.get("extra_edge_pred"):
            ## SPECIAL logic: create extra "physical interaction" edge for some "affects" edges
            ## should be identical to original edge, except predicate/no qualifiers. And CX decided not to include dgidb scores
            extra_assoc = ChemicalGeneInteractionAssociation(
                id=entity_id(),
                subject=chemical.id,
                object=gene.id,
                ## KL/AT is for dgidb
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.automated_agent,
                sources=build_association_knowledge_sources(primary=INFORES_DGIDB, supporting=record_support_infores),
                publications=record_pubs,
                predicate=data_modeling["extra_edge_pred"],
            )
            ## return both edges
            return KnowledgeGraph(nodes=[chemical, gene], edges=[association, extra_assoc])
        else:
            ## return only 1 edge
            return KnowledgeGraph(nodes=[chemical, gene], edges=[association])
