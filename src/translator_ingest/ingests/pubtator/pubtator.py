## FROM template, modified for this ingest
import koza
from typing import Any, Iterable
from koza.model.graphs import KnowledgeGraph
from translator_ingest.util.transform_utils import entity_id
## DRAFT: UPDATE THIS for transform
from translator_ingest.util.biolink import INFORES_PUBTATOR, build_association_knowledge_sources
from translator_ingest.util.http_utils import get_modify_date
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    DiseaseOrPhenotypicFeature,  ## using just in case: NodeNorm does use MESH IDs for some phenos
    Gene,
    Association,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
## DRAFT: UPDATE THIS after doing mapping
## import from mapping file
from translator_ingest.ingests.pubtator.mappings import (
    RELATION_MODELING
)
## ADDED packages for this ingest
import polars as pl


## HARD-CODED VALUES, see mapping.py for more
## based on README. Table doesn't include column names
STARTING_COLS = ["pmid", "relation", "entity1", "entity2"]
SPLIT_COLS = [
    "pmid",
    "relation",
    "entity1_type",
    "entity1_id",
    "entity2_type",
    "entity2_id",
]
DEFINE_EDGE_COLS = ["entity1_id", "relation", "entity2_id"]
## NodeNorm currently doesn't include variant namespaces, so filtering out
EXCLUDED_ENTITY_TYPES = ["DNAMutation", "ProteinMutation", "SNP", "Mutation"]
## not a useful relationship or no biolink-model mapping
EXCLUDED_RELATIONS = ["compare", "cotreat"]
EXCLUDED_ENTITIES = [
    "MESH:C100843",  ## Lacteol: Pubtator classifies as Chemical, NodeNormed to OrganismTaxon
    "MESH:C000598555",  ## 2,5-dihexyl-N,N'-dicyano-p-quinonediimine: Pubtator classifies as Chemical, NodeNormed to OrganismTaxon
    "MESH:C000719328",  ## smoker's inclusion bodies: Pubtator classifies as Disease, NodeNormed CORRECTLY to CellularComponent
]
# Build the sources property for use on edges
PUBTATOR_SOURCES = build_association_knowledge_sources(primary=INFORES_PUBTATOR)


## CUSTOM FUNCTIONS
def get_node(pubtator_type: str, input_id: str):
    if pubtator_type == "Chemical":
        return ChemicalEntity(id=input_id)
    elif pubtator_type == "Disease":
        return DiseaseOrPhenotypicFeature(id=input_id)
    ## using elif just in case
    elif pubtator_type == "Gene":
        return Gene(id=f"NCBIGene:{input_id}")


## PIPELINE MAIN FUNCTIONS

def get_latest_version() -> str:
    return get_modify_date("https://ftp.ncbi.nlm.nih.gov/pub/lu/PubTator3/relation2pubtator3.gz")


@koza.prepare_data()
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:
    relation_path = f"{koza.input_files_dir}/relation2pubtator3.gz"  ## path to downloaded file

    df = (
        pl.read_csv(
            relation_path,  ## polars can handle gz automatically
            separator="\t",
            ## no header, column names
            has_header=False,
            new_columns=STARTING_COLS,
        )
        .with_columns(
            ## add PMID prefix
            pl.concat_str([pl.lit("PMID:"), pl.col("pmid").cast(pl.Utf8)]).alias("pmid"),
        ## entity cols are "Type|ID", split
            pl.col("entity1").str.split_exact("|", 1).alias("entity1_parts"),
            pl.col("entity2").str.split_exact("|", 1).alias("entity2_parts"),
        )
        ## setup cols after split
        .with_columns(
            pl.col("entity1_parts").struct.field("field_0").alias("entity1_type"),
            pl.col("entity1_parts").struct.field("field_1").alias("entity1_id"),
            pl.col("entity2_parts").struct.field("field_0").alias("entity2_type"),
            pl.col("entity2_parts").struct.field("field_1").alias("entity2_id"),
        )
        ## remove orig entity cols, split intermediate
        .select(SPLIT_COLS)
    )
    ## log number of starting rows
    koza.log(f"{df.shape[0]} rows at start.")

    ## log, filter out rows with EXCLUDED_ENTITY_TYPES
    n_before = df.shape[0]    ## save for log: calculating change
    df = df.filter(
        ~(
            pl.col("entity1_type").is_in(EXCLUDED_ENTITY_TYPES)
            | pl.col("entity2_type").is_in(EXCLUDED_ENTITY_TYPES)
        )
    )
    koza.log(f"{df.shape[0]} rows ({df.shape[0] / n_before:.1%}) after filtering out entity types that can't be NodeNormed: {", ".join(EXCLUDED_ENTITY_TYPES)}")

    ## log, filter out rows with EXCLUDED_RELATIONS
    n_before = df.shape[0]    ## save for log: calculating change
    df = df.filter(~(pl.col("relation").is_in(EXCLUDED_RELATIONS)))
    koza.log(f"{df.shape[0]} rows ({df.shape[0] / n_before:.1%}) after filtering out some relation values: {", ".join(EXCLUDED_RELATIONS)}")

    ## log, filter out rows with EXCLUDED_ENTITIES
    n_before = df.shape[0]    ## save for log: calculating change
    df = df.filter(
        ~(
            pl.col("entity1_id").is_in(EXCLUDED_ENTITIES)
            | pl.col("entity2_id").is_in(EXCLUDED_ENTITIES)
        )
    )
    koza.log(f"{n_before - df.shape[0]} rows filtered out because these entity IDs have NodeNorm category issues: {", ".join(EXCLUDED_ENTITIES)}")

    ## group-by/merge rows by unique triple
    df = (
        df.group_by(DEFINE_EDGE_COLS)
        .agg(
            ## doing unique just in case. This creates lists
            pl.col("pmid").unique().alias("pmid_set"),
            ## only take first value - NodeNorm will fix category later
            pl.col("entity1_type").first().alias("entity1_type"),
            pl.col("entity2_type").first().alias("entity2_type"),
        )
    )
    koza.log(f"{df.shape[0]} rows after merging by unique triple")

    ## DONE - output to transform step
    return df.to_dicts()

encountered_node_ids = set()

@koza.transform_record()
def transform_row(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    ## Nodes
    entity1_id = record["entity1_id"]
    entity2_id = record["entity2_id"]
    nodes=[]
    if entity1_id not in encountered_node_ids:
        nodes.append(get_node(record["entity1_type"], entity1_id))
        encountered_node_ids.add(entity1_id)
    if entity2_id not in encountered_node_ids:
        nodes.append(get_node(record["entity2_type"], entity2_id))
        encountered_node_ids.add(entity2_id)

    ## DRAFT - ANOTHER IDEA: don't include typing at all
    # entity1 = NamedThing(id=record["entity1_id"])
    # entity2 = NamedThing(id=record["entity2_id"])

    data_modeling = RELATION_MODELING.get(record["relation"], None)
    ## just in case - if there isn't a mapping, skip the record
    if not data_modeling:
        return None

    ## some mappings include a specific Association-type. If absent, use general Association (default)
    if data_modeling.get("association"):
        association = data_modeling["association"](
            id=entity_id(),
            subject=entity1_id,
            predicate=data_modeling["predicate"],
            object=entity2_id,
            ## for text-mining
            knowledge_level=KnowledgeLevelEnum.not_provided,
            agent_type=AgentTypeEnum.text_mining_agent,
            sources=PUBTATOR_SOURCES,
            publications=record["pmid_set"],
        )
        return KnowledgeGraph(nodes=nodes, edges=[association])
    else:
    ## use general Association
        association = Association(
            id=entity_id(),
            subject=entity1_id,
            predicate=data_modeling["predicate"],
            object=entity2_id,
            ## for text-mining
            knowledge_level=KnowledgeLevelEnum.not_provided,
            agent_type=AgentTypeEnum.text_mining_agent,
            sources=PUBTATOR_SOURCES,
            publications=record["pmid_set"],
        )
        return KnowledgeGraph(nodes=nodes, edges=[association])
