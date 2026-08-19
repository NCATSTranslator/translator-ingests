import koza
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from koza.model.graphs import KnowledgeGraph
from translator_ingest.util.biolink import build_association_knowledge_sources
from translator_ingest.util.transform_utils import entity_id
from translator_ingest.ingests.gtopdb.rules import InteractionRule, resolve_rule

from biolink_model.datamodel.pydanticmodel_v2 import (
    # Gene,
    Protein,
    ChemicalEntity,
    NamedThing,
    Association,
    ChemicalAffectsGeneAssociation,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    PairwiseMolecularInteraction,
    DirectionQualifierEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)

from translator_ingest.util.biolink import (
    INFORES_GTOPDB
)

GTOPDB_SOURCES = build_association_knowledge_sources(primary=INFORES_GTOPDB)

# adding additional needed resources
BIOLINK_CAUSES = "biolink:causes"
BIOLINK_AFFECTS = "biolink:affects"
BIOLINK_REGULATES = "biolink:regulates"
BIOLINK_RELATED = "biolink:related_to"


def _pipe_values(value: Any) -> tuple[str, ...]:
    """Parse a pipe-delimited source field without inventing identifiers."""
    if value is None or pd.isna(value):
        return ()
    return tuple(part.strip() for part in str(value).split("|") if part.strip())


@dataclass(frozen=True)
class TargetDescriptor:
    """Source identity and component evidence for one GtoPdb target."""

    source_id: str
    name: str
    species: str
    subunit_ids: tuple[str, ...]
    gene_symbols: tuple[str, ...]
    uniprot_ids: tuple[str, ...]

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "TargetDescriptor":
        """Build a target descriptor from a prepared GtoPdb interaction record."""
        return cls(
            source_id=str(record.get("target_id") or "").strip(),
            name=str(record.get("target_name") or record.get("object_name") or "").strip(),
            species=str(record.get("target_species") or "").strip(),
            subunit_ids=_pipe_values(record.get("target_subunit_ids")),
            gene_symbols=_pipe_values(record.get("target_gene_symbols")),
            uniprot_ids=_pipe_values(record.get("target_uniprot_ids") or record.get("object_id")),
        )

    @property
    def is_composite(self) -> bool:
        """Whether the source target carries evidence for multiple components."""
        return max(len(self.subunit_ids), len(self.gene_symbols), len(self.uniprot_ids)) > 1

    @property
    def single_protein_curie(self) -> str | None:
        """Return a UniProt CURIE only when the source identifies one protein."""
        if len(self.uniprot_ids) != 1:
            return None
        return f"UniProtKB:{self.uniprot_ids[0]}"


def get_latest_version() -> str:
    # lacking a better programmatic approach, derive the version from the gtopdb html
    html_page: requests.Response = requests.get('https://www.guidetopharmacology.org/download.jsp')
    resp: BeautifulSoup = BeautifulSoup(html_page.content, 'html.parser')

    # we expect the html to contain version text like 'Downloads are from the 2025.4 version.'
    # the following should extract the version from it (2025.4)
    search_text = 'Downloads are from the *'
    b_tag: BeautifulSoup.Tag = resp.find('b', string=re.compile(search_text))
    if len(b_tag) > 0:
        html_value = b_tag.text
        html_value = html_value[len(search_text) - 1:]  # remove the 'Downloads are from the' part
        source_version = html_value.split(' version')[0]  # remove the ' version.' part
        return source_version

    raise RuntimeError('Could not find the "Downloads are from the" text in the html to find the latest version.')

@koza.prepare_data(tag="gtopdb_interaction_parsing")
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:

    ## used for debugging only
    ## check whether the mapping tag is in the same execution context
    # print("STATE KEYS:", koza.state.keys())
    # print("MAPPING SIZE:", len(koza.state.get("pubchem_id_mapping_dict", {})))

    ## Load ligands mapping CSV directly
    ## skip the metadata row
    ## Specify that 'Ligand ID' and "PubChem CID" should be read as a string
    ligands_file_path = Path(koza.input_files_dir) / "ligands.csv"
    mapping_df = pd.read_csv(ligands_file_path, skiprows = 1, dtype={'Ligand ID': str, 'PubChem CID': str})
    ## used for debugging only
    # print("Mapping CSV columns:", mapping_df.columns.tolist())

    mapping_dict = dict(zip(
        mapping_df["Ligand ID"].astype(str).str.strip(),
        mapping_df["PubChem CID"].astype(str).str.strip()
    ))

    ## convert the input dataframe into pandas df format
    source_df = pd.DataFrame(data)

    ## Only select needed columns
    sele_cols = [
        'Target', 'Target ID', 'Target Subunit IDs', 'Target Gene Symbol',
        'Target UniProt ID', 'Target Species', 'Ligand ID', 'Ligand', 'Type',
        'Action', 'Endogenous', 'Ligand Context', 'PubMed ID',
    ]
    source_subset_df = source_df[sele_cols].drop_duplicates()

    ## Specify that 'Ligand ID' and "Target UniProt ID" should be read as a string ('object' dtype) to avoid pandas changing identifier from 1102 -> 1102.0
    source_subset_df = source_subset_df.astype({
        "Ligand ID": "string",
        "Target UniProt ID": "string"
    })

    ## debugging usage
    # koza.log(f"DataFrame columns: {source_df.columns.tolist()}")

    ## Drop nan values
    source_subset_df = source_subset_df.dropna(subset=["Target UniProt ID", "Ligand ID"])

    ## Implement logic to aggregate source records into a single edge based on SPO + qualifier pair (subject_name, subject_category, object_name, object_category, MECHANISM, EFFECT, DIRECT)
    group_cols = ['Target', 'Target UniProt ID', 'Ligand ID', 'Ligand', 'Type', 'Action', 'Endogenous']

    source_agg_df = (
        ## In pandas, groupby() drops rows with NA in any grouping key by default, which can silently discard interaction rows (and makes downstream Type/Action is None handling unreachable).
        ## use groupby(..., dropna=False) if intend to keep records with missing qualifiers
        source_subset_df.groupby(group_cols, as_index=False, dropna=False)
        .agg({
            "PubMed ID": lambda x: "|".join(pd.unique(x.dropna().astype(str))),
            "Target ID": "first",
            "Target Subunit IDs": "first",
            "Target Gene Symbol": "first",
            "Target Species": "first",
            })
    )

    ## rename those columns into desired format, note we need to obtain "pubchem CID" as subject id from "Ligand ID"
    source_agg_df.rename(
        columns={
            "Ligand": "subject_name",
            "Target": "target_name",
            "Target ID": "target_id",
            "Target Subunit IDs": "target_subunit_ids",
            "Target Gene Symbol": "target_gene_symbols",
            "Target UniProt ID": "target_uniprot_ids",
            "Target Species": "target_species",
        },
        inplace=True,
    )

    ## avoid mismatching by converting string ids into integer IDs
    source_agg_df["subject_id"] = (
        source_agg_df["Ligand ID"]
        .astype(str)
        .str.strip()
        .map(mapping_dict)
    )

    ## drop NA of those dont find a mapping
    source_agg_df = source_agg_df.dropna(subset=["subject_id"])

    return source_agg_df.drop_duplicates().to_dict(orient="records")


@koza.transform(tag="gtopdb_interaction_parsing")
def transform_ingest_all(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    """Transform prepared GtoPdb records through declarative Type/Action rules."""
    nodes: list[NamedThing] = []
    edges: list[Association] = []
    unsupported_composite_count = 0

    for record in data:
        target = TargetDescriptor.from_record(record)
        if target.is_composite:
            unsupported_composite_count += 1
            continue

        rule = resolve_rule(record["Type"], record["Action"])
        if rule is None or rule.skip:
            continue

        subject = ChemicalEntity(
            id="PUBCHEM.COMPOUND:" + record["subject_id"],
            name=record["subject_name"],
        )
        raw_uniprot_id = record.get("target_uniprot_ids") or record.get("object_id") or ""
        object = Protein(
            id=target.single_protein_curie or f"UniProtKB:{raw_uniprot_id}",
            name=target.name,
        )
        publications = [f"PMID:{pmid}" for pmid in record["PubMed ID"].split("|")] if record["PubMed ID"] else None

        primary = _build_primary_association(subject, object, record["Endogenous"], rule)
        emitted_edges = [primary]
        if rule.physical_interaction:
            emitted_edges.append(_build_physical_interaction(subject, object))

        if publications:
            for edge in emitted_edges:
                edge.publications = publications

        nodes.extend((subject, object))
        edges.extend(emitted_edges)

    if unsupported_composite_count:
        record_word = "record" if unsupported_composite_count == 1 else "records"
        koza.log(
            f"Excluded {unsupported_composite_count} GtoPdb interaction {record_word} "
            "with an unsupported composite target; no compound UniProt CURIE was emitted.",
            level="WARNING",
        )

    return [KnowledgeGraph(nodes=nodes, edges=edges)]


def _build_primary_association(
    subject: ChemicalEntity,
    object: Protein,
    endogenous: str,
    rule: InteractionRule,
) -> Association:
    """Construct the one pharmacological edge selected by an interaction rule."""
    if rule.relation == "related":
        return Association(
            id=entity_id(),
            subject=subject.id,
            predicate=BIOLINK_RELATED,
            object=object.id,
            sources=GTOPDB_SOURCES,
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        )

    predicate, direction = _endogenous_projection(endogenous, rule)
    return ChemicalAffectsGeneAssociation(
        id=entity_id(),
        subject=subject.id,
        predicate=predicate,
        object=object.id,
        qualified_predicate=BIOLINK_CAUSES if rule.qualified else None,
        object_aspect_qualifier=GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
        object_direction_qualifier=direction,
        causal_mechanism_qualifier=rule.mechanism,
        sources=GTOPDB_SOURCES,
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )


def _endogenous_projection(
    endogenous: str,
    rule: InteractionRule,
) -> tuple[str, DirectionQualifierEnum | None]:
    """Project source polarity into predicate and direction under endogenous policy."""
    if endogenous == "TRUE":
        predicate = BIOLINK_REGULATES
        directions = {
            "positive": DirectionQualifierEnum.upregulated,
            "negative": DirectionQualifierEnum.downregulated,
        }
    else:
        predicate = BIOLINK_AFFECTS
        directions = {
            "positive": DirectionQualifierEnum.increased,
            "negative": DirectionQualifierEnum.decreased,
        }
    return predicate, directions.get(rule.polarity)


def _build_physical_interaction(subject: ChemicalEntity, object: Protein) -> PairwiseMolecularInteraction:
    """Construct the companion direct physical-interaction edge for a rule."""
    return PairwiseMolecularInteraction(
        id=entity_id(),
        subject=subject.id,
        predicate="biolink:directly_physically_interacts_with",
        object=object.id,
        sources=GTOPDB_SOURCES,
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
