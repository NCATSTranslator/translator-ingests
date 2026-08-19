"""GtoPdb ingest preparation and graph emission."""

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Iterable

from bs4 import BeautifulSoup
import koza
from koza.model.graphs import KnowledgeGraph
import pandas as pd
import requests
from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    Association,
    ChemicalAffectsGeneAssociation,
    ChemicalEntity,
    DirectionQualifierEnum,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    KnowledgeLevelEnum,
    NamedThing,
    PairwiseMolecularInteraction,
    Protein,
)

from translator_ingest.ingests.gtopdb.rules import InteractionRule, resolve_rule
from translator_ingest.util.biolink import INFORES_GTOPDB, build_association_knowledge_sources
from translator_ingest.util.transform_utils import entity_id


GTOPDB_SOURCES = build_association_knowledge_sources(primary=INFORES_GTOPDB)

BIOLINK_CAUSES = "biolink:causes"
BIOLINK_AFFECTS = "biolink:affects"
BIOLINK_REGULATES = "biolink:regulates"
BIOLINK_RELATED = "biolink:related_to"

LIGAND_ID_COLUMN = "Ligand ID"
PUBCHEM_ID_COLUMN = "PubChem CID"
PUBLICATIONS_COLUMN = "PubMed ID"

SOURCE_COLUMNS = (
    "Target",
    "Target ID",
    "Target Subunit IDs",
    "Target Gene Symbol",
    "Target UniProt ID",
    "Target Species",
    LIGAND_ID_COLUMN,
    "Ligand",
    "Type",
    "Action",
    "Endogenous",
    "Ligand Context",
    PUBLICATIONS_COLUMN,
)

GROUP_COLUMNS = (
    "Target",
    "Target UniProt ID",
    LIGAND_ID_COLUMN,
    "Ligand",
    "Type",
    "Action",
    "Endogenous",
)

TARGET_METADATA_COLUMNS = (
    "Target ID",
    "Target Subunit IDs",
    "Target Gene Symbol",
    "Target Species",
)

PREPARED_COLUMN_RENAMES = {
    "Ligand": "subject_name",
    "Target": "target_name",
    "Target ID": "target_id",
    "Target Subunit IDs": "target_subunit_ids",
    "Target Gene Symbol": "target_gene_symbols",
    "Target UniProt ID": "target_uniprot_ids",
    "Target Species": "target_species",
}


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
    """Derive the GtoPdb release version from its download page."""
    response = requests.get("https://www.guidetopharmacology.org/download.jsp")
    soup = BeautifulSoup(response.content, "html.parser")
    version_tag = soup.find("b", string=re.compile("Downloads are from the *"))
    if version_tag is None:
        raise RuntimeError("Could not find the GtoPdb download version text.")

    version_text = version_tag.text
    return version_text[len("Downloads are from the "):].split(" version")[0]


def _load_ligand_mapping(input_files_dir: Path) -> dict[str, str]:
    """Load the source Ligand ID to PubChem CID crosswalk."""
    ligands = pd.read_csv(
        input_files_dir / "ligands.csv",
        skiprows=1,
        dtype={LIGAND_ID_COLUMN: str, PUBCHEM_ID_COLUMN: str},
    )
    return dict(
        zip(
            ligands[LIGAND_ID_COLUMN].astype(str).str.strip(),
            ligands[PUBCHEM_ID_COLUMN].astype(str).str.strip(),
        )
    )


def _join_publications(values: pd.Series) -> str:
    """Combine distinct source publication cells in their input order."""
    return "|".join(pd.unique(values.dropna().astype(str)))


def _prepare_interactions(
    data: Iterable[dict[str, Any]],
    ligand_mapping: dict[str, str],
) -> list[dict[str, Any]]:
    """Aggregate source rows and retain source target metadata for emission."""
    source = pd.DataFrame(data)[list(SOURCE_COLUMNS)].drop_duplicates()
    source = source.astype({LIGAND_ID_COLUMN: "string", "Target UniProt ID": "string"})
    source = source.dropna(subset=["Target UniProt ID", LIGAND_ID_COLUMN])

    aggregations: dict[str, Any] = {PUBLICATIONS_COLUMN: _join_publications}
    aggregations.update({column: "first" for column in TARGET_METADATA_COLUMNS})
    prepared = source.groupby(list(GROUP_COLUMNS), as_index=False, dropna=False).agg(aggregations)
    prepared = prepared.rename(columns=PREPARED_COLUMN_RENAMES)
    prepared["subject_id"] = prepared[LIGAND_ID_COLUMN].astype(str).str.strip().map(ligand_mapping)
    prepared = prepared.dropna(subset=["subject_id"]).drop_duplicates()
    return prepared.to_dict(orient="records")


@koza.prepare_data(tag="gtopdb_interaction_parsing")
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Prepare GtoPdb interactions for record-level graph transformation."""
    return _prepare_interactions(data, _load_ligand_mapping(Path(koza.input_files_dir)))


def _publication_list(value: str | None) -> list[str] | None:
    """Convert the source's pipe-delimited publication field into PubMed CURIEs."""
    if not value:
        return None
    return [f"PMID:{pmid}" for pmid in value.split("|")]


def _nodes_for_record(record: dict[str, Any], target: TargetDescriptor) -> tuple[ChemicalEntity, Protein]:
    """Create the single-protein node pair used by the current projection."""
    subject = ChemicalEntity(
        id=f"PUBCHEM.COMPOUND:{record['subject_id']}",
        name=record["subject_name"],
    )
    raw_uniprot_id = record.get("target_uniprot_ids") or record.get("object_id") or ""
    object = Protein(
        id=target.single_protein_curie or f"UniProtKB:{raw_uniprot_id}",
        name=target.name,
    )
    return subject, object


def _attach_publications(edges: list[Association], publications: list[str] | None) -> None:
    """Attach shared publications to every edge emitted for one source record."""
    if publications:
        for edge in edges:
            edge.publications = publications


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


def _edges_for_record(
    subject: ChemicalEntity,
    object: Protein,
    endogenous: str,
    rule: InteractionRule,
    publications: list[str] | None,
) -> list[Association]:
    """Build every graph edge emitted for one supported source record."""
    edges: list[Association] = [_build_primary_association(subject, object, endogenous, rule)]
    if rule.physical_interaction:
        edges.append(_build_physical_interaction(subject, object))
    _attach_publications(edges, publications)
    return edges


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

        subject, object = _nodes_for_record(record, target)
        emitted_edges = _edges_for_record(
            subject,
            object,
            record["Endogenous"],
            rule,
            _publication_list(record[PUBLICATIONS_COLUMN]),
        )
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
