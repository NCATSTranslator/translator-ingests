"""GtoPdb ingest preparation and graph emission."""

from dataclasses import dataclass
from enum import Enum
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
    MacromolecularComplex,
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

SPECIES_TO_TAXON = {
    "bovine": "NCBITaxon:9913",
    "chicken": "NCBITaxon:9031",
    "dog": "NCBITaxon:9615",
    "escherichia coli": "NCBITaxon:562",
    "ferret": "NCBITaxon:9669",
    "gorilla": "NCBITaxon:9595",
    "guinea pig": "NCBITaxon:10141",
    "hepatitis c virus": "NCBITaxon:11103",
    "honeybee": "NCBITaxon:7460",
    "human": "NCBITaxon:9606",
    "mers-cov": "NCBITaxon:1335626",
    "mouse": "NCBITaxon:10090",
    "mycobacterium tuberculosis": "NCBITaxon:1773",
    "pig": "NCBITaxon:9823",
    "plasmodium berghei": "NCBITaxon:5821",
    "plasmodium cynomolgi": "NCBITaxon:5827",
    "plasmodium falciparum": "NCBITaxon:5833",
    "plasmodium knowlesi": "NCBITaxon:5850",
    "plasmodium vivax": "NCBITaxon:5855",
    "plasmodium yoelii": "NCBITaxon:5861",
    "rabbit": "NCBITaxon:9986",
    "rat": "NCBITaxon:10116",
    "sars-cov": "NCBITaxon:694009",
    "sars-cov-2": "NCBITaxon:2697049",
    "sheep": "NCBITaxon:9940",
    "turkey": "NCBITaxon:9103",
    "zika virus": "NCBITaxon:64320",
}


def _pipe_values(value: Any) -> tuple[str, ...]:
    """Parse a pipe-delimited source field without inventing identifiers."""
    if value is None or pd.isna(value):
        return ()
    return tuple(part.strip() for part in str(value).split("|") if part.strip())


def _source_text(value: Any) -> str:
    """Normalize a nullable source scalar to stripped text.

    >>> _source_text(pd.NA)
    ''
    >>> _source_text(" Human ")
    'Human'
    """
    return "" if value is None or pd.isna(value) else str(value).strip()


class TargetClassification(str, Enum):
    """The source-supported representation of a GtoPdb target descriptor."""

    SINGLE_PROTEIN = "single_protein"
    MACROMOLECULAR_COMPLEX = "macromolecular_complex"
    UNRESOLVED_MULTI_PROTEIN_GROUP = "unresolved_multi_protein_group"
    UNMAPPED = "unmapped"


def _canonical_uniprot_accession(accession: str) -> str:
    """Remove a terminal UniProt isoform suffix from an accession.

    >>> _canonical_uniprot_accession("P56856-2")
    'P56856'
    >>> _canonical_uniprot_accession("P46098")
    'P46098'
    """
    return re.sub(r"-\d+$", "", accession)


def _known_species(value: str) -> str | None:
    """Return a normalized source species value, excluding unknown placeholders.

    >>> _known_species("Human")
    'human'
    >>> _known_species("Unknown") is None
    True
    """
    normalized = _source_text(value).casefold()
    return normalized if normalized not in {"", "none", "unknown"} else None


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
            source_id=_source_text(record.get("target_id")),
            name=_source_text(record.get("target_name"))
            or _source_text(record.get("object_name")),
            species=_source_text(record.get("target_species")),
            subunit_ids=_pipe_values(record.get("target_subunit_ids")),
            gene_symbols=_pipe_values(record.get("target_gene_symbols")),
            uniprot_ids=_pipe_values(record.get("target_uniprot_ids"))
            or _pipe_values(record.get("object_id")),
        )

    @property
    def key(self) -> tuple[str, str]:
        """Return the source target identifier together with its species context."""
        return self.source_id, self.species

    @property
    def canonical_uniprot_ids(self) -> tuple[str, ...]:
        """Return the distinct canonical accessions in source order."""
        return tuple(dict.fromkeys(_canonical_uniprot_accession(accession) for accession in self.uniprot_ids))

    @property
    def classification(self) -> TargetClassification:
        """Classify the target using subunit evidence before accession cardinality.

        Multiple subunit IDs identify a source-defined complex. Multiple
        canonical accessions without that evidence remain unresolved rather
        than being inferred to be a complex.

        Examples from GtoPdb 2026.2 ``interactions.csv``:

        - Target 378 (5-HT3AB, Human) has subunits ``373|374`` and proteins
          ``P46098|O95264``; it is a macromolecular complex.
        - Target 2903 (claudin 18, Human) has ``P56856|P56856-2`` but no
          subunits; both values identify the same canonical protein.
        - Target 34 (AT1 receptor, Rat) has ``P29089|P25095`` and no
          subunits; it remains an unresolved multi-protein group.
        - Target 1287 (guanylyl cyclase alpha1/beta1, Bovine) has subunits
          ``1288|1290`` but no UniProt mapping; it is still a complex.
        """
        if len(self.subunit_ids) > 1:
            # Source subunits, not UniProt cardinality, establish a complex.
            return TargetClassification.MACROMOLECULAR_COMPLEX
        canonical_uniprot_ids = self.canonical_uniprot_ids
        if len(canonical_uniprot_ids) == 1:
            # A pipe-delimited list can contain canonical and isoform accessions.
            return TargetClassification.SINGLE_PROTEIN
        if len(canonical_uniprot_ids) > 1:
            # Do not turn source groups or paralog alternatives into complexes.
            return TargetClassification.UNRESOLVED_MULTI_PROTEIN_GROUP
        return TargetClassification.UNMAPPED

    @property
    def protein_curie(self) -> str | None:
        """Return the canonical protein CURIE for a single-protein descriptor."""
        if self.classification is not TargetClassification.SINGLE_PROTEIN:
            return None
        return f"UniProtKB:{self.canonical_uniprot_ids[0]}"

    @property
    def complex_curie(self) -> str | None:
        """Return the GtoPdb target CURIE for a source-defined complex."""
        if self.classification is not TargetClassification.MACROMOLECULAR_COMPLEX:
            return None
        return f"IUPHARobj:{self.source_id}"

    @property
    def species_context_qualifier(self) -> str | None:
        """Return the NCBI Taxonomy CURIE for a recognized source species."""
        species = _known_species(self.species)
        return SPECIES_TO_TAXON.get(species) if species else None


def source_target_species_descriptors(
    targets: Iterable[TargetDescriptor],
) -> dict[str, dict[str, TargetDescriptor]]:
    """Index source target descriptors by ID and known species.

    Complex membership and multi-species coverage are independent properties:
    a GtoPdb target can be a complex in several species.

    For example, target 378 (5-HT3AB) has distinct Human and Mouse descriptors
    in GtoPdb 2026.2, and both descriptors identify the same two source
    subunits. It is therefore a multi-species complex, not an ambiguous list
    of proteins to split into individual interaction edges.

    >>> human = TargetDescriptor("378", "5-HT3AB", "Human", (), (), ("P46098",))
    >>> mouse = TargetDescriptor("378", "5-HT3AB", "Mouse", (), (), ("P23979",))
    >>> sorted(source_target_species_descriptors((human, mouse))["378"])
    ['human', 'mouse']
    """
    descriptors: dict[str, dict[str, TargetDescriptor]] = {}
    for target in targets:
        species = _known_species(target.species)
        if target.source_id and species:
            descriptors.setdefault(target.source_id, {})[species] = target
    return descriptors


def multi_species_source_target_ids(targets: Iterable[TargetDescriptor]) -> frozenset[str]:
    """Return source target IDs represented for more than one known species."""
    descriptors = source_target_species_descriptors(targets)
    return frozenset(
        source_id for source_id, species in descriptors.items() if len(species) > 1
    )


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
    source = source.astype({LIGAND_ID_COLUMN: "string", "Target ID": "string"})
    source = source.dropna(subset=["Target ID", LIGAND_ID_COLUMN])
    source = source[
        source["Target ID"].str.strip().ne("")
        & source[LIGAND_ID_COLUMN].str.strip().ne("")
    ]

    aggregations: dict[str, Any] = {PUBLICATIONS_COLUMN: _join_publications}
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


def _nodes_for_record(record: dict[str, Any], target: TargetDescriptor) -> tuple[ChemicalEntity, NamedThing]:
    """Create the source-supported chemical and target nodes for one record."""
    subject = ChemicalEntity(
        id=f"PUBCHEM.COMPOUND:{record['subject_id']}",
        name=record["subject_name"],
    )
    if target.protein_curie:
        object: NamedThing = Protein(
            id=target.protein_curie,
            name=target.name,
            in_taxon=[target.species_context_qualifier]
            if target.species_context_qualifier
            else None,
        )
    elif target.complex_curie:
        object = MacromolecularComplex(
            id=target.complex_curie,
            name=target.name,
            # IUPHARobj identifies the source target concept. The taxon keeps
            # human target 378 (5-HT3AB) distinct in meaning from its mouse
            # descriptor, even though both use IUPHARobj:378 as the node ID.
            in_taxon=[target.species_context_qualifier]
            if target.species_context_qualifier
            else None,
        )
    else:
        raise ValueError(f"Cannot create a target node for {target.key}")
    return subject, object


def _component_nodes(target: TargetDescriptor) -> list[Protein]:
    """Create protein nodes for the explicitly identified complex components."""
    if not target.complex_curie:
        return []
    return [
        Protein(
            id=f"UniProtKB:{accession}",
            in_taxon=[target.species_context_qualifier]
            if target.species_context_qualifier
            else None,
        )
        for accession in target.canonical_uniprot_ids
    ]


def _attach_publications(edges: list[Association], publications: list[str] | None) -> None:
    """Attach shared publications to every edge emitted for one source record."""
    if publications:
        for edge in edges:
            edge.publications = publications


def _build_primary_association(
    subject: ChemicalEntity,
    object: NamedThing,
    endogenous: str,
    rule: InteractionRule,
    species_context_qualifier: str | None,
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
        species_context_qualifier=species_context_qualifier,
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


def _build_physical_interaction(
    subject: ChemicalEntity,
    object: NamedThing,
    species_context_qualifier: str | None,
) -> PairwiseMolecularInteraction:
    """Construct the companion direct physical-interaction edge for a rule."""
    return PairwiseMolecularInteraction(
        id=entity_id(),
        subject=subject.id,
        predicate="biolink:directly_physically_interacts_with",
        object=object.id,
        sources=GTOPDB_SOURCES,
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
        species_context_qualifier=species_context_qualifier,
    )


def _edges_for_record(
    subject: ChemicalEntity,
    object: NamedThing,
    target: TargetDescriptor,
    endogenous: str,
    rule: InteractionRule,
    publications: list[str] | None,
    emit_component_edges: bool,
) -> list[Association]:
    """Build every graph edge emitted for one supported source record.

    ``IUPHARobj:<Target ID>`` is shared by GtoPdb species descriptors. Until
    Biolink permits a taxon-qualified ``has_part`` assertion, component edges
    would merge the human and mouse compositions for target 378 (5-HT3AB).
    Therefore multi-species source IDs omit those edges conservatively; see
    https://github.com/biolink/biolink-model/pull/1797 and translator-ingests#510.
    """
    edges: list[Association] = [
        _build_primary_association(
            subject,
            object,
            endogenous,
            rule,
            target.species_context_qualifier,
        )
    ]
    if rule.physical_interaction:
        edges.append(
            _build_physical_interaction(subject, object, target.species_context_qualifier)
        )
    if target.complex_curie and emit_component_edges:
        edges.extend(
            Association(
                id=entity_id(),
                subject=target.complex_curie,
                predicate="biolink:has_part",
                object=f"UniProtKB:{accession}",
                sources=GTOPDB_SOURCES,
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
            )
            for accession in target.canonical_uniprot_ids
        )
    _attach_publications(edges, publications)
    return edges


@koza.transform(tag="gtopdb_interaction_parsing")
def transform_ingest_all(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    """Transform prepared GtoPdb records through declarative Type/Action rules."""
    records = list(data)
    targets = tuple(TargetDescriptor.from_record(record) for record in records)
    descriptors = source_target_species_descriptors(targets)
    multi_species_target_ids = frozenset(
        source_id for source_id, species in descriptors.items() if len(species) > 1
    )
    nodes: list[NamedThing] = []
    edges: list[Association] = []
    unsupported_target_counts: dict[TargetClassification, int] = {}

    for record, target in zip(records, targets, strict=True):
        if target.classification in {
            TargetClassification.UNRESOLVED_MULTI_PROTEIN_GROUP,
            TargetClassification.UNMAPPED,
        }:
            unsupported_target_counts[target.classification] = (
                unsupported_target_counts.get(target.classification, 0) + 1
            )
            continue

        rule = resolve_rule(record["Type"], record["Action"])
        if rule is None or rule.skip:
            continue

        subject, object = _nodes_for_record(record, target)
        emitted_edges = _edges_for_record(
            subject,
            object,
            target,
            record["Endogenous"],
            rule,
            _publication_list(record[PUBLICATIONS_COLUMN]),
            emit_component_edges=target.source_id not in multi_species_target_ids,
        )
        # Keep the species-qualified ligand-target assertion, but do not emit
        # unqualified components for shared source IDs. See the RIG's
        # multi-species composition note, biolink-model#1797, and
        # translator-ingests#510.
        component_nodes = (
            _component_nodes(target)
            if target.source_id not in multi_species_target_ids
            else []
        )
        nodes.extend((subject, object, *component_nodes))
        edges.extend(emitted_edges)

    for classification, count in unsupported_target_counts.items():
        record_word = "record" if count == 1 else "records"
        koza.log(
            f"Excluded {count} GtoPdb interaction {record_word} with a "
            f"{classification.value} target; no unsupported target CURIE was emitted.",
            level="WARNING",
        )

    return [KnowledgeGraph(nodes=nodes, edges=edges)]
