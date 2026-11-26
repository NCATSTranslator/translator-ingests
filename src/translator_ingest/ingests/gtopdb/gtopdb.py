"""GtoPdb ingest preparation and graph emission."""

from collections import Counter
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
import re
from typing import Any, Iterable

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
    KnowledgeLevelEnum,
    MacromolecularComplex,
    NamedThing,
    PairwiseMolecularInteraction,
    Protein,
    RetrievalSource,
)

from translator_ingest.ingests.gtopdb.rules import (
    InteractionRule,
    PrimaryAssociationRule,
    resolve_rule,
)
from translator_ingest.util.biolink import INFORES_GTOPDB, build_association_knowledge_sources
from translator_ingest.util.transform_utils import entity_id

GTOPDB_RECORD_BASE_URL = "https://www.guidetopharmacology.org/GRAC"

BIOLINK_CAUSES = "biolink:causes"
BIOLINK_AFFECTS = "biolink:affects"
BIOLINK_REGULATES = "biolink:regulates"
BIOLINK_RELATED = "biolink:related_to"

# The interactions file's first metadata line carries its release version.
GTOPDB_INTERACTIONS_URL = "https://www.guidetopharmacology.org/DATA/interactions.csv"
GTOPDB_VERSION_PATTERN = re.compile(r"GtoPdb Version:\s*(?P<version>\S+)")

# Complex Portal files listed in download.yaml, one per species of a GtoPdb complex target.
COMPLEX_PORTAL_FILE_PATTERN = "complexportal_*.tsv"
COMPLEX_PORTAL_ID_COLUMN = "#Complex ac"
COMPLEX_PORTAL_TAXON_COLUMN = "Taxonomy identifier"
COMPLEX_PORTAL_MEMBERS_COLUMN = "Identifiers (and stoichiometry) of molecules in complex"
# A plain UniProt accession, optionally an isoform, with its stoichiometry. Members that
# are other complexes (CPX-), chemicals (CHEBI:), RNAs (URS), processed chains (-PRO_)
# or bracketed alternatives do not match, so complexes containing them are never used.
COMPLEX_PORTAL_PROTEIN_MEMBER = re.compile(
    r"(?P<accession>[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9](?:[A-Z][A-Z0-9]{2}[0-9]){1,2})(?:-\d+)?\(\d+\)"
)

LIGAND_ID_COLUMN = "Ligand ID"
PUBCHEM_ID_COLUMN = "PubChem CID"
PUBLICATIONS_COLUMN = "PubMed ID"

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
SOURCE_COLUMNS = (*GROUP_COLUMNS, "Ligand Context", PUBLICATIONS_COLUMN)

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


def gtopdb_ligand_record_url(ligand_id: str) -> str:
    """Link to a ligand's biology page using its source ID, never its PubChem ID.

    >>> gtopdb_ligand_record_url("10250")
    'https://www.guidetopharmacology.org/GRAC/LigandDisplayForward?tab=biology&ligandId=10250'
    """
    identifier = _source_text(ligand_id)
    if not re.fullmatch(r"[0-9]+", identifier):
        raise ValueError(f"Expected a numeric GtoPdb Ligand ID, got {ligand_id!r}")
    return f"{GTOPDB_RECORD_BASE_URL}/LigandDisplayForward?tab=biology&ligandId={identifier}"


def gtopdb_target_record_url(target_id: str) -> str:
    """Link complex membership evidence to its source target, independently of ligand.

    >>> gtopdb_target_record_url("378")
    'https://www.guidetopharmacology.org/GRAC/ObjectDisplayForward?objectId=378'
    """
    identifier = _source_text(target_id)
    if not re.fullmatch(r"[0-9]+", identifier):
        raise ValueError(f"Expected a numeric GtoPdb Target ID, got {target_id!r}")
    return f"{GTOPDB_RECORD_BASE_URL}/ObjectDisplayForward?objectId={identifier}"


def _gtopdb_sources(record_url: str) -> list[RetrievalSource]:
    """Attribute one edge to GtoPdb together with the source page that supports it."""
    return build_association_knowledge_sources(primary=(INFORES_GTOPDB, [record_url]))


class TargetClassification(str, Enum):
    """The source-supported representation of a GtoPdb target descriptor."""

    SINGLE_PROTEIN = "single_protein"
    MACROMOLECULAR_COMPLEX = "macromolecular_complex"
    UNRESOLVED_MULTI_PROTEIN_GROUP = "unresolved_multi_protein_group"
    UNMAPPED = "unmapped"


UNSUPPORTED_TARGET_CLASSES = frozenset(
    {
        TargetClassification.UNRESOLVED_MULTI_PROTEIN_GROUP,
        TargetClassification.UNMAPPED,
    }
)


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
    complex_portal_id: str = ""

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "TargetDescriptor":
        """Build a target descriptor from a prepared GtoPdb interaction record."""
        return cls(
            source_id=_source_text(record.get("target_id")),
            name=_source_text(record.get("target_name")) or _source_text(record.get("object_name")),
            species=_source_text(record.get("target_species")),
            subunit_ids=_pipe_values(record.get("target_subunit_ids")),
            gene_symbols=_pipe_values(record.get("target_gene_symbols")),
            uniprot_ids=_pipe_values(record.get("target_uniprot_ids")) or _pipe_values(record.get("object_id")),
            complex_portal_id=_source_text(record.get("complex_portal_id")),
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
    def iuphar_curie(self) -> str | None:
        """Return the GtoPdb target CURIE for a source-defined complex."""
        if self.classification is not TargetClassification.MACROMOLECULAR_COMPLEX:
            return None
        return f"IUPHARobj:{self.source_id}"

    @property
    def complex_curie(self) -> str | None:
        """Return the node CURIE for a complex, preferring a matched Complex Portal ID.

        NodeNorm does not resolve IUPHARobj (NCATSTranslator/Babel#1277), so a
        species-specific Complex Portal ID with the same members is used when one
        was matched during preparation.

        >>> human = TargetDescriptor("378", "5-HT3AB", "Human", ("373", "374"), (), ("P46098", "O95264"))
        >>> human.complex_curie
        'IUPHARobj:378'
        >>> TargetDescriptor("378", "5-HT3AB", "Human", ("373", "374"), (), (), "CPX-271").complex_curie
        'ComplexPortal:CPX-271'
        """
        if self.iuphar_curie is None:
            return None
        return f"ComplexPortal:{self.complex_portal_id}" if self.complex_portal_id else self.iuphar_curie

    @property
    def complex_member_key(self) -> tuple[str, frozenset[str]] | None:
        """Return the (taxon, canonical UniProt members) key used to match Complex Portal."""
        if self.iuphar_curie is None or not self.species_context_qualifier or not self.canonical_uniprot_ids:
            return None
        return self.species_context_qualifier, frozenset(self.canonical_uniprot_ids)

    @property
    def species_context_qualifier(self) -> str | None:
        """Return the NCBI Taxonomy CURIE for a recognized source species."""
        species = _known_species(self.species)
        return SPECIES_TO_TAXON.get(species) if species else None


def get_latest_version() -> str:
    """Determine the latest GtoPdb release version.

    The download.jsp web page that used to advertise the version now requires a login, so the version is
    read from the metadata comment on the first line of the interactions file instead. That file is the
    same one this ingest downloads, so the version is guaranteed to describe the data actually ingested.

    :return: The GtoPdb version, e.g. '2026.2'
    :raises RuntimeError: If the version could not be parsed from the file.
    """
    with requests.get(GTOPDB_INTERACTIONS_URL, stream=True, timeout=60) as response:
        response.raise_for_status()
        first_line = next(response.iter_lines(decode_unicode=True), "")

    match = GTOPDB_VERSION_PATTERN.search(first_line)
    if match is None:
        raise RuntimeError(
            f"Could not parse the GtoPdb version from the first line of {GTOPDB_INTERACTIONS_URL}: {first_line!r}"
        )
    return match.group("version")


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


def _complex_portal_protein_members(members: str) -> frozenset[str] | None:
    """Return canonical UniProt members, or None if any member is not a plain protein.

    >>> sorted(_complex_portal_protein_members("P46098(2)|O95264(3)"))
    ['O95264', 'P46098']
    >>> _complex_portal_protein_members("CPX-6024(1)|P15260(2)|P38484(2)") is None
    True
    >>> _complex_portal_protein_members("Q6UXL0(1)|Q9UHD0-PRO_0000015379(1)") is None
    True
    """
    accessions = []
    for member in members.split("|"):
        match = COMPLEX_PORTAL_PROTEIN_MEMBER.fullmatch(member.strip())
        if match is None:
            return None
        accessions.append(match["accession"])
    return frozenset(accessions)


def _load_complex_portal_index(input_files_dir: Path) -> dict[tuple[str, frozenset[str]], str]:
    """Index protein-only Complex Portal complexes by taxon and members.

    A key shared by several complexes is ambiguous and is left out, so a GtoPdb
    complex keeps its IUPHARobj ID unless exactly one complex matches.
    """
    candidates: dict[tuple[str, frozenset[str]], set[str]] = {}
    for path in sorted(input_files_dir.glob(COMPLEX_PORTAL_FILE_PATTERN)):
        table = pd.read_csv(path, sep="\t", dtype=str).fillna("")
        for complex_id, taxon, members in table[
            [COMPLEX_PORTAL_ID_COLUMN, COMPLEX_PORTAL_TAXON_COLUMN, COMPLEX_PORTAL_MEMBERS_COLUMN]
        ].itertuples(index=False):
            proteins = _complex_portal_protein_members(members)
            if proteins:
                candidates.setdefault((f"NCBITaxon:{taxon.strip()}", proteins), set()).add(complex_id.strip())
    return {key: next(iter(ids)) for key, ids in candidates.items() if len(ids) == 1}


def _join_publications(values: pd.Series) -> str:
    """Combine distinct source publication cells in their input order."""
    return "|".join(pd.unique(values.dropna().astype(str)))


def _prepare_interactions(
    data: Iterable[dict[str, Any]],
    ligand_mapping: dict[str, str],
    complex_portal_index: dict[tuple[str, frozenset[str]], str],
) -> list[dict[str, Any]]:
    """Aggregate source rows, retain target metadata, and record Complex Portal matches."""
    source = pd.DataFrame(data)[list(SOURCE_COLUMNS)].drop_duplicates()
    source = source.astype({LIGAND_ID_COLUMN: "string", "Target ID": "string"})
    source = source.dropna(subset=["Target ID", LIGAND_ID_COLUMN])
    source = source[source["Target ID"].str.strip().ne("") & source[LIGAND_ID_COLUMN].str.strip().ne("")]

    aggregations: dict[str, Any] = {PUBLICATIONS_COLUMN: _join_publications}
    prepared = source.groupby(list(GROUP_COLUMNS), as_index=False, dropna=False).agg(aggregations)
    prepared = prepared.rename(columns=PREPARED_COLUMN_RENAMES)
    prepared["subject_id"] = prepared[LIGAND_ID_COLUMN].astype(str).str.strip().map(ligand_mapping)
    prepared = prepared.dropna(subset=["subject_id"]).drop_duplicates()
    records = prepared.to_dict(orient="records")
    for record in records:
        key = TargetDescriptor.from_record(record).complex_member_key
        record["complex_portal_id"] = complex_portal_index.get(key, "") if key else ""
    return records


@koza.prepare_data(tag="gtopdb_interaction_parsing")
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Prepare GtoPdb interactions for record-level graph transformation."""
    input_files_dir = Path(koza.input_files_dir)
    return _prepare_interactions(
        data, _load_ligand_mapping(input_files_dir), _load_complex_portal_index(input_files_dir)
    )


def _publication_list(value: str | None) -> list[str] | None:
    """Prefix publication tokens without changing whitespace or multiplicity.

    >>> _publication_list("123|123|| 456 ")
    ['PMID:123', 'PMID:123', 'PMID:', 'PMID: 456 ']
    >>> _publication_list("") is None
    True
    """
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
            in_taxon=[target.species_context_qualifier] if target.species_context_qualifier else None,
        )
    elif target.complex_curie:
        object = MacromolecularComplex(
            id=target.complex_curie,
            name=target.name,
            # Without a Complex Portal match, IUPHARobj identifies the source target
            # concept for every species, and the taxon keeps human target 378
            # (5-HT3AB) distinct in meaning from its mouse descriptor.
            in_taxon=[target.species_context_qualifier] if target.species_context_qualifier else None,
            xref=[target.iuphar_curie] if target.complex_curie != target.iuphar_curie else None,
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
            in_taxon=[target.species_context_qualifier] if target.species_context_qualifier else None,
        )
        for accession in target.canonical_uniprot_ids
    ]


def _interaction_edges(
    subject: ChemicalEntity,
    object: NamedThing,
    endogenous: Any,
    rule: InteractionRule,
    species_context_qualifier: str | None,
    source_record_url: str,
) -> list[Association]:
    """Build the optional primary and physical edges, each with its own qualifiers.

    Endogenous projection applies only to an affects/regulates primary edge.
    Related-to edges remain unqualified; physical edges carry their own mechanism
    and species context without asserting an effect on activity.
    """
    common_fields: dict[str, Any] = {
        "subject": subject.id,
        "object": object.id,
        "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
        "agent_type": AgentTypeEnum.manual_agent,
    }
    edges: list[Association] = []
    primary = rule.primary
    if primary is not None:
        if primary.relation == "related":
            edges.append(
                Association(
                    id=entity_id(),
                    predicate=BIOLINK_RELATED,
                    sources=_gtopdb_sources(source_record_url),
                    **common_fields,
                )
            )
        else:
            predicate, direction = _endogenous_projection(endogenous, primary)
            edges.append(
                ChemicalAffectsGeneAssociation(
                    id=entity_id(),
                    predicate=predicate,
                    qualified_predicate=BIOLINK_CAUSES if primary.qualified else None,
                    object_aspect_qualifier=primary.aspect,
                    object_direction_qualifier=direction,
                    causal_mechanism_qualifier=primary.mechanism,
                    species_context_qualifier=species_context_qualifier,
                    sources=_gtopdb_sources(source_record_url),
                    **common_fields,
                )
            )
    if rule.physical is not None:
        edges.append(
            PairwiseMolecularInteraction(
                id=entity_id(),
                predicate="biolink:directly_physically_interacts_with",
                causal_mechanism_qualifier=rule.physical.mechanism,
                species_context_qualifier=species_context_qualifier,
                sources=_gtopdb_sources(source_record_url),
                **common_fields,
            )
        )
    return edges


def _endogenous_projection(
    endogenous: Any,
    rule: PrimaryAssociationRule,
) -> tuple[str, DirectionQualifierEnum | None]:
    """Select endogenous context from any true-like source value, including bools.

    GtoPdb and the Koza pipeline deliver the flag as the lowercase string
    "true"/"false", so matching is case-insensitive.

    >>> rule = PrimaryAssociationRule(polarity="positive")
    >>> predicate, direction = _endogenous_projection("true", rule)
    >>> predicate, direction.value
    ('biolink:regulates', 'upregulated')
    >>> predicate, direction = _endogenous_projection(True, rule)
    >>> predicate, direction.value
    ('biolink:regulates', 'upregulated')
    >>> predicate, direction = _endogenous_projection("FALSE", rule)
    >>> predicate, direction.value
    ('biolink:affects', 'increased')
    >>> _endogenous_projection(None, PrimaryAssociationRule())
    ('biolink:affects', None)
    """
    if str(endogenous).strip().lower() == "true":
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

    direction = directions.get(rule.polarity) if rule.polarity is not None else None
    return predicate, direction


def _transform_record(record: dict[str, Any], target: TargetDescriptor) -> KnowledgeGraph | None:
    """Skip unsupported records, then assemble nodes, interaction edges, and evidence.

    ``IUPHARobj:<Target ID>`` is shared by species descriptors, so each
    ``has_part`` edge carries the descriptor's ``species_context_qualifier``.
    Human and mouse target 378 (5-HT3AB) therefore keep separate component
    sets under one complex node (biolink/biolink-model#1797, Biolink 4.4.6;
    translator-ingests#510).
    """
    if target.classification in UNSUPPORTED_TARGET_CLASSES:
        return None

    rule = resolve_rule(record["Type"], record["Action"])
    if rule is None:
        return None

    subject, object = _nodes_for_record(record, target)
    endogenous = record["Endogenous"]
    publications = _publication_list(record[PUBLICATIONS_COLUMN])
    edges = _interaction_edges(
        subject,
        object,
        endogenous,
        rule,
        target.species_context_qualifier,
        source_record_url=gtopdb_ligand_record_url(record[LIGAND_ID_COLUMN]),
    )

    component_nodes = _component_nodes(target)
    edges.extend(
        Association(
            id=entity_id(),
            subject=object.id,
            predicate="biolink:has_part",
            object=component.id,
            species_context_qualifier=target.species_context_qualifier,
            sources=_gtopdb_sources(gtopdb_target_record_url(target.source_id)),
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        )
        for component in component_nodes
    )
    if publications:
        for edge in edges:
            edge.publications = publications
    return KnowledgeGraph(nodes=[subject, object, *component_nodes], edges=edges)


@koza.transform(tag="gtopdb_interaction_parsing")
def transform_ingest_all(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[KnowledgeGraph]:
    """Aggregate record graphs and report exclusions."""
    records = list(data)
    targets = tuple(TargetDescriptor.from_record(record) for record in records)
    nodes: list[NamedThing] = []
    edges: list[Association] = []

    for record, target in zip(records, targets, strict=True):
        graph = _transform_record(record, target)
        if graph is not None:
            nodes.extend(graph.nodes)
            edges.extend(graph.edges)

    unsupported_target_counts = Counter(
        target.classification for target in targets if target.classification in UNSUPPORTED_TARGET_CLASSES
    )
    for classification, count in unsupported_target_counts.items():
        record_word = "record" if count == 1 else "records"
        koza.log(
            f"Excluded {count} GtoPdb interaction {record_word} with a "
            f"{classification.value} target; no unsupported target CURIE was emitted.",
            level="WARNING",
        )

    return [KnowledgeGraph(nodes=nodes, edges=edges)]
