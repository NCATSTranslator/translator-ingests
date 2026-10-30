"""Transform ClinPGx's positive gene–chemical relationship summaries.

This first increment follows the PGKG PharmGKB parser's positive-association
filter, without inferring drug response, causality, or variant-to-gene mappings.
"""

import re
from collections import Counter
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5
from zipfile import ZipFile

import koza
import pandas as pd
from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    Association,
    ChemicalEntity,
    Gene,
    KnowledgeLevelEnum,
    NamedThing,
)
from koza.model.graphs import KnowledgeGraph

from translator_ingest.util.biolink import INFORES_CLINPGX, build_association_knowledge_sources

PREDICATE = "biolink:associated_with"
SOURCES = build_association_knowledge_sources(primary=INFORES_CLINPGX)
ACCESSION = "PharmGKB Accession Id"
RELATIONSHIP_COLUMNS = ["Entity1_id", "Entity1_type", "Entity2_id", "Entity2_type", "Evidence", "Association", "PMIDs"]


def get_latest_version() -> str:
    """Use the UTC retrieval day for the rolling downloads, not a release claim."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def read_table(directory: Path, name: str, columns: list[str]) -> pd.DataFrame:
    """Read required columns from a ZIP member; missing files/columns fail loudly."""
    with ZipFile(directory / f"{name}.zip") as archive:
        with archive.open(f"{name}.tsv") as stream:
            # Some chemical fields exceed Python csv's default field-size limit.
            return pd.read_csv(stream, sep="\t", usecols=columns, dtype=str, keep_default_na=False)


def numeric_curie(value: str, prefix: str) -> str | None:
    """Accept one positive integer ID, avoiding guessed or ambiguous mappings.

    >>> numeric_curie(' 7157 ', 'NCBIGene')
    'NCBIGene:7157'
    >>> numeric_curie('100288687, 22947', 'NCBIGene') is None
    True
    >>> numeric_curie('nan', 'PUBCHEM.COMPOUND') is None
    True
    """
    value = value.strip()
    return f"{prefix}:{value}" if re.fullmatch(r"[1-9][0-9]*", value) else None


def parse_publications(value: str) -> list[str]:
    """Normalize the source's semicolon-separated PMIDs and remove duplicates.

    >>> parse_publications('123; PMID:456;123; ')
    ['PMID:123', 'PMID:456']
    """
    publications = set()
    for item in value.split(";"):
        item = item.strip()
        if not item:
            continue
        curie = numeric_curie(item.removeprefix("PMID:"), "PMID")
        if curie is None:
            raise ValueError(f"Invalid ClinPGx PMID: {item!r}")
        publications.add(curie)
    return sorted(publications)


def load_entities(directory: Path) -> dict[tuple[str, str], NamedThing]:
    """Index unambiguously mapped source entities, checking overlapping drug tables."""
    entities: dict[tuple[str, str], NamedThing] = {}
    for table, entity_type, id_column, prefix, node_class in (
        ("genes", "Gene", "NCBI Gene ID", "NCBIGene", Gene),
        ("chemicals", "Chemical", "PubChem Compound Identifiers", "PUBCHEM.COMPOUND", ChemicalEntity),
        ("drugs", "Chemical", "PubChem Compound Identifiers", "PUBCHEM.COMPOUND", ChemicalEntity),
    ):
        for row in read_table(directory, table, [ACCESSION, "Name", id_column]).to_dict(orient="records"):
            accession = row[ACCESSION].strip()
            identifier = numeric_curie(row[id_column], prefix)
            if not accession or identifier is None:
                continue
            key = (entity_type, accession)
            if key in entities and entities[key].id != identifier:
                raise ValueError(f"Conflicting ClinPGx identifier mappings for {accession}")
            entities[key] = node_class(id=identifier, name=row["Name"], xref=[f"PharmGKB:{accession}"])
    return entities


@koza.prepare_data()
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    """Select the initial scope and merge reciprocal/duplicate rows with PMID union."""
    directory = Path(koza.input_files_dir)
    entities = load_entities(directory)
    counts: Counter[str] = Counter(
        {
            "input_rows": 0,
            "included_rows": 0,
            "excluded_not_positive": 0,
            "excluded_outside_initial_scope": 0,
            "excluded_unmapped_or_ambiguous_identifier": 0,
        }
    )
    evidence: Counter[str] = Counter()
    pairs: dict[tuple[str, str], dict[str, Any]] = {}
    nodes: dict[str, NamedThing] = {}

    for row in read_table(directory, "relationships", RELATIONSHIP_COLUMNS).to_dict(orient="records"):
        counts["input_rows"] += 1
        if row["Association"] != "associated":
            counts["excluded_not_positive"] += 1
            continue
        if {row["Entity1_type"], row["Entity2_type"]} != {"Gene", "Chemical"}:
            counts["excluded_outside_initial_scope"] += 1
            continue
        gene_side = "1" if row["Entity1_type"] == "Gene" else "2"
        chemical_side = "2" if gene_side == "1" else "1"
        gene = entities.get(("Gene", row[f"Entity{gene_side}_id"]))
        chemical = entities.get(("Chemical", row[f"Entity{chemical_side}_id"]))
        if gene is None or chemical is None:
            counts["excluded_unmapped_or_ambiguous_identifier"] += 1
            continue
        for node in (gene, chemical):
            if node.id not in nodes:
                nodes[node.id] = node.model_copy(deep=True)
            else:
                nodes[node.id].xref = sorted(set(nodes[node.id].xref or []) | set(node.xref or []))
        key = (gene.id, chemical.id)
        if key not in pairs:
            pairs[key] = {"subject": gene.id, "object": chemical.id, "publications": set()}
        pairs[key]["publications"].update(parse_publications(row["PMIDs"]))
        counts["included_rows"] += 1
        evidence.update(label.strip() for label in row["Evidence"].split(",") if label.strip())

    for pair in pairs.values():
        pair["publications"] = sorted(pair["publications"])
    counts["output_edges"] = len(pairs)
    counts["output_nodes"] = len(nodes)
    counts["merged_rows"] = counts["included_rows"] - len(pairs)
    koza.state["nodes"] = nodes
    koza.state["written_nodes"] = set()
    koza.transform_metadata["clinpgx"] = {
        "counts": dict(counts),
        "included_evidence_labels_by_source_row": dict(sorted(evidence.items())),
    }
    archive_dates = {}
    for table in ("relationships", "genes", "chemicals", "drugs"):
        with ZipFile(directory / f"{table}.zip") as archive:
            archive_dates[table] = sorted(
                name.removeprefix("CREATED_").removesuffix(".txt")
                for name in archive.namelist()
                if re.fullmatch(r"CREATED_\d{4}-\d{2}-\d{2}\.txt", name)
            )
    koza.transform_metadata["clinpgx"]["archive_creation_dates"] = archive_dates
    koza.log(f"ClinPGx initial-scope counts: {dict(counts)}", level="INFO")
    return [pairs[key] for key in sorted(pairs)]


@koza.transform_record()
def transform_row(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph:
    """Emit a generic association without claiming a particular drug response."""
    nodes = []
    for identifier in (record["subject"], record["object"]):
        if identifier not in koza.state["written_nodes"]:
            nodes.append(koza.state["nodes"][identifier])
            koza.state["written_nodes"].add(identifier)
    edge = Association(
        id=str(uuid5(NAMESPACE_URL, f"{INFORES_CLINPGX}|{record['subject']}|{PREDICATE}|{record['object']}")),
        subject=record["subject"],
        predicate=PREDICATE,
        object=record["object"],
        publications=record["publications"] or None,
        sources=SOURCES,
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.automated_agent,
    )
    return KnowledgeGraph(nodes=nodes, edges=[edge])


@koza.on_data_end()
def finish(koza: koza.KozaTransform) -> None:
    """Create empty graph files when all rows are filtered, for pipeline consumers."""
    if not koza.state["written_nodes"]:
        # Koza opens output files lazily. Iterators initialize the writer even
        # when empty, while empty lists are skipped before files are opened.
        koza.writer.write_nodes(iter(()))
        koza.writer.write_edges(iter(()))
