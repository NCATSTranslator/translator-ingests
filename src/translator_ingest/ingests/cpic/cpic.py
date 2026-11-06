"""Ingest published CPIC gene–drug pairs, preserving the draft RIG's FDA filter.

The API is the source of the published/provisional tables and offers stable
accessions in addition to the names in the TSV. No ClinPGx ingest code is needed.
"""

import json
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
    Drug,
    Gene,
    KnowledgeLevelEnum,
    NamedThing,
)
from koza.model.graphs import KnowledgeGraph

from translator_ingest.util.biolink import INFORES_CLINPGX, build_association_knowledge_sources

INFORES_CPIC = "infores:cpic"
PREDICATE = "biolink:associated_with_response_to"
ACCESSION = "PharmGKB Accession Id"
AUDIT_FIELDS = (
    "pairid",
    "cpgxgeneid",
    "cpgxdrugid",
    "clinpgxid",
    "cpiclevel",
    "pgkbcalevel",
    "pgxtesting",
    "provisional",
    "usedforrecommendation",
)


def get_latest_version() -> str:
    """Label the rolling API download by UTC retrieval day, not an upstream release."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def numeric_curie(value: str, prefix: str) -> str | None:
    """Map exactly one positive numeric identifier without guessing ambiguous IDs.

    >>> numeric_curie(' 123 ', 'NCBIGene')
    'NCBIGene:123'
    >>> numeric_curie('123, 456', 'NCBIGene') is None
    True
    """
    value = value.strip()
    return f"{prefix}:{value}" if re.fullmatch(r"[1-9][0-9]*", value) else None


def publications(values: list[str] | None) -> list[str]:
    """Validate and deduplicate the API's PMID array.

    >>> publications(['123', 'PMID:456', '123'])
    ['PMID:123', 'PMID:456']
    >>> publications(None)
    []
    """
    if values is not None and not isinstance(values, list):
        raise ValueError("CPIC pmids must be a list")
    result = set()
    for value in values or []:
        curie = numeric_curie(str(value).strip().removeprefix("PMID:"), "PMID")
        if curie is None:
            raise ValueError(f"Invalid CPIC PMID: {value!r}")
        result.add(curie)
    return sorted(result)


def load_entities(directory: Path) -> dict[tuple[str, str], NamedThing]:
    """Join on source accessions, requiring one supported ID per entity."""
    entities: dict[tuple[str, str], NamedThing] = {}
    for table, kind, id_column, prefix, node_class in (
        ("genes", "gene", "NCBI Gene ID", "NCBIGene", Gene),
        ("chemicals", "drug", "PubChem Compound Identifiers", "PUBCHEM.COMPOUND", Drug),
        ("drugs", "drug", "PubChem Compound Identifiers", "PUBCHEM.COMPOUND", Drug),
    ):
        with ZipFile(directory / f"{table}.zip") as archive:
            with archive.open(f"{table}.tsv") as stream:
                rows = pd.read_csv(
                    stream, sep="\t", usecols=[ACCESSION, "Name", id_column], dtype=str, keep_default_na=False
                ).to_dict(orient="records")
        for row in rows:
            accession = row[ACCESSION].strip()
            identifier = numeric_curie(row[id_column], prefix)
            if not accession or identifier is None:
                continue
            key = (kind, accession)
            if key in entities and entities[key].id != identifier:
                raise ValueError(f"Conflicting CPIC entity mapping for {accession}")
            entities[key] = node_class(id=identifier, name=row["Name"], xref=[f"PharmGKB:{accession}"])
    return entities


def read_pairs(path: Path) -> list[dict[str, Any]]:
    """Validate the API envelope and the scope-critical flag before filtering."""
    payload = json.loads(path.read_text())
    if payload.get("status") != "success" or not isinstance(payload.get("data"), list):
        raise ValueError("Expected a successful CPIC API response containing a data list")
    required = {"pairid", "provisional", "cpiclevel", "pgxtesting", "cpgxgeneid", "cpgxdrugid", "clinpgxid"}
    for row in payload["data"]:
        if not isinstance(row, dict) or not required.issubset(row):
            raise ValueError("CPIC pair is missing required source fields")
        if type(row["provisional"]) is not bool:
            raise ValueError("CPIC provisional must be a boolean")
        if not isinstance(row["cpiclevel"], str) or not row["cpiclevel"].strip():
            raise ValueError("CPIC pair must have a nonempty level")
    return payload["data"]


@koza.prepare_data()
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    """Filter published pairs, map IDs, and merge duplicate assertions and evidence."""
    directory = Path(koza.input_files_dir)
    entities = load_entities(directory)
    counts: Counter[str] = Counter(
        {
            key: 0
            for key in (
                "input_rows",
                "excluded_provisional",
                "excluded_retired",
                "excluded_no_clinical_pgx",
                "excluded_unmapped_or_ambiguous_identifier",
                "included_rows",
            )
        }
    )
    pairs: dict[tuple[str, str], dict[str, Any]] = {}
    nodes: dict[str, NamedThing] = {}
    level_counts: Counter[str] = Counter()
    for row in read_pairs(directory / "pairs.json"):
        counts["input_rows"] += 1
        if row["provisional"]:
            counts["excluded_provisional"] += 1
            continue
        if row["cpiclevel"].strip().casefold() == "retired":
            counts["excluded_retired"] += 1
            continue
        if (row["pgxtesting"] or "").strip().casefold() == "no clinical pgx":
            counts["excluded_no_clinical_pgx"] += 1
            continue
        gene = entities.get(("gene", row["cpgxgeneid"]))
        drug = entities.get(("drug", row["cpgxdrugid"]))
        if gene is None or drug is None:
            counts["excluded_unmapped_or_ambiguous_identifier"] += 1
            continue
        for node in (gene, drug):
            if node.id not in nodes:
                nodes[node.id] = node.model_copy(deep=True)
            else:
                nodes[node.id].xref = sorted(set(nodes[node.id].xref or []) | set(node.xref or []))
        key = (gene.id, drug.id)
        if key not in pairs:
            pairs[key] = {
                "id": str(uuid5(NAMESPACE_URL, f"{INFORES_CPIC}|{gene.id}|{PREDICATE}|{drug.id}")),
                "subject": gene.id,
                "object": drug.id,
                "publications": set(),
                "guidelines": set(),
                "annotations": [],
            }
        pair = pairs[key]
        pair["publications"].update(publications(row.get("pmids")))
        if row["clinpgxid"]:
            if not re.fullmatch(r"PA[0-9]+", row["clinpgxid"]):
                raise ValueError(f"Invalid CPIC guideline accession: {row['clinpgxid']!r}")
            pair["guidelines"].add(f"https://www.clinpgx.org/guideline/{row['clinpgxid']}")
        annotation = {field: row.get(field) for field in AUDIT_FIELDS}
        if annotation not in pair["annotations"]:
            pair["annotations"].append(annotation)
        counts["included_rows"] += 1
        level_counts[row["cpiclevel"]] += 1

    for pair in pairs.values():
        pair["publications"] = sorted(pair["publications"])
        pair["guidelines"] = sorted(pair["guidelines"])
        pair["annotations"].sort(key=lambda record: json.dumps(record, sort_keys=True))
    counts.update(output_nodes=len(nodes), output_edges=len(pairs), merged_rows=counts["included_rows"] - len(pairs))
    koza.state["nodes"] = nodes
    koza.state["written_nodes"] = set()
    koza.transform_metadata["cpic"] = {
        "counts": dict(counts),
        "included_cpic_levels": dict(sorted(level_counts.items())),
        "pair_annotations": {pair["id"]: pair["annotations"] for pair in pairs.values()},
    }
    koza.log(f"CPIC counts: {dict(counts)}", level="INFO")
    return [pairs[key] for key in sorted(pairs)]


@koza.transform_record()
def transform_row(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph:
    """Emit the draft RIG's response association with publication and retrieval provenance."""
    nodes = []
    for identifier in (record["subject"], record["object"]):
        if identifier not in koza.state["written_nodes"]:
            nodes.append(koza.state["nodes"][identifier])
            koza.state["written_nodes"].add(identifier)
    edge = Association(
        id=record["id"],
        subject=record["subject"],
        predicate=PREDICATE,
        object=record["object"],
        publications=record["publications"] or None,
        sources=build_association_knowledge_sources(
            primary=(INFORES_CPIC, record["guidelines"]),
            aggregating=INFORES_CLINPGX,
        ),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.text_mining_agent,
    )
    return KnowledgeGraph(nodes=nodes, edges=[edge])


@koza.on_data_end()
def finish(koza: koza.KozaTransform) -> None:
    """Ensure an empty result still creates both graph files for pipeline consumers."""
    if not koza.state["written_nodes"]:
        # Empty iterators initialize Koza's lazy output files; empty lists do not.
        koza.writer.write_nodes(iter(()))
        koza.writer.write_edges(iter(()))
