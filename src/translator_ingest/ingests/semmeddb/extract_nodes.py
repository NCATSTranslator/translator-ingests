import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional

import koza
from koza.model.graphs import KnowledgeGraph

from biolink_model.datamodel.pydanticmodel_v2 import (
    AnatomicalEntity,
    ChemicalEntity,
    Disease,
    Gene,
    NamedThing,
    PhenotypicFeature,
    Protein,
)


# minimal, biolink-centric node extraction from semmeddb edges
# - streams jsonl edges to avoid memory issues
# - creates nodes for subject and object using biolink pydantic classes
# - deduplicates by id using koza.state


def _iter_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    """stream json objects from a jsonl file."""
    with path.open("r") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


PREFIX_TO_CLASS = {
    "NCBIGene": Gene,
    "HGNC": Gene,
    "ENSEMBL": Gene,
    "PR": Protein,
    "UniProtKB": Protein,
    "CHEBI": ChemicalEntity,
    "DRUGBANK": ChemicalEntity,
    "MONDO": Disease,
    "DOID": Disease,
    "HP": PhenotypicFeature,
    "UBERON": AnatomicalEntity,
}


def _make_node(curie: str) -> NamedThing:
    """construct a biolink node instance from a curie.

    - chooses class by prefix; falls back to NamedThing
    - uses class default category to remain biolink-compliant
    """
    if ":" not in curie:
        # malformed id, still emit as NamedThing to retain referential integrity
        return NamedThing(id=curie, category=NamedThing.model_fields["category"].default)

    prefix = curie.split(":", 1)[0]
    cls = PREFIX_TO_CLASS.get(prefix, NamedThing)
    return cls(id=curie, category=cls.model_fields["category"].default)


@koza.prepare_data()
def prepare_edges(koza: koza.KozaTransform, data: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    """yield edge records from input jsonl.

    koza yaml will declare a file, but we allow env override for flexibility.
    """
    # environment overrides; default to filtered output if present
    env_in: Optional[str] = os.getenv("SEM_MED_IN")
    candidates: list[Path] = []
    if env_in:
        candidates.append(Path(env_in))
    candidates.extend(
        [
            Path("data/semmeddb/semmeddb_edges.jsonl"),
            Path("data/semmeddb_kg2_kgx/kg2.10.3-semmeddb-edges.jsonl"),
        ]
    )

    input_path: Optional[Path] = next((p for p in candidates if p.exists()), None)
    if not input_path:
        # no file found; yield nothing
        return

    # store for logging in on_data_end if desired
    koza.state["semmeddb_input_path"] = str(input_path)

    for record in _iter_jsonl(input_path):
        yield record


@koza.on_data_begin()
def on_begin(koza: koza.KozaTransform) -> None:
    # seen set to deduplicate node emissions
    koza.state["seen_node_ids"] = set()


@koza.transform_record()
def extract_nodes(koza: koza.KozaTransform, record: Dict[str, Any]) -> KnowledgeGraph | None:
    """produce nodes for subject and object from a semmeddb edge record."""
    subject: Optional[str] = record.get("subject")
    object_: Optional[str] = record.get("object")
    if not subject and not object_:
        return None

    seen: set[str] = koza.state["seen_node_ids"]
    nodes: list[NamedThing] = []

    if subject and subject not in seen:
        nodes.append(_make_node(subject))
        seen.add(subject)
    if object_ and object_ not in seen:
        nodes.append(_make_node(object_))
        seen.add(object_)

    if not nodes:
        return None
    return KnowledgeGraph(nodes=nodes, edges=[])


def _resolve_yaml_path() -> Path:
    # resolve adjacent yaml config file path
    return Path(__file__).with_name("semmeddb_nodes.yaml")


if __name__ == "__main__":
    # direct execution fallback using biolink pydantic, avoiding koza runner tag discovery issues
    # determines input and writes unique nodes to kgx jsonl
    env_in: Optional[str] = os.getenv("SEM_MED_IN")
    candidates: list[Path] = []
    if env_in:
        candidates.append(Path(env_in))
    candidates.extend(
        [
            Path("data/semmeddb/semmeddb_edges.jsonl"),
            Path("data/semmeddb_kg2_kgx/kg2.10.3-semmeddb-edges.jsonl"),
        ]
    )
    input_path: Optional[Path] = next((p for p in candidates if p.exists()), None)
    if not input_path:
        raise SystemExit(
            "No SemMedDB edges file found. Set SEM_MED_IN or place file in data/semmeddb/semmeddb_edges.jsonl"
        )

    out_nodes = Path(os.getenv("SEM_MED_NODES_OUT", "data/semmeddb/semmeddb_nodes.jsonl"))
    out_nodes.parent.mkdir(parents=True, exist_ok=True)

    seen: set[str] = set()
    written = 0
    with out_nodes.open("w") as outf:
        for rec in _iter_jsonl(input_path):
            for key in ("subject", "object"):
                curie = rec.get(key)
                if not curie or curie in seen:
                    continue
                node = _make_node(curie)
                outf.write(json.dumps(node.model_dump(exclude_none=True)) + "\n")
                seen.add(curie)
                written += 1
    print(f"Wrote {written} nodes to {out_nodes}")
