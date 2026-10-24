#!/usr/bin/env python3
"""
Script to generate the contents of the 'cl_lookup.csv' Koza mapping file,
mapping cell type names onto Cell Ontology (CL; http://purl.obolibrary.org/obo/cl.owl) terms.
Note that this script as a dependency on OakLib which is specified in the 'koza_map' uv dependency group.
"""
import pandas as pd
from oaklib import get_adapter
from translator_ingest import TRANSLATOR_INGEST_PATH

cl_lookup_file = TRANSLATOR_INGEST_PATH / "ingests" / "hmdb" / "maps" / "cl_lookup.csv"


def normalize(text: str) -> str:
    """
    Normalize a term label or synonym into a lookup key.

    >>> normalize("  T_Cell ")
    't cell'
    """
    return (
        text.lower()
            .strip()
            .replace("_", " ")
    )


adapter = get_adapter("sqlite:obo:cl")

rows = []

print("Loading CL entities...")
for curie in adapter.entities():

    # The CL release imports terms from other ontologies (UBERON, GO, PR, etc.),
    # so we only keep the native Cell Ontology terms
    if not curie.startswith("CL:"):
        continue

    label = adapter.label(curie)

    if not label:
        continue

    rows.append(
        {
            "source_term": normalize(label),
            "cl_id": curie,
            "cl_label": label,
            "match_type": "label"
        }
    )

    aliases = adapter.entity_aliases(curie)

    if aliases:
        for alias in aliases:

            alias = alias.strip()

            if not alias:
                continue

            rows.append(
                {
                    "source_term": normalize(alias),
                    "cl_id": curie,
                    "cl_label": label,
                    "match_type": "synonym"
                }
            )

print(".. creating data frame...")
df = pd.DataFrame(rows)

print("...dropping duplicates...")
# entity_aliases() also returns the label itself, so the 'label' rows
# (sorted first) are retained in preference to identical 'synonym' rows
df.sort_values(
    ["source_term", "cl_id", "match_type"],
    inplace=True
)
df.drop_duplicates(
    subset=["source_term", "cl_id"],
    inplace=True
)

print("... publishing mapping file.")
df.to_csv(
    cl_lookup_file,
    sep="\t",
    index=False
)

print(
    f"Generated {len(df):,} mappings"
)
