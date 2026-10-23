#!/usr/bin/env python3
"""
Script to generate the contents of the HMDB 'uberon_lookup.csv' Koza mapping file.
Note that this script as a dependency on OakLib which is specified in the 'koza_map' uv dependency group.
"""
import pandas as pd
from oaklib import get_adapter
from translator_ingest import TRANSLATOR_INGEST_PATH

uberon_lookup_file = TRANSLATOR_INGEST_PATH / "ingests" / "hmdb" / "maps" / "uberon_lookup.csv"

# Local file
adapter = get_adapter("sqlite:obo:uberon")

rows = []

print("Loading UBERON entities...")
for curie in adapter.entities():

    if not curie.startswith("UBERON:"):
        continue

    label = adapter.label(curie)

    if not label:
        continue

    rows.append({
        "source_term": label.lower(),
        "uberon_id": curie,
        "uberon_label": label,
        "match_type": "label"
    })

    aliases = adapter.entity_aliases(curie)

    if aliases:
        for alias in aliases:

            if alias.lower() == label.lower():
                continue

            rows.append({
                "source_term": alias.lower(),
                "uberon_id": curie,
                "uberon_label": label,
                "match_type": "alias"
            })

print(".. creating data frame...")
df = pd.DataFrame(rows)

print("...dropping duplicates...")
df.drop_duplicates(
    subset=["source_term", "uberon_id"],
    inplace=True
)

print("... publishing mapping file.")
df.to_csv(
    uberon_lookup_file,
    sep="\t",
    index=False
)

print(f"Generated {len(df):,} mappings")
