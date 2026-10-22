#!/usr/bin/env python3
"""
Script to generate the contents of the HMDB 'go_cellular_lookup.csv' Koza mapping file.
Note that this script as a dependency on OakLib which is specified in the 'koza_map' uv dependency group.
"""
import pandas as pd
from oaklib import get_adapter
from semsql.sqla.semsql import Statements
from translator_ingest import TRANSLATOR_INGEST_PATH

go_cellular_lookup_file = TRANSLATOR_INGEST_PATH / "ingests" / "hmdb" / "maps" / "go_cellular_lookup.csv"

def normalize(text: str) -> str:
    return (
        text.lower()
            .strip()
            .replace("_", " ")
    )


adapter = get_adapter("sqlite:obo:go")

rows = []

print("Loading GO term namespaces...")
#
# Bulk lookup of each GO term's OBO namespace ('cellular_component', 'biological_process',
# 'molecular_function') in a single query, rather than calling adapter.entity_metadata_map(curie)
# once per entity (which internally rescans the whole annotation-property table on every call,
# and whose result was being matched against the wrong key - 'namespace' instead of
# 'oio:hasOBONamespace' - so it never actually kept any term).
#
namespace_by_curie = {
    subject: value
    for subject, value in adapter.session.query(Statements.subject, Statements.value).filter(
        Statements.predicate == "oio:hasOBONamespace"
    )
}

print("Loading GO entities...")
n=0
for curie in adapter.entities():

    if not curie.startswith("GO:"):
        continue

    #
    # Keep only Cellular Component terms
    #
    if namespace_by_curie.get(curie) != "cellular_component":
        continue

    label = adapter.label(curie)

    if not label:
        continue

    # rudimentary progress bar for this long-lived script?
    n = n + 1
    if n%100 == 0:
        print(". ",sep=None, end=None)
        if n%1000 == 0:
            print("\n")

    rows.append(
        {
            "source_term": normalize(label),
            "go_id": curie,
            "go_label": label,
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
                    "go_id": curie,
                    "go_label": label,
                    "match_type": "synonym"
                }
            )

print(".. creating data frame...")
df = pd.DataFrame(rows)

print("...dropping duplicates...")
df.drop_duplicates(
    subset=["source_term", "go_id"],
    inplace=True
)

print("...sorting values...")
df.sort_values(
    ["source_term", "go_id"],
    inplace=True
)

print("... Publishing mapping file.")
df.to_csv(
    go_cellular_lookup_file,
    index=False
)

print(
    f"Generated {len(df):,} mappings"
)