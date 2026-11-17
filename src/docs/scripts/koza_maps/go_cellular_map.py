#!/usr/bin/env python3
"""
Script to generate the contents of the HMDB 'go_lookup.csv' Koza mapping file.
Although the mapping file only says 'go', in fact, for HMDB, it only contains 'cellular components' GO terms.
Note that this script as a dependency on OakLib which is specified in the 'koza_map' uv dependency group.
"""
import pandas as pd
from oaklib import get_adapter
from semsql.sqla.semsql import Statements
from translator_ingest import TRANSLATOR_INGEST_PATH

# Although this lookup file is constrained to just the GO Cellular Compartment terms,
# for practical convenience in accessing it, we simply name it the 'go_lookup' file.
go_cellular_lookup_file = TRANSLATOR_INGEST_PATH / "ingests" / "hmdb" / "maps" / "go_lookup.csv"

def normalize(text: str) -> str:
    return (
        text.lower()
            .strip()
            .replace("_", " ")
    )


adapter = get_adapter("sqlite:obo:go")

rows = []

print("Loading GO term namespaces...")
namespace_by_curie = {
    subject: value
    for subject, value in adapter.session.query(Statements.subject, Statements.value).filter(
        Statements.predicate == "oio:hasOBONamespace"
    )
}

print("Loading GO entities...")
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

print("... publishing mapping file.")
df.to_csv(
    go_cellular_lookup_file,
    sep="\t",
    index=False
)

print(
    f"Generated {len(df):,} mappings"
)