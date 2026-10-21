import pandas as pd
from oaklib import get_adapter

# Local file
adapter = get_adapter("sqlite:obo:uberon")

rows = []

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

df = pd.DataFrame(rows)
df.drop_duplicates(
    subset=["source_term", "uberon_id"],
    inplace=True
)

df.to_csv("uberon_lookup.csv", index=False)

print(f"Generated {len(df):,} mappings")
