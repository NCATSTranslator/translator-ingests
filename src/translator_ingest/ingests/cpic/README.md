# CPIC ingest — initial implementation

This independent ingest implements the CPIC RIG and notebook from the adjacent
`TranslatorPharcogenomicsKG` checkout (commit `40b7ee5`). It does not import the
ClinPGx ingest or consume its `relationships.tsv`.

## Source and mapping

The upstream download YAML points to an HTML page and calls it a TSV. Instead,
this ingest downloads [the API used by that page](https://api.clinpgx.org/v1/cpic/pair)
as `pairs.json`. The response contains both published and provisional pairs.
The 319 non-provisional pairs matched the downloaded `cpic-genes-drugs.tsv` table
on 2026-10-23. The API additionally provides stable ClinPGx gene/drug accessions,
so mapping does not depend on matching drug names or aliases.

Entity mappings come from the official ClinPGx gene, chemical, and drug ZIP files.
Only single numeric NCBI Gene and PubChem Compound identifiers are accepted.
Missing or ambiguous mappings are counted and excluded. Overlapping drug and
chemical mappings must agree, or the transform fails.

The output follows the draft RIG:

- Gene → `biolink:associated_with_response_to` → Drug.
- `knowledge_assertion` and `text_mining_agent` (agent type requires review).
- Primary source `infores:cpic`, retrieved through aggregator `infores:clinpgx`.
- PMID citations and guideline URLs; duplicate mapped pairs union these values.
- Stable edge IDs and original PharmGKB accessions as node cross-references.

The transform excludes provisional and retired pairs and the RIG's
`No Clinical PGx` FDA-label category. Empty FDA labels are retained, as in the
upstream notebook. Level C is retained under the broad RIG scope, **not treated
as a prescribing recommendation**. Raw CPIC levels, ClinPGx evidence levels,
FDA categories, recommendation-use flags, and original pair IDs are retained in
`transform_metadata.cpic.pair_annotations`, keyed by output edge ID. They are not
yet emitted as edge attributes or converted into confidence scores.

## Run

From this worktree's root:

```sh
make transform SOURCES=cpic
uv run pytest tests/unit/ingests/cpic tests/config/test_source_config_yaml.py -k cpic
uv run pytest --doctest-modules src/translator_ingest/ingests/cpic/cpic.py
```

The download version is the UTC retrieval day, not an upstream release number.
No changes to `graphs.yaml` enable this draft in the default graph.

## Review before release

1. **Predicate scope and levels:** the current table includes many published
   level C pairs. Decide whether they support the RIG's response predicate or
   need a different representation/filter. Do not equate publication with
   clinical actionability.
2. **FDA-label filter:** the upstream rule excludes the current ondansetron–CYP2D6
   pair despite its CPIC level A. Review whether this filter belongs in the CPIC
   ingest at all. It is implemented explicitly for comparison with the RIG.
3. **Agent type:** both the RIG and notebook specify `text_mining_agent`; confirm
   whether curated CPIC assertions should instead use `manual_agent`.
4. **Evidence attributes:** agree on Biolink representations for the raw levels
   and FDA categories before moving them from audit metadata onto edges.
5. **License:** the original RIG says CC BY 4.0, but the required ClinPGx entity
   mapping ZIPs contain CC BY-NC-SA 4.0 licenses. Confirm the terms for the
   ClinPGx-hosted pair payload and mixed-source annotations before redistribution.

## Snapshot transform

On 2026-10-23, the API contained 574 pairs: 255 provisional and 319 published.
The transform excluded 4 retired pairs, 6 with No Clinical PGx labels, and 8
without unambiguous supported identifiers. It produced **301 edges and 190 nodes**.
Included CPIC levels were A: 90, B: 15, C: 196. The source downloads and generated
KGX are under the ignored `data/cpic-development-input` and
`data/cpic-development-output` directories in this worktree.

Tests use synthetic source data and the actual Koza configuration and JSONL
writer; no network calls or mocked transformation components are needed.
All 21 targeted tests (including doctests and configuration checks), RIG schema
validation, and Ruff checks passed. The snapshot output passed Biolink Pydantic
validation and checks for unique IDs/triples, endpoint integrity, numeric PMIDs,
and complete per-edge audit coverage. Node normalization and the release pipeline
have not yet been run.
