# ClinPGx ingest — initial implementation

This is the first increment of the ClinPGx ingest, based on the adjacent
`TranslatorPharcogenomicsKG` repository's `RIGs/ClinPGX_rig.yaml`,
`RIGs/ClinPGx_download.yaml`, and `PharmGKB_parser.ipynb`.
CPIC has its own RIG and source file; this module does not consume it.

## Implemented scope

- Read `relationships.tsv`, `genes.tsv`, `chemicals.tsv`, and `drugs.tsv` directly
  from the official ZIP downloads, without extracting archives to disk.
- Retain only `Association == associated` for Gene–Chemical pairs in either order.
- Map genes to `NCBIGene` and chemicals to `PUBCHEM.COMPOUND`, requiring a single
  positive numeric identifier. Skip missing or ambiguous identifiers, rather than
  emitting `nan` IDs or expanding a multi-gene accession into guessed assertions.
- Orient each edge gene → chemical, merge duplicate/reciprocal mapped pairs, and
  union their PMIDs. Keep records without PMIDs; do not manufacture citations.
- Emit `biolink:associated_with`, `knowledge_assertion`, `automated_agent`, and
  primary source `infores:clinpgx`. Edge IDs are stable across repeated transforms.
  Preserve original PharmGKB accessions as node cross-references.
- Record filtering/merging counts, source evidence-label counts, and archive
  creation dates in transform metadata. Evidence counts include reciprocal source
  rows and are **not independent evidence counts**.

The scope intentionally omits phenotypes, variants, alleles, gene–gene, and
chemical–chemical relationships. No variant is collapsed to its gene. Raw
Evidence labels and PK/PD flags are not yet represented on output edges.

## Run locally

From the repository root:

```sh
make transform SOURCES=clinpgx
uv run pytest tests/unit/ingests/clinpgx tests/config/test_source_config_yaml.py -k clinpgx
uv run pytest --doctest-modules src/translator_ingest/ingests/clinpgx/clinpgx.py
```

`get_latest_version()` labels the rolling download by its UTC retrieval day.
This is not an upstream release number. The transform records each archive's
`CREATED_YYYY-MM-DD.txt` marker separately; the pipeline retains source downloads.

## Decisions to review before release

1. **License:** all four downloads inspected on 2026-10-23 contain a
   `LICENSE.txt` stating **CC BY-NC-SA 4.0**, with additional redistribution wording
   and a link to the [ClinPGx data usage policy](https://www.clinpgx.org/page/dataUsagePolicy).
   The upstream RIG says CC BY 4.0. Resolve this discrepancy before graph inclusion.
   This ingest is not added to `graphs.yaml`.
2. **Source attribution:** the draft uses the existing `infores:clinpgx` constant.
   The older RIG says `infores:PharmGKB`; review attribution for the relationship
   summaries and their underlying annotation providers, including CPIC overlap.
3. **Knowledge level and agent type:** the notebook uses `automated_agent` while
   the RIG uses `text_mining_agent`. This draft follows the notebook for automated
   relationship summaries; confirm this and `knowledge_assertion` with Standards.
4. **Predicate and evidence:** retain the RIG's generic association rather than
   infer `associated_with_response_to`. Review Evidence and PK/PD semantics before
   introducing more specific predicates, qualifiers, or confidence claims.
5. **Broader scope:** the RIG's prose mentions only gene–drug associations (and
   accidentally names CPIC), but its edge table lists other entity combinations.
   Resolve the intended scope and phenotype/variant mapping before expanding.

The archive creation markers observed during development were 2026-10-05.
Tests use synthetic TSV fixtures, not redistributed source records.

## Initial snapshot validation

The four archives retrieved on 2026-10-23 were run through the production Koza
configuration and JSONL writer:

| Transform outcome | Count |
| --- | ---: |
| Input relationship rows | 246,714 |
| Excluded negative or ambiguous rows | 110,792 |
| Excluded positive rows outside the initial entity scope | 124,400 |
| Excluded positive gene–chemical rows without unambiguous mappings | 2,548 |
| Included source rows | 8,974 |
| Output edges after merging reciprocal/duplicate rows | 4,487 |
| Output nodes | 2,324 |

All output records passed their Biolink Pydantic model validation. Node IDs,
edge IDs, and edge triples were unique, every edge endpoint resolved to an output
node, and all publications were valid numeric PMID CURIEs. The 17 targeted tests
(including doctests and configuration checks) passed, as did RIG schema
validation. Node normalization and release-pipeline validation have not been run.
