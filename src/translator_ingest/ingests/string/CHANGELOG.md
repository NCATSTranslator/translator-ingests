# STRING ingest — design changelog

Tradeoff decisions made during the STRING ingest design. Newest entries first.
Captures the *why*, not the *what* — code reflects the current state; this file
records what we considered and rejected, so the next iteration can pick up
without re-deriving the reasoning.

## 2026-05-27 — Add STITCH protein–chemical edges under a `stitch_pcl` tag

**Decision.** Extend the ingest with STITCH (`9606.protein_chemical.links.v5.0.tsv.gz`)
as a second tagged reader inside the existing `string.yaml`:

- New tag `stitch_pcl` reads STITCH; existing PPI is now tagged `string_ppi` (was untagged).
- Edge type: `biolink:ChemicalEntity —interacts_with→ biolink:Protein`
- PSI-MI attribute: `MI:0190` (interaction — root term) attached via `has_attribute`
- `primary_knowledge_source: infores:stitch`; KL/AT both `not_provided`
- Same `>500` combined_score cutoff as STRING
- Chemical IDs: `CIDm{N}` / `CIDs{N}` → `PUBCHEM.COMPOUND:{int(N)}` (leading zeros stripped)

**Considered and dropped:**

| Idea | Why dropped now | Bring back when |
|---|---|---|
| New `stitch.yaml` + `stitch.py` + `stitch_rig.yaml` (separate ingest) | User picked tagged-readers-in-one-yaml to keep the two sister DBs co-located. Identifier schemes and scoring share too much to justify duplication. | Only if STITCH grows independent enough (different filtering, cadence, or team ownership) to warrant its own ingest cycle. |
| Per-`CIDm` vs `CIDs` distinction in the output | STITCH itself collapses them in the protein_chemical scoring; preserving the distinction would create duplicate edges with no extra signal. | If we ever need stereo-aware downstream filtering — but PubChem CIDs alone don't carry that info anyway. |
| Stronger predicate (e.g. `binds_to`, `affects`) | STITCH doesn't characterize the kind of interaction on a per-row basis. The combined_score aggregates binding assays, modulation, substrate relationships, etc. `interacts_with` is the honest umbrella. | When the `actions.v5.0.tsv` file (which DOES carry mode-of-action: `activation`, `inhibition`, `binding`, …) is added; predicates can then split by action. |
| Same `physically_interacts_with` predicate as STRING | Many STITCH rows reflect biochemical affinity without direct physical contact (text-mined co-mentions, database curation). Would re-create the predicate-overclaim we already noted for STRING's functional file. | If we ever restrict STITCH to only the binding-assay-derived subset. |

**Output sizes (5000-row integration fixture):** ~31 STITCH edges + 19 chemical nodes + ~14 additional protein nodes (some overlap with PPI proteins). Full human STITCH file is ~14.7M rows; estimated full-run output is ~200K-500K edges depending on the score distribution above 500.

**Parked.** Multi-organism STITCH (mouse, rat) is the natural next step — the transform already handles arbitrary taxa via the same `SUPPORTED_TAXA` dict used by `string_ppi`. The STITCH `actions.v5.0.tsv` file (mode-of-action) and `detailed`/`full` per-channel subscores variants are documented in `future_considerations`.

## 2026-05-26 — Hybrid: emit `equivalent_identifiers` on Protein nodes

**Decision.** Stay protein-native at the node level (ENSEMBL CURIEs as PPI subject/object) but populate `equivalent_identifiers` on each Protein with NCBIGene CURIEs from STRING's `all_organisms.entrez_2_string.tsv` mapping file.

**Why.** Kevin Schaper (Monarch, 2026-05) confirmed Monarch's pattern is to download the mapping file and emit gene-endpoint Gene nodes. Richard's earlier work was heading in the same direction. We chose the hybrid: keep our protein-endpoint output (still useful for protein-level reasoning) and add the gene equivalents as a node property so gene-centric consumers can pivot without the graph being reshaped. Lossless for both audiences.

**Mechanism.** `@koza.on_data_begin(tag="string_ppi")` loads the 285 MB mapping file once per run into `koza.state["string_to_entrez"]`. The transform looks up each protein's NCBIGene equivalents and sets `equivalent_identifiers` on the Protein node. Missing entries yield `None` (downstream NodeNormalizer still resolves most via UniProtKB).

**Rejected:** emitting separate `same_as` edges between Protein and Gene nodes. Would double node count for no informational gain — `equivalent_identifiers` is the conventional biolink slot for exactly this.

## 2026-05-20 — Downgrade `knowledge_level` to `not_provided`; attach PSI-MI `MI:0915`

**Decision.** STRING's `combined_score` is a computational aggregation across heterogeneous evidence channels — not an explicit curator claim. `knowledge_assertion` overclaims; use `not_provided`. Matches Automat's STRING-DB KP convention.

Also attach `MI:0915` (PSI-MI: physical association) to every PPI edge via the `has_attribute` slot. Makes the PSI-MI claim explicit alongside the biolink predicate, useful for downstream consumers that key off PSI-MI terms.

**Why this slot, not another:** `supporting_method_types` (referenced in IntAct's RIG) does not exist on `Association` / `PairwiseMolecularInteraction` in the current biolink_model. `has_attribute` was the cleanest available slot. A typed `biolink:Attribute` wrapper would be more canonical but requires a heavier code refactor; deferred.

## 2026-05-19 — Reduce scope to human-only, single predicate, no subscores

**Decision.** First-iteration scope is the smallest defensible STRING ingest:

- One edge type: `biolink:Protein —physically_interacts_with→ biolink:Protein`
- One species: human (NCBITaxon:9606)
- One file: `9606.protein.links.v12.0.txt.gz` (the 3-column variant — no subscores)
- One threshold: `combined_score > 500`
- KL/AT: `knowledge_assertion / not_provided`
- No STITCH (protein–chemical interactions)
- No per-channel predicates

**Considered and dropped (parked in `future_considerations` in the RIG):**

| Idea | Why dropped now | Bring back when |
|---|---|---|
| Seven Translator model organisms (mouse, rat, zebrafish, fly, worm, yeast) | Multiplies file count and complicates the reader yaml. Single-org keeps the first cut readable. | Human ingest is shipped and stable; add organisms one at a time. |
| Per-channel predicates (`coexpressed_with`, `homologous_to`, `related_to`, etc.) | Couldn't pin down a clean selection rule that matches both ORION source and Automat production — too speculative. | After EDA confirms a defensible rule, ideally one we can cross-validate against Automat. |
| Per-channel `knowledge_level` / `agent_type` (ORION's scheme) | Adds nuance the consumer doesn't currently need; the rule had edge cases. | If a consumer asks for evidence-type provenance. |
| Subscores as edge properties (matching Automat) | Requires switching to the `detailed` or `full` file. Drops naturally once we want per-channel anything. | Same trigger as per-channel predicates — they're coupled. |
| STITCH protein–chemical edges | Separate file format, separate predicate question, separate ID-normalization concerns. Doubles the test surface. | After PPI is shipped; standalone follow-up. |
| Two-threshold design (500 row, 750 per-channel) | Only meaningful if per-channel predicates exist. | Coupled to per-channel predicates. |
| Mapping STRING proteins to NCBI Gene IDs (Monarch's approach) | Loses information; requires the `entrez_2_string` mapping table; conflicts with our protein-native RIG declaration. | If downstream consumers explicitly want gene-level edges. |

**Why this is conservative.** Each future axis (organisms, channels, STITCH) is
independent and additive. No design choice we're making now prevents any of
them. The cost of starting small is one more iteration; the cost of starting
big is a complex first PR that's hard to review and harder to validate.

## 2026-05-19 — Automat (RENCI) findings inform but don't bind us

**Source.** <https://automat.renci.org/string-db/> exposes RENCI's
production-normalized STRING graph (human-only, v12.0, ~19k nodes, ~10.9M
edges). Probed via the `/cypher` endpoint.

**What we learned:**

- Production Automat ships **4 predicates**, not the 6 in the ORION source
  code: `coexpressed_with`, `physically_interacts_with`, `homologous_to`,
  `related_to`. So the ORION repo file we read is not what's actually deployed.
- All Automat edges carry `knowledge_level: knowledge_assertion` and
  `agent_type: manual_agent`, regardless of whether the underlying evidence is
  experimental or purely text-mined / transferred.
- Automat attaches **all 16 subscore columns** as edge properties (`Coexpression`,
  `Experiments`, `Homology`, the `*_transferred` variants, `Combined_score`,
  `species_context_qualifier`, etc.).
- Automat's effective `combined_score` threshold is much lower than STRING's
  recommended cutoffs: we saw real edges with `Combined_score: 152` and all
  native channels zero (only transferred evidence).
- Automat's predicate-selection rule is *not* a clean argmax. Probing showed
  cases where Coexpression-transferred at 55 produced `coexpressed_with` despite
  Textmining at 167 — implying a priority or tie-break we couldn't fully
  reverse-engineer without their source.

**Decision.** Use Automat as a source of **realistic test fixtures** (real
ENSEMBL IDs, real combined_score distributions) but do **not** attempt
byte-for-byte parity with Automat's predicate selection. The first-iteration
ingest emits a single predicate, sidestepping the question entirely. When we
revisit channel-aware predicates, Automat remains useful as a cross-validation
target — but our rule will be one we can document, not one we have to guess
at.

## 2026-05-19 — Version discovery via STRING JSON API, not HTML scraping

**Decision.** `get_latest_version()` calls
<https://string-db.org/api/json/version> which returns
`[{"string_version": "12.0", ...}]`. We prefix with `v` to match STRING's
download-URL convention (`/protein.links.v12.0/`).

**Alternative rejected.** ORION's reference parser scrapes the homepage HTML
for `string_database_version_dotted:`. Brittle against page redesigns; the
JSON endpoint is stable and documented.

**Alternative rejected.** Hard-coding `"v12.0"`. Defensible (STRING releases
infrequently) but requires a code change on every release. The JSON endpoint
is free.

## 2026-05-19 — RIG schema: switched from nested template to flat IntAct-style

**Decision.** Rewrote the RIG using the schema in
[intact_rig.yaml](../intact/intact_rig.yaml) (flat `terms_of_use`,
`subject_categories` plural, structured `edge_type_info` and `node_type_info`).

**Rationale.** 26 of the 27 RIGs in the repo use this schema; only the
unmodified template stub uses the older nested form. Going with the majority
makes the file diff-comparable against neighbors and avoids tooling surprises.

## 2026-05-19 — Codebase patterns adopted from IntAct, not Monarch's STRING

**Decision.** Use this repo's helpers:
- [`build_association_knowledge_sources`](../../util/biolink.py) for the `sources` field
- [`entity_id()`](../../util/transform_utils.py) for edge IDs (not raw `uuid.uuid1()`)
- `koza.state["seen"]` for symmetric-pair dedup (not `hasattr(koza, "seen_rows")`)
- Per-record `@koza.transform_record(...)` pattern as used by IntAct

**Rationale.** The Monarch reference code is from a different framework
generation. The IntAct ingest is the closest in-tree analogue
(protein–protein interactions, similar file format, similar predicate
question) and demonstrates the current idioms.

## 2026-05-19 — `INFORES_STRING` / `INFORES_STITCH` constants added

**Decision.** Added module-level constants to
[`util/biolink.py`](../../util/biolink.py) following the existing
`INFORES_*` convention.

**Why this is mostly bookkeeping.** The constants give typo protection and a
single `grep` target, but they're not required by any framework. The literal
string `"infores:string"` is the only thing that matters at runtime. We added
them for consistency with the other 30+ entries in the same file — a reviewer
scanning `INFORES_` would expect to find ours there.
