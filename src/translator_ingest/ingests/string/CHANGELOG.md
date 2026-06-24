# STRING ingest — design changelog

Tradeoff decisions made during the STRING ingest design. Newest entries first.
Captures the *why*, not the *what* — code reflects the current state; this file
records what we considered and rejected, so the next iteration can pick up
without re-deriving the reasoning.

## 2026-06-24 — Matt Brush's predicate mapping (`string-functional-predicates` branch)

**Decision.** Implement the channel→predicate mapping Matt Brush proposed in the
DINGO thread (2026-06-16), on a fresh revision branch for review (re-run the KGX
summary, then share with Matt/Sierra). Collapses STRING to **three** Protein↔Protein
predicates and drops every gene-family predicate:

| Signal | Predicate |
|---|---|
| experiments > 750 | `physically_interacts_with` |
| coexpression > 750 | `coexpressed_with` |
| **database** > 750 | **`functionally_interacts_with`** |
| **combined_score > 800**, no channel qualifies | **`functionally_interacts_with`** (fallback) |
| else | *(no edge)* |

`textmining`, `fusion`, `cooccurence`, `neighborhood`, `homology` are read but no
longer mint their own predicate — their signal flows only through `combined_score`.

**Why the changes vs. the prior iteration.**
- **Promote the `database` channel** to a predicate driver (we'd excluded it,
  ORION-style). Curated-DB evidence is high-value.
- **Re-point the fallback** from `physically_interacts_with` to
  `functionally_interacts_with`, and **raise the gate 500 → 800**. The old fallback
  over-claimed *physical* interaction for aggregate-only evidence (it was ~93% of
  the graph). Functional + >0.8 is the honest claim.
- **Drop text-mining as a standalone driver** (Translator convention on text-mined
  co-occurrence; cf. the `diseases` ingest). Removing its contribution from
  `combined_score` itself is a separate longer-term step (Matt).
- **Gene-family predicates dropped** — they're biolink domain/range = gene, invalid
  on Protein nodes (root of the `BIOLINK_SUBOBJ_ERRORS`), and Matt's mapping omits
  them. This also moots the cooccurence-450 tuning from the prior iteration.

**Open biolink gap (needs Sierra).** `biolink:functionally_interacts_with` is **not
in the Biolink Model** — `PairwiseMolecularInteraction` rejects it. It is emitted as
a generic **`Association`** (predicate accepted as a free CURIE there) pending
addition of the predicate to biolink. Until then, KGX/biolink validation will flag
these edges. Semantics (Matt): "two proteins participate in one or more common
biological processes, pathways, reactions, complexes, or cellular functions" (not
necessarily contributing in the same direction).

**Also reverted the config-driven threshold machinery** (the 2026-06-11
`transform.channel_thresholds` YAML block + `resolve_thresholds`/`DEFAULT_THRESHOLDS`).
Thresholds are now hardcoded module constants in `string_utils.py` (Kevin/Richard's
"keep it pythonic" call); the RIG documents the values. The Koza reader `filters`
(`combined_score > 500`) remains a coarse efficiency pre-filter only.

**Decided earlier, still holds:** `neighborhood_transferred` is not used (orthology-
projected; would mis-assert adjacency). Native `neighborhood` is empty for vertebrates.

**PSI-MI:** `physically_interacts_with` → MI:0915; `functionally_interacts_with` →
MI:2286 (functional interaction); `coexpressed_with` → none.

**Fixture output (200-row head-slices ×3 taxa):** 28 edges, all
`functionally_interacts_with` (22 from the database channel → knowledge_assertion /
manual_agent; 6 from the >800 combined fallback) — the slices contain no
experiments/coexpression rows above 750. **Tests:** 85 pass / 1 skip (unit +
integration + doctests) rewritten for the new mapping.

## 2026-06-11 — Config-driven per-channel filter thresholds (canonical in `string.py`)

**Decision.** Move the filter thresholds out of hardcoded module constants and
into **per-channel, YAML-declared config** that `string.py` consumes as the
single source of truth. Each channel is now an **independently tunable knob**,
validated against the EDA (`eda/`). Default values reproduce the prior
ORION-aligned behavior exactly (per-channel 750, combined 500), so this is a
pure mechanism refactor with no output change.

**Why the canonical filter lives in `string.py`, not Koza's `filters:`.** The
proper per-channel rule is "include a row if **any** channel clears **its own**
threshold (or combined_score clears the fallback gate)" — an **OR** across
per-column comparisons with different thresholds. Koza's declarative `filters:`
are **AND-only** (`koza/utils/row_filter.py` returns False on first miss; 8
filter codes; no OR/compound/nested support), so that semantics is **not
expressible** in the reader config. We therefore keep the per-channel logic in
the transform and use Koza's `filters:` only as a coarse efficiency pre-filter.

**Mechanism.** Thresholds are declared under a `transform.channel_thresholds:`
block in `string.yaml` and surfaced to the transform via
`KozaTransform.extra_fields` (the same pattern the `go_cam` ingest uses for its
`transform.filters`). `on_data_begin` resolves them once
(`resolve_thresholds(DEFAULT_THRESHOLDS + overrides)`) into
`koza_transform.state["thresholds"]`; the per-row transform reads that dict and
passes it to `predicates_for_row` / `passes_combined_score` /
`knowledge_level_and_agent_type_for_row` (all now accept an optional per-channel
`thresholds` dict, defaulting to the old constants so direct unit calls are
unchanged). Verified end-to-end that `extra_fields` carries the block.

**Reader pre-filter invariant.** The `combined_score > 500` Koza filter is now
documented as a *performance floor only*; it must stay `<= min(channel_thresholds)`
so it never pre-drops a row a channel would fire. This is provably safe for any
channel threshold `>= 501`: an EDA scan of all three taxa found
`combined_score >= (max native predicate-channel score) - 1` (max gap = 1), so a
row with `channel > T` always has `combined_score >= T`. Lowering a channel knob
to `<= 500` requires lowering the reader filter value to match (documented in
`string.yaml`).

**What this enables.** Independent per-channel filtering/validation — e.g.
lowering the `cooccurence` knob below 542 surfaces `genetically_interacts_with`
edges the uniform 750 gate hides (new doctest + unit tests cover this). Native
`neighborhood` stays empty at any threshold (EDA: 0 in every row).

**Out of scope (still open):** the gene-vs-protein predicate-set question
(Automat Protein/Protein meta_kg vs the Monarch precedent of NCBIGene nodes +
`PairwiseGeneToGeneInteraction` + generic `interacts_with`). This refactor is
agnostic to it — `CHANNEL_PREDICATES` is unchanged; only its thresholds became
tunable. Resolving that decision is the next step and will adjust the
channel→predicate map and the RIG `edge_type_info`.

**Tests:** existing 75 unit + 12 integration pass unchanged; added
`resolve_thresholds` defaults/overrides, per-channel `predicates_for_row`
override, default-equivalence, injected per-channel + combined-gate transform
tests, and a tunability doctest.

## 2026-06-11 — QA EDA + provider-meta_kg-constrained predicate set (`qa-string-ingest` branch)

**Context.** The "Custom Build Summary Jun 11 2026" normalized run emitted **0
`PairwiseGeneToGeneInteraction` edges** — neither `genetic_neighborhood_of` nor
`genetically_interacts_with` appeared. Opened the `qa-string-ingest` branch to
find out why and to QA the predicate model against production.

**EDA (all 3 taxa, ~40M rows each, streamed single-pass — see [`eda/`](../../../docs/ingests/string/eda/)).**

- Native `neighborhood` is **0 in every row** of human/mouse/rat → `genetic_neighborhood_of`
  can never fire. `neighborhood_transferred` exists (~4% of rows) but maxes at **385**.
- `cooccurence` maxes at **542** → never clears the 750 per-channel gate, so
  `genetically_interacts_with` never fires either.
- Predicate-driving channels are statistically **independent** (|r| < 0.15) — no
  overencoding; each carries distinct signal. No clean threshold band isolates the
  gene signal without molecular co-firing, and the whole band sits below STRING's
  confidence floor → splitting out gene predicates is unjustified by the data.
- Native `experiments` evidence is **human-skewed** (8.3% human / 1.0% mouse /
  0.2% rat) → curated physical edges are largely a human phenomenon.
- The `combined_score > 500` gate **never drops a row a per-channel predicate
  (>750) would have fired** (0 exceptions across 40M rows). Its only function is
  governing the volume of **fallback** edges (~1.1M kept per taxon vs ~13.7M
  rows). So today's 1.43M `physically_interacts_with` (93% of the graph) is
  **mostly fallback, not real physical evidence** — corroborated by its KL/AT
  carrying `not_provided` / `text_mining_agent`.

**Decision — constrain the predicate set to the Automat STRING-DB meta_kg.**
Per the standing rule that the RIG must not exceed what provider meta_kgs
support, fetched `https://automat.renci.org/string-db/meta_knowledge_graph`. The
provider supports exactly these **Protein→Protein** interaction triples:
`physically_interacts_with`, `coexpressed_with`, `homologous_to`, `related_to`
(plus `subclass_of` ontology backbone). No gene-family predicates; no
`interacts_with`. Resulting constrained mapping:

| channel | current predicate | constrained predicate |
|---|---|---|
| experiments > 750 | physically_interacts_with | **physically_interacts_with** (keep) |
| coexpression > 750 | coexpressed_with | **coexpressed_with** (keep) |
| textmining > 750 | interacts_with | **related_to** (remap — provider's choice) |
| fallback (combined > 500) | physically_interacts_with | **related_to** (fixes over-claim) |
| fusion > 750 | gene_fusion_with | **drop** |
| neighborhood > 750 | genetic_neighborhood_of | **drop** (empty + invalid) |
| cooccurence > 750 | genetically_interacts_with | **drop** (sub-threshold + invalid) |

**Why drop the gene-family predicates (not just leave them empty).** All three —
`genetically_interacts_with`, `genetic_neighborhood_of`, `gene_fusion_with` —
have biolink **domain/range = gene**, but STRING nodes are **Protein**. That is
the root cause of the `BIOLINK_SUBOBJ_ERRORS` in the build summary (e.g.
`gene_fusion_with | BAD SUBJECT | gene | protein`); the 732 `gene_fusion_with`
edges that shipped are already domain/range-violating. They are also absent from
the provider meta_kg. Both reasons point the same way.

**Why not introduce `neighborhood_transferred`.** It is orthology-projected
("interolog") evidence — `genetic_neighborhood_of` would assert a chromosomal
adjacency the actual pair doesn't have (the same semantic misread that excludes
HOMOLOGY). The only node-type-valid target is the generic `related_to`, which it
would merely duplicate — and at max 385 it yields **zero** edges at any
defensible threshold anyway.

**Net.** For these taxa STRING is a **protein-level interaction + coexpression**
source: 3 provider-supported predicates (`physically_interacts_with`,
`coexpressed_with`, `related_to`), all Protein-valid, removing every
domain/range warning. Implementation (transform `CHANNEL_PREDICATES` / fallback,
RIG `edge_type_info`) is the next step on this branch.

## 2026-05-28 — Split STITCH out of this ingest (preserved on `stitch-ingest` branch)

**Decision.** Remove the STITCH protein–chemical layer (the `stitch_pcl` tagged
reader, transform, helpers, RIG edge/node types, tests, and fixtures) from this
ingest so the PR ships **STRING-only**. The complete STITCH implementation is
preserved on the **`stitch-ingest` branch** (at commit `e2386459`) for a later,
properly-scoped reintegration.

**Why.** STITCH had accreted against the original 2026-05-19 scope-reduction
decision (which explicitly dropped it), and in its shipped form it was the
weakest possible version — a single `biolink:interacts_with` predicate,
`knowledge_level=not_provided`, no mode-of-action. Three findings made removal
the right call:

1. **No production precedent to match.** We probed Automat's STRING-DB MKG
   expecting to find STITCH protein-chemical edges. There are none. The only
   Protein↔ChemicalEntity edges in that graph are `subclass_of` ontology-backbone
   edges injected by **Ubergraph** (`primary_knowledge_source: infores:ubergraph`)
   — a protein *is-a* biomacromolecule *is-a* chemical entity via the Protein
   Ontology / ChEBI hierarchy. Not STITCH, not interactions. And there's no
   separate `stitch` KP in the Automat registry. So STITCH would be wholly
   net-new surface area, not a gap-filler.

2. **Net-new ⇒ value depends entirely on doing it well.** With no baseline to
   merely match, a bare `interacts_with` STITCH adds low-information edges that
   dilute the (strong) STRING deliverable. STITCH's real value lives in
   `actions.v5.0.tsv` mode-of-action predicates (binding / activation /
   inhibition / …), which the shipped form didn't have.

3. **The STRING ingest is the finished, defensible thing.** 6 per-channel
   predicates, per-channel KL/AT, multi-organism, ORION parity. Bundling a weak
   STITCH with it weakens the whole for a reviewer.

**Reintegration pointer.** When STITCH is taken up again:
- Start from the `stitch-ingest` branch (full implementation: CIDm/CIDs →
  PUBCHEM.COMPOUND parsing, ChemicalEntity → interacts_with → Protein,
  human/mouse/rat, tests, fixtures).
- Land `actions.v5.0.tsv` mode-of-action predicates **from the start** rather
  than the bare `interacts_with` form.
- Reconsider the ChemicalEntity identifier prefix (we used PUBCHEM.COMPOUND;
  worth checking against whatever chemical KP it will co-reside with).
- Decide whether it's a tagged reader in `string.yaml` again or its own
  `stitch.yaml` ingest — the original tagged-reader coupling made STRING and
  STITCH harder to evolve independently.

**Cost of removal:** surgical (STITCH was entangled across several commits), not
a clean `git revert`. 22 STITCH tests removed (113 STRING tests remain pass).

## 2026-05-28 — Per-channel KL/AT, reader-level threshold filter, helper cleanup

A round of TODO resolution on top of the per-channel predicate work.

**Per-channel knowledge_level / agent_type.** Previously uniform `not_provided`.
Now derived per row from the dominant (max-score) evidence channel, mirroring
ORION's STRING parser:

| Dominant channel | knowledge_level | agent_type |
|---|---|---|
| EXPERIMENTS, DATABASE | `knowledge_assertion` | `manual_agent` |
| COEXPRESSION, COOCCURENCE | `statistical_association` | `data_analysis_pipeline` |
| NEIGHBORHOOD, FUSION | `prediction` | `data_analysis_pipeline` |
| HOMOLOGY | `prediction` | `computational_model` |
| TEXTMINING | `not_provided` | `text_mining_agent` |

Plus an upgrade rule: if ≥2 channels exceed 750, KL becomes `knowledge_assertion`
and AT prefers `manual_agent` (when any high-confidence channel is curator-backed)
else `data_analysis_pipeline`. All-zero synthetic rows fall back to
`(not_provided, not_provided)`.

Key design point: **KL/AT is a row-level property, not a predicate-level one.**
It's derived from the dominant channel and shared by every edge emitted from
that row, even if a given edge's predicate came from a different (lower-scoring)
channel. This matches ORION and keeps the determination simple, at the cost of
some edges carrying a KL/AT that reflects the row's strongest evidence rather
than the specific channel that fired their predicate.

This reverses the earlier (2026-05-27) decision to keep uniform `not_provided`.
That decision was made to keep plumbing simple for the first per-channel
iteration; the KL/AT logic adds real downstream-filter value (e.g. "give me only
experimentally-grounded edges" → filter KL=knowledge_assertion). STITCH keeps
uniform `not_provided` — the basic protein_chemical.links file has no per-channel
scores to derive KL/AT from.

**Reader-level threshold filter.** The `combined_score > 500` gate now also lives
in `string.yaml` as a Koza `ComparisonFilter` on both tagged readers (so Koza
drops sub-threshold rows before constructing objects — the production efficiency
path). The in-transform `passes_combined_score` guard is retained so the
transform stays correct when called directly in unit tests. `combined_score` is
declared `int` in the reader `columns` so the numeric filter compares correctly
(untyped CSV columns are strings, and `'594' > 500` raises TypeError).

**Helper cleanup.**
- `knowledge_level_and_agent_type_for_row()` — the KL/AT derivation above, doctested.
- `make_string_ppi_edge()` — encapsulates the predicate→Association-class dispatch
  + has_attribute logic, so the transform body is a flat list comprehension.
- `sorted_pair_key` symmetry and ENSEMBL-only questions resolved inline (all STRING
  predicates are symmetric → sorted-pair dedup is correct; all 3 supported taxa
  use ENSEMBL protein IDs exclusively → the simple parser is correct).

**Still open (needs external input):** whether `MI:0915` is the right PSI-MI term
for the physical-interaction edges — flagged for confirmation with Sierra. Left as
an inline TODO since it's a modeling sign-off, not a code task.

**Tests:** 104 pass (was 89) — +15 KL/AT unit tests (single-channel mapping,
multi-high-conf upgrade, all-zero fallback, transform propagation) + integration
assertions that KL/AT varies across the output rather than being uniformly
`not_provided`.

## 2026-05-27 — Per-channel STRING predicates (6 edge types) matching ORION

**Decision.** Switch the STRING PPI source from the 3-column `protein.links.v12.0.txt.gz`
to the 16-column `protein.links.full.v12.0.txt.gz`, and emit per-channel predicates
based on which evidence channels exceed a high-confidence threshold. Matches
ORION's STRING parser exactly on the predicate-mapping side; uniform KL/AT on our
side (defers ORION's per-channel KL/AT for a later iteration).

**Why now.** Probing Automat's STRING-DB MKG (the canonical Translator-hosted
STRING graph that RoboKOP and other ARAs query) revealed that production emits
multiple PPI predicates per pair (physically_interacts_with, coexpressed_with,
homologous_to, related_to). Our single-predicate output would have been a
**downstream compatibility cliff** — queries for the other 3 predicates would
silently return zero results once our ingest replaces what's currently in
production. Per-channel emission closes that gap.

**Channel → predicate map (matches ORION):**

| Channel | Predicate | Biolink class |
|---|---|---|
| `experiments` | `biolink:physically_interacts_with` | `PairwiseMolecularInteraction` |
| `coexpression` | `biolink:coexpressed_with` | `GeneToGeneCoexpressionAssociation` |
| `textmining` | `biolink:interacts_with` | `PairwiseMolecularInteraction` |
| `neighborhood` | `biolink:genetic_neighborhood_of` | `PairwiseMolecularInteraction` |
| `fusion` | `biolink:gene_fusion_with` | `PairwiseMolecularInteraction` |
| `cooccurence` | `biolink:genetically_interacts_with` | `PairwiseMolecularInteraction` |

**Rules:**
- Row gate: `combined_score > 500` (unchanged from prior iterations).
- Per-channel high-confidence threshold: `> 750` (ORION's `high_conf_threshold`).
- Emit one edge per channel above threshold. Up to 6 edges per pair; typically 1–2.
- Fallback: if no channel exceeds 750 but the row passes the combined-score gate, emit one `biolink:physically_interacts_with` edge (matches ORION).
- Dedup key is `(sorted_pair, predicate)`, so multiple predicates can coexist on the same pair without colliding.
- MI:0915 (PSI-MI "physical association") attached only to `physically_interacts_with` edges — semantically misleading on the others.

**Considered and dropped:**

| Idea | Why dropped now | Bring back when |
|---|---|---|
| Include HOMOLOGY → `biolink:homologous_to` as a predicate | STRING's HOMOLOGY score means "interaction inferred via orthologs in another species", not "A is homologous to B" (per STRING docs and ORION's comment). Using it as `homologous_to` would be a semantic misread. | If STRING ever surfaces direct homology scores in a separate channel. |
| Include DATABASE as a predicate | ORION doesn't map it; DATABASE contributes to combined_score but doesn't drive its own channel-specific predicate beyond EXPERIMENTS-driven `physically_interacts_with`. | If there's evidence that downstream Translator queries depend on a database-specific predicate. |
| Implement ORION's per-channel KL/AT logic (channel-dependent knowledge_level/agent_type with multi-channel override) | Adds non-trivial plumbing (per-row class state) for a benefit that's mostly downstream-filter convenience. Kept uniform `not_provided` for this iteration. | If a Translator consumer explicitly wants to filter by KL=knowledge_assertion to get only experimentally-grounded edges. |
| Use the `_transferred` (orthology-projected) channel variants in the predicate-selection logic | ORION ignores them for predicate selection (uses only native channel columns); we follow suit. They're available in `.full` but unused by our rule. | If we ever want to give "this evidence is borrowed from orthologs in another species" its own qualifier. |
| Use the `.detailed.` variant instead of `.full.` (10 columns vs 16) | The `_transferred` columns aren't used for predicate selection, so `.detailed` would suffice — but the size difference is small (~10%) and `.full` keeps the option open for future per-channel-attribute work. | If disk really becomes the binding constraint at scale. |
| Per-channel `biolink:Attribute` instances (subscores attached to edges) | Heavier wire format; not directly needed for predicate emission; ORION doesn't do it. | Added to `future_considerations` for the next iteration. |
| Restrict to physical subnetwork (`protein.physical.links`) as a separate source | Largely redundant once per-channel predicates fire: physical evidence (`experiments` channel) already gets its own `physically_interacts_with` edges from the `.full` file. | The dual-physical/functional source idea has been removed from `future_considerations` as superseded by this change. |

**Two biolink class subtleties hit during implementation:**

- `PairwiseMolecularInteraction` accepts a restricted predicate set that does NOT include `coexpressed_with` (biolink considers coexpression a non-physical association). Coexpression edges must use `GeneToGeneCoexpressionAssociation` instead. The transform now maps predicate → Association class via `PREDICATE_TO_ASSOCIATION_CLASS`.
- `Association.category` is constrained to `["biolink:Association"]` literally; you can't override it to a more-specific class name. The category comes from whichever Pydantic class you instantiate.

**Output sizes (integration fixture run, 200-row .full slices per taxon):**

- ~80 above-threshold rows across 3 taxa → ~95 edges (mostly fallback `physically_interacts_with`; ~12 non-fallback from high-conf channels)
- 65 unit tests + 17 integration tests + 7 doctests pass

**Parked.** Per-channel KL/AT and per-channel subscore attributes (both in `future_considerations`). Mode-of-action splitting for STITCH (`actions.v5.0.tsv`) is unaffected by this change. Switching the chemical-entity ID prefix from PUBCHEM.COMPOUND to CHEBI (to match Automat's MKG) remains an open coordination question.

## 2026-05-27 — Wire mouse + rat for STRING + STITCH (multi-organism)

**Decision.** Extend both tagged readers (`string_ppi` and `stitch_pcl`) to the three
Translator-target mammals: human (9606), mouse (10090), rat (10116). No transform code
changes needed — `parse_string_protein_id` was already taxon-aware and `SUPPORTED_TAXA`
already enumerated all three. This was purely a yaml-wiring chunk plus fixture +
RIG/CHANGELOG updates.

**Pre-flight sanity probes confirmed:**

- All 9 file URLs (3 taxa × {STRING functional, STRING physical, STITCH}) return 200
- Mouse uses `ENSMUSP*`, rat uses `ENSRNOP*` ENSEMBL prefixes — parser handles both unchanged
- STITCH chemical format is identical across species (`CIDm`/`CIDs` + same per-species protein prefix)
- NodeNormalizer resolution: 100% (mouse, 50/50), 98% (rat, 49/50) — comparable to or better than human's 98.5%
- entrez_2_string mapping covers mouse + rat (existing file, no new download)

**Considered and dropped:**

| Idea | Why dropped now | Bring back when |
|---|---|---|
| Push threshold from `>500` to `>700` for the multi-organism full run | Adds a config drift between dev (small fixture, threshold 500) and prod (full file, threshold 700) — easy to confuse. | When disk is the binding constraint on a particular run; can be done via a single constant change for a one-off. |
| Add zebrafish / fly / worm / yeast in the same chunk | Beyond the three target Translator species; non-vertebrate species may use non-ENSEMBL protein IDs (e.g. yeast `YAL001C`), and NodeNormalizer coverage hasn't been probed for them. | When there's a specific Translator need for non-mammalian PPI. |
| Per-species tagged readers (`string_ppi_9606`, `string_ppi_10090`, …) | Would multiply the reader/transform count for no semantic gain; STRING ships per-species files with identical schema, and the per-row taxon parsing handles them uniformly. | Never; the current shape scales linearly with species count via reader's `files:` list. |

**Output sizes (multi-organism integration fixture run):** 105 PPI edges + ~92 STITCH edges across all three taxa; nodes count similarly. Full-file multi-organism run would be ~5–10 GB JSONL — same disk concern as the human-only full run, mitigated the same way (push threshold up, run on a server with proper disk, or use `--row-limit` for development).

**Parked.** Adding additional species (zebrafish, fly, worm, yeast) is now a one-line `SUPPORTED_TAXA` change + 2 URLs per species. Beyond that, the open work items are dual physical/functional STRING sources (split predicate by source) and STITCH `actions.v5.0.tsv` (mode-of-action qualifiers).

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
