<!--
Draft GitHub issue for discussion. Per source-ingest-sop.md, content/modeling
questions go to a `source-ingest`-labeled ticket in the DINGO repo
(NCATSTranslator/Data-Ingest-Coordination-Working-Group). Cross-link from the
STRING RIG provenance once filed. Evidence: src/translator_ingest/ingests/string/eda/.
-->

# STRING: how should we treat the `neighborhood` vs `neighborhood_transferred` channels (and native-vs-transferred channels in general)?

**Source:** STRING v12.0 `*.protein.links.full.v12.0.txt.gz`, taxa 9606 / 10090 / 10116
**Component:** `src/translator_ingest/ingests/string`
**Label:** source-ingest

## TL;DR

For human/mouse/rat, STRING's **native `neighborhood` channel is empty (0 in every
row)**, so the `biolink:genetic_neighborhood_of` predicate we map it to **can never
fire**. The only neighborhood signal that exists is in **`neighborhood_transferred`**
(orthology-projected), but it is (a) **sub-threshold** (max 385, never > 500), (b) a
**different semantic claim** (orthologs are neighbors, not *this* pair), and (c)
**domain/range-invalid** on our `Protein` nodes (the gene-family predicates require
`Gene`). We need a decision on (1) whether to keep `genetic_neighborhood_of` at all,
(2) whether `_transferred` channels may ever drive a predicate, and (3) the general
native-vs-transferred policy.

## Background: what these columns mean

STRING scores every evidence channel **twice**:

- **native** (e.g. `neighborhood`) — evidence observed *in this organism*. For the
  neighborhood channel: are the two genes physically **adjacent on the chromosome**
  in human/mouse/rat (operon-like co-location)?
- **transferred** (`neighborhood_transferred`) — the same evidence type observed in
  *other* genomes and **projected onto this pair via orthology** ("interologs"):
  "orthologs of these two genes are genomic neighbors in some other (usually
  prokaryotic) genome."

Vertebrate genes aren't arranged in operons, so the **native** neighborhood score is
structurally ~0 for mammals; any neighborhood signal lives only in the **transferred**
column. `cooccurence` (phylogenetic co-occurrence) is the sibling genomic-context
channel and has **no** transferred variant in the file.

## EDA evidence (full files, ~40M rows total — see `eda/`)

| channel | human (9606) | mouse (10090) | rat (10116) |
|---|---|---|---|
| `neighborhood` (native) | **0 nonzero, max 0** | 0 nonzero, max 0 | 0 nonzero, max 0 |
| `neighborhood_transferred` | 571,332 nz (4.2%), **max 357**, p99 69 | 533,870 nz (4.2%), **max 366**, p99 76 | 563,086 nz (4.1%), **max 385**, p99 70 |
| `cooccurence` (native) | 216,390 nz (1.6%), **max 537**, p99 146 | 266,766 nz (2.1%), **max 537**, p99 201 | 380,440 nz (2.8%), **max 542**, p99 228 |

Consequences with the current gates (per-channel `> 750`, combined `> 500`):

- `genetic_neighborhood_of` (native `neighborhood`): **0 edges, all taxa, at any
  threshold** — the column is empty.
- `neighborhood_transferred`: **max 385 < 500**, so it never clears even the row gate
  on its own; it is the *dominant* channel for only 0.1–0.2% of rows. Lowering a gate
  far enough to surface it (< 385) is below STRING's medium-confidence floor (noise).
- `cooccurence` → `genetically_interacts_with`: **max 542**, never clears 750; could
  surface only a few hundred edges per taxon if the gate were dropped to ~450 (also
  sub-floor).

## The modeling problem

`biolink:genetic_neighborhood_of` is defined as *"holds between two genes located
nearby one another on a chromosome"*, **domain = gene, range = gene**, `is_a
genetically_interacts_with`. Two independent issues:

1. **Semantics.** A *transferred* score does **not** say the human/mouse/rat genes are
   neighbors — it says their orthologs elsewhere are. Emitting `genetic_neighborhood_of`
   from `neighborhood_transferred` would assert a chromosomal adjacency that is false
   for the actual subject/object. (Same trap that excludes STRING's HOMOLOGY channel.)
2. **Domain/range.** All three gene-family predicates — `genetic_neighborhood_of`,
   `genetically_interacts_with`, `gene_fusion_with` — are `gene`/`gene`, but the ingest
   emits **`Protein`** nodes. This is the root cause of the `BIOLINK_SUBOBJ_ERRORS` in
   the build summary (e.g. `gene_fusion_with | BAD SUBJECT | gene | protein`); even the
   732 `gene_fusion_with` edges that shipped are domain/range-violating.

## Provider precedents (meta_kgs)

- **Automat STRING-DB** (`/meta_knowledge_graph`): **Protein→Protein** only;
  predicates `physically_interacts_with`, `coexpressed_with`, `homologous_to`,
  `related_to`. **No** neighborhood / gene-family predicate.
- **Monarch** (`monarch-initiative/string-ingest`): ingests at the **Gene** level
  (`NCBIGene` nodes), association class **`PairwiseGeneToGeneInteraction`**, but a
  single **generic `biolink:interacts_with`** predicate — *not* the specific
  gene-family predicates — and no score threshold (any `evidence_type > 0`).

So provider practice is split on node type (Protein vs Gene) but neither emits a
`genetic_neighborhood_of`/`genetically_interacts_with`/`gene_fusion_with` predicate.

## Questions for discussion

1. **Drop `genetic_neighborhood_of`?** Native `neighborhood` is empty for our taxa, so
   the mapping is dead weight. Keep it (documented as empty, for potential non-mammalian
   taxa later) or remove it?
2. **May `_transferred` channels ever drive a predicate?** Today we read only native
   columns (ORION parity). Given the semantic mismatch + sub-threshold magnitude, do we
   keep "transferred contributes to `combined_score` but never mints its own predicate"
   as an explicit policy?
3. **General native-vs-transferred principle** — codify one rule for all channels, and
   if transferred evidence is ever surfaced, should it be an **edge attribute / evidence
   qualifier** ("inferred via orthologs") rather than a predicate?
4. **(Linked, separable)** Does the gene-vs-protein node decision change this? If we go
   gene-level (Monarch-style), `PairwiseGeneToGeneInteraction` becomes domain/range-valid
   — but the *specific* neighborhood predicate still asserts adjacency the transferred
   evidence doesn't support.

## Options

| # | Option | Pros | Cons |
|---|---|---|---|
| A | **Drop** `genetic_neighborhood_of` (and the empty/sub-threshold genomic-context predicates); keep `physically_interacts_with` / `coexpressed_with` / textmining. | Matches Automat; removes domain/range warnings; no dead mappings. | Loses a (currently empty) predicate slot. |
| B | **Keep** the native `neighborhood` mapping but document it as structurally empty for mammals. | No code change; ready for non-vertebrate taxa. | Build keeps emitting a predicate that's always 0; warnings remain. |
| C | Surface `neighborhood_transferred` under a **generic** predicate (`related_to`) with an orthology/interolog **qualifier** + prediction KL/AT. | Honest about provenance; reuses a Protein-valid predicate. | Still ~0 edges at defensible thresholds; adds complexity for little signal. |
| D | Go **gene-level** (Monarch-style) so gene predicates are valid. | Domain/range-correct; enables `PairwiseGeneToGeneInteraction`. | Large endpoint change; `genetic_neighborhood_of` *still* semantically wrong for transferred evidence. |

## Recommendation

- **Native `neighborhood` → Option A:** drop `genetic_neighborhood_of` (empty for all
  target taxa; domain/range-invalid on Protein). Likewise reconsider `gene_fusion_with`
  and `genetically_interacts_with` under the same domain/range lens.
- **`neighborhood_transferred` → do not mint a predicate.** Keep the explicit policy:
  transferred channels feed `combined_score` only; if ever surfaced, do it as a
  qualified evidence attribute, never as a neighborhood/adjacency assertion.
- Defer the **gene-vs-protein** endpoint choice (Q4) to its own ticket; it changes the
  node model but not the conclusion above about transferred-neighborhood semantics.

## Asks

- Sign-off on dropping `genetic_neighborhood_of` for the STRING-on-Protein ingest.
- Agreement on the native-vs-transferred policy statement (for the RIG `additional_notes`).
- Pointer to any downstream consumer that actually queries `genetic_neighborhood_of` /
  `genetically_interacts_with` from STRING (to confirm no compatibility cliff).
