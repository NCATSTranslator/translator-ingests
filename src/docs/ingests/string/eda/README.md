# STRING ingest — exploratory data analysis (qa-string-ingest)

Reproducible EDA over STRING `*.protein.links.full.v12.0.txt.gz` for the three
Translator-target taxa (human 9606, mouse 10090, rat 10116). Used to justify
predicate selection, per-channel filter thresholds, and the combined_score gate.

## How to run (streaming, no disk cache)

The script reads a gzip stream from stdin and computes the whole report in a
single pass, so the ~150 MB-per-taxon files never touch disk:

```bash
for tax in 9606 10090 10116; do
  curl -s https://stringdb-downloads.org/download/protein.links.full.v12.0/$tax.protein.links.full.v12.0.txt.gz \
    | uv run python eda.py $tax > reports/report_$tax.txt
done
```

Captured outputs live in [`reports/`](reports/) (one per taxon).

## What each report contains

1. **Per-channel stats** — nonzero %, max, mean, median, p90, p99 for all 13
   evidence channels + `combined_score`.
2. **Correlation matrix** — Pearson (x100) across channels, to detect
   overencoding / redundancy between predicate-driving channels.
3. **Predicate-channel firing counts** at thresholds 150–750.
4. **Co-firing matrix** — rows where two predicate channels both clear a
   threshold (molecular-vs-gene ambiguity).
5. **combined_score decomposition** — which channel dominates, and the gap
   between combined and the strongest single channel.

## Key findings (v12.0, June 2026)

- **Native `neighborhood` is 0 in every row, all three taxa** — `genetic_neighborhood_of`
  can never fire. `neighborhood_transferred` exists (~4% of rows) but maxes at 385.
- **`cooccurence` maxes at ~540** — never clears the 750 per-channel gate, so
  `genetically_interacts_with` never fires either.
- **Predicate-driving channels are statistically independent** (|r| < 0.15) —
  no overencoding; each predicate carries distinct signal.
- **Native `experiments` evidence is human-skewed**: 8.3% of human pairs vs
  1.0% mouse, 0.2% rat. Curated physical-interaction edges are mostly a human
  phenomenon.
- **The `combined_score > 500` gate never drops a row that a per-channel
  predicate (>750) would have fired** (0 exceptions across ~40M rows). Its only
  effect is governing the volume of fallback `physically_interacts_with` edges
  (~1.1M kept per taxon vs ~13.7M total rows).
</content>
