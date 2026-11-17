# TMKP chemical → protein filter and repairs

TMKP's 683,406 chemical → protein edges (`biolink:affects`, qualified `causes` increased/decreased
`activity_or_abundance`, relation `ChemicalToGeneAssociation`) rest on 1,502,246 supporting sentences. On a blind
labeled sample, 58% of sentences don't support their edge as mapped. The main reason isn't the relation: it's that
one of the two mentions was mapped to the wrong concept ("COX-2" to a mitochondrial cytochrome c oxidase subunit,
"Nrf2" to GABPA, "IL-4" to the drug binetrakin).

This directory decides every sentence (keep, repair or drop) and produces three files the ingest downloads from
`https://stars.renci.org/var/translator/ingests/tmkp/`:

| File | Contents | Ingest use |
|---|---|---|
| `tmkp_chem_gene_dropped_edges.txt.gz` | 276,321 (edge id, direction) pairs | Edges dropped: none of their sentences support them as mapped |
| `tmkp_chem_gene_pruned_sentences.txt.gz` | 112,601 (edge id, direction, sentence id) | Sentences removed from kept edges' evidence |
| `tmkp_chem_gene_repaired_edges.tsv.gz` | 37,891 edges, TMKP's `edges.tsv` column layout | New edges for sentences repaired to the right concepts (read as the `repaired_edges` reader) |

TMKP's edge id ignores the direction qualifier (an increased and a decreased edge for the same pair share one id) and
the predicate, so the ingest matches these lists on relation, id and direction.

## Decisions

Each sentence is decided in this order (`build_repairs.decide`):

1. **Source defect.** A mention span missing or past the end of the text: drop (355 sentences).
2. **Hard rules** (`rules.py`), on either mention: drop.
   - a protein family or complex mapped to one member ("NF-κB" → NFKB1, "p38 MAPK" → MAPK14, "PKC", "AP-1")
   - a chemical mention cut from a longer name ("chloride" from lithium chloride, "acetate" from PMA)
   - a generic lipid class mapped to one molecular species ("diacylglycerol" → diacylglycerol (18:0/…))
   - an unlabeled mention mapped to an isotope-labeled compound
3. **Each mention's concept.**
   - **Per pair.** The mapping is judged once per distinct (mention, concept) pair, 52,113 in all, from up to three
     example sentences. TMKP grounds a string the same way everywhere, so one verdict covers every sentence with the
     pair.
   - **Pairs judged wrong** (P(wrong) ≥ 0.9) go to repair: candidates from exact HGNC matches and NameRes, and Jev
     chooses one or none from the examples. A choice at P ≥ 0.9 repairs the mention; otherwise the sentence is
     dropped.
   - **HGNC symbol hint.** A protein mention that is exactly another gene's approved symbol is a hint, not a rule:
     common aliases collide with other genes' symbols ("PDK1" usually means PDPK1, "MST1" means STK4). These pairs get
     an extra "keep the current mapping" option and are kept, repaired or dropped at P ≥ 0.7.
   - **CPT1** is chosen per sentence (CPT1A liver, CPT1B muscle and heart, CPT1C brain, or none) from context, at
     P ≥ 0.7.
4. **Relation.** The sentence must say the regulator raises (or lowers) the target, in the edge's direction:
   P ≥ 0.7 on the relation question. Otherwise drop.
5. **Outcome.** A sentence whose concepts are unchanged is kept on its edge. One with a changed concept moves to a
   repaired edge between the corrected concepts, as long as the target is a gene product. An original edge with no
   kept sentence is dropped.

Policy decisions, made by Chris Bizon:

- **Map everything to human,** as TMKP's design does. A gene from any organism (mouse, rat, fish, yeast, plant) maps
  to its human counterpart, or to none if there isn't one. Candidates are human UniProt proteins.
- **A protein family or complex mapped to one member is wrong,** with two exceptions: "HIF-1" → HIF1A, which matches
  how HIF-1α is handled, and CPT1, chosen per sentence.
- **Cell-line evidence counts** for regulation edges.
- **Processed peptides use Protein Ontology ids,** not ChEBI and not their precursor protein. These are angiotensin
  I–IV, 1–7 and 1–9, and amyloid beta peptide (`repair_candidates.PEPTIDES`). PR's gene-level protein classes aren't
  merged with their genes in NodeNorm, so no other PR ids are offered.
- **Repaired edge type.** A gene product as regulator makes the repaired edge gene → gene regulation:
  `GeneRegulatesGeneAssociation`, predicate `biolink:regulates`, qualified predicate `causes`, as SIGNOR models it.
  A chemical regulator stays `ChemicalAffectsGeneAssociation` / `biolink:affects`.

The questions and their options are in `prompts.py`, exactly as run.

## Result

| Sentences | Count | Share |
|---|---|---|
| Kept as mapped | 891,865 | 59.4% |
| Repaired | 93,149 | 6.2% |
| Dropped | 517,232 | 34.4% |

Drops by reason:

| Reason | Sentences |
|---|---|
| Relation not supported | 291,754 |
| Wrong mapping, no repair | 100,989 |
| Fragment rule | 61,045 |
| Family/complex rule | 43,053 |
| Lipid-species rule | 12,516 |
| Symbol hint unresolved | 4,937 |
| CPT1 unresolved | 1,476 |
| Isotope rule | 1,105 |
| Span defect | 355 |

Repairs:

| Repair | Sentences |
|---|---|
| Symbol-hint repairs | 43,804 |
| Other wrong pairs, target side | 40,194 |
| Other wrong pairs, regulator side | 7,291 |
| CPT1 isoforms | 1,195 |

Of the 683,406 original edges, 407,085 are kept (with 112,601 sentences pruned from them) and 276,321 dropped. The
repaired sentences form 37,891 new edges: 35,021 chemical → gene `affects` and 2,870 gene → gene `regulates`. For
example, the binetrakin → desmoglein-1 edge is dropped, and its five sentences now support IL-4 (UniProtKB:P05112)
`regulates` desmoglein-1, decreased.

## Validation

Everything here was checked against samples labeled blind to Jev's answers, by separate Claude instances working
from written rubrics. Re-score from `validation/` with:

```bash
python evaluate_filter.py validation
python evaluate_repairs.py validation
```

| Check | Sample | Result |
|---|---|---|
| Filter: sentences not kept on their edge as mapped | 300 held-out sentences (174 unsupported) | precision 0.96, recall 0.77; supported share among kept sentences rises from 42% to 75% |
| Repairs of pairs judged wrong | 100 pairs | 90% of acted-on choices correct (99.6% by sentences) |
| HGNC symbol-hint pairs (keep or repair) | 70 pairs | 94% correct (99.8% by sentences) |

The held-out sample was drawn after the pilot that shaped the prompts and rules, and excludes the pilot's edges.

Known limits:

- **Repair mistakes are concentrated in small pairs** (1–25 sentences): a peptide analog, a cell line, a family
  name, an alias read the wrong way.
- **Missed repairs.** About 13 of 100 wrong pairs had a right candidate that wasn't accepted at the threshold. Those
  sentences are dropped, not repaired.
- **Repair labels were partly re-adjudicated.** The repair-sample labels predate the human-only rule. 18 rows about
  non-human genes were re-adjudicated under it, recorded in `validation/repair_labels_human_overrides.json`.

## Reproducing

Run from this directory, with a data directory holding TMKP's `edges.tsv.gz`, `nodes.tsv.gz` and HGNC's
`hgnc_complete_set.txt`. Steps 3, 5, 6 and 7 call Jev and need `TYPESAFE_API_KEY`: fill it from a secret store for
the one command (see `../README.md`). `uv run` installs the SDK from each script's inline metadata.

```bash
python extract_sentences.py edges.tsv.gz nodes.tsv.gz data/chem_gene_sentences.jsonl            # 1. sentences
python build_pairs.py edges.tsv.gz nodes.tsv.gz data/chem_gene_pairs.jsonl                     # 2. distinct pairs
uv run score.py pairs data/chem_gene_pairs.jsonl data/chem_gene_pair_scores.jsonl pair_errors.jsonl
uv run score.py relation data/chem_gene_sentences.jsonl data/chem_gene_relation_scores.jsonl rel_errors.jsonl
python repair_candidates.py data/chem_gene_pairs.jsonl data/chem_gene_pair_scores.jsonl \
    hgnc_complete_set.txt data/chem_gene_repair_candidates.jsonl                                # 4. candidates
python normalize_candidates.py data/chem_gene_repair_candidates.jsonl \
    data/chem_gene_repair_candidates_norm.jsonl hgnc_complete_set.txt
uv run choose_repairs.py data/chem_gene_repair_candidates_norm.jsonl data/chem_gene_pairs.jsonl \
    data/chem_gene_repair_choices.jsonl                                                         # 6. repair choices
uv run choose_cpt1.py data/chem_gene_sentences.jsonl data/chem_gene_cpt1_choices.jsonl          # 7. CPT1
cp edges.tsv.gz data/ && python build_repairs.py data                                           # 8. decide and write
```

- **Steps 1, 2 and 8 are deterministic.** Given the same Jev answers and candidates, step 8 reproduces the published
  files exactly.
- **Step 4 depends on the live NameRes service,** and Jev's answers vary slightly between runs. In a 71-call check
  against the original run, 70 gave the same answer. So a full rerun gives nearly, not exactly, the published files.
- **Only the three ingest files are published.** The original run's Jev answers and repair candidates aren't; the
  answers this directory's validation needs are in `validation/`.

**Cost.** About $63 in Jev calls for the full run: $2.12 for the pair questions and $60.81 for the relation
questions. The repair choices, CPT1 choices and the validation runs added well under a dollar each. Jev charges $0.042
per million input tokens, and output is free.

## Not covered here

TMKP's protein → protein edges (`GeneRegulatoryRelationship`, 343,983 edges, 93% of pilot sentences unsupported) and
chemical → disease `contributes_to` edges (122,391 edges) aren't filtered yet. The protein → protein edges are also
still emitted as `affects` / `GeneToGeneAssociation` rather than the `regulates` form used for repaired edges.
