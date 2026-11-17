# TMKP regulation filter and repairs

Two TMKP edge families say one thing raises or lowers another: `biolink:affects`, qualified `causes`
increased/decreased `activity_or_abundance`. They differ only in the regulator.

| Family | Relation | Edges | Sentences | Unsupported as mapped (blind sample) |
|---|---|---|---|---|
| `chem_gene`: chemical → protein | `ChemicalToGeneAssociation` | 683,406 | 1,502,246 | 58% |
| `gene_gene`: protein → protein | `GeneRegulatoryRelationship` | 343,983 | 589,036 | 93% |

The main reason isn't the relation: it's that one of the two mentions was mapped to the wrong concept ("COX-2" to a
mitochondrial cytochrome c oxidase subunit, "Nrf2" to GABPA, "IL-4" to the drug binetrakin). Protein → protein
sentences also often state no regulation at all (co-expression, binding, two proteins measured together).

Both families run through the same scripts; `families.py` holds what differs (relation, how each role is described
to Jev, whether the regulator is a chemical). For each family this directory decides every sentence (keep, repair or
drop) and produces three files the ingest downloads from `https://stars.renci.org/var/translator/ingests/tmkp/`:

| File (`<family>` = `chem_gene` or `gene_gene`) | Contents | Ingest use |
|---|---|---|
| `tmkp_<family>_dropped_edges.txt.gz` | (edge id, direction) pairs | Edges dropped: none of their sentences support them as mapped |
| `tmkp_<family>_pruned_sentences.txt.gz` | (edge id, direction, sentence id) | Sentences removed from kept edges' evidence |
| `tmkp_<family>_repaired_edges.tsv.gz` | Edges in TMKP's `edges.tsv` column layout | New edges for sentences repaired to the right concepts (read as the `repaired_edges` reader) |

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
   - **Per pair.** The mapping is judged once per distinct (mention, concept) pair (52,113 chem_gene, 48,503
     gene_gene), from up to three example sentences. TMKP grounds a string the same way everywhere, so one verdict covers every sentence with the
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
- **Gene → gene edges are regulation.** An edge whose regulator is a gene product is emitted as
  `GeneRegulatesGeneAssociation`, predicate `biolink:regulates`, qualified predicate `causes`, as SIGNOR models it.
  This covers repaired edges from either family and every kept protein → protein edge, which TMKP itself labels
  `affects`. A chemical regulator stays `ChemicalAffectsGeneAssociation` / `biolink:affects`.

The questions and their options are in `prompts.py`, exactly as run.

## Result

| Sentences | chem_gene | | gene_gene | |
|---|---|---|---|---|
| Kept as mapped | 891,865 | 59.4% | 72,810 | 12.4% |
| Repaired | 93,149 | 6.2% | 13,336 | 2.3% |
| Dropped | 517,232 | 34.4% | 502,890 | 85.4% |

Drops by reason (sentences):

| Reason | chem_gene | gene_gene |
|---|---|---|
| Relation not supported | 291,754 | 391,509 |
| Wrong mapping, no repair | 100,989 | 68,927 |
| Fragment rule | 61,045 | — |
| Family/complex rule | 43,053 | 30,238 |
| Lipid-species rule | 12,516 | — |
| Symbol hint unresolved | 4,937 | 6,213 |
| CPT1 unresolved | 1,476 | 209 |
| Isotope rule | 1,105 | — |
| Span defect | 355 | 5,794 |

The fragment, lipid and isotope rules are about chemical mentions, so they don't fire on protein → protein sentences.

Repairs (sentences):

| Repair | chem_gene | gene_gene |
|---|---|---|
| Symbol-hint repairs | 43,804 | 8,468 |
| Other wrong pairs | 47,485 | 4,842 |
| CPT1 isoforms | 1,195 | 26 |

Edges:

| Edges | chem_gene | gene_gene |
|---|---|---|
| Original, kept | 407,085 | 43,893 |
| Original, dropped | 276,321 | 300,090 |
| Sentences pruned from kept edges | 112,601 | 22,538 |
| Repaired (new) edges | 37,891 (35,021 `affects`, 2,870 `regulates`) | 6,880 (all `regulates`) |

For example, the binetrakin → desmoglein-1 edge is dropped, and its five sentences now support IL-4
(UniProtKB:P05112) `regulates` desmoglein-1, decreased.

## Validation

Everything here was checked against samples labeled blind to Jev's answers, by separate Claude instances working
from written rubrics. Re-score a family from `validation/<family>/` with:

```bash
python evaluate_filter.py validation/chem_gene
python evaluate_repairs.py validation/chem_gene
python evaluate_filter.py validation/gene_gene
python evaluate_repairs.py validation/gene_gene
```

| Check | Sample | Result |
|---|---|---|
| chem_gene filter: sentences not kept on their edge as mapped | 300 held-out sentences (174 unsupported) | precision 0.96, recall 0.77; supported share among kept sentences rises from 42% to 75% |
| chem_gene repairs of pairs judged wrong | 100 pairs | 90% of acted-on choices correct (99.6% by sentences) |
| chem_gene HGNC symbol-hint pairs (keep or repair) | 70 pairs | 94% correct (99.8% by sentences) |
| gene_gene filter | 300 pilot sentences (278 unsupported) | precision 0.99, recall 0.97; supported share among kept sentences rises from 7% to 69% |
| gene_gene repairs (wrong and symbol-hint pairs) | 80 pairs | 92% of acted-on choices correct (97.1% by sentences) |

The chem_gene held-out sample was drawn after the pilot that shaped the prompts and rules, and excludes the pilot's
edges. The gene_gene sample is that family's pilot. Nothing was tuned on it: the gene_gene run reuses the chem_gene
prompts, rules and thresholds, changing only the description of the regulator. With only 29 kept sentences in the
sample, the 69% is a rough figure.

Known limits:

- **Repair mistakes are concentrated in small pairs** (1–25 sentences): a peptide analog, a cell line, a family
  name, an alias read the wrong way.
- **Missed repairs.** About 13 of 100 chem_gene and 11 of 80 gene_gene sampled pairs had a right candidate that
  wasn't accepted at the threshold. Those sentences are dropped, not repaired.
- **Most protein → protein evidence is gone.** 87% of those edges are dropped. That matches the pilot (93% of
  sentences unsupported), but the family is now small.
- **Repair labels were partly re-adjudicated.** The repair-sample labels predate the human-only rule. 18 rows about
  non-human genes were re-adjudicated under it, recorded in
  `validation/chem_gene/repair_labels_human_overrides.json`. The gene_gene labels were made under the final rule.

## Reproducing

Run from this directory, with a data directory holding TMKP's `edges.tsv.gz`, `nodes.tsv.gz` and HGNC's
`hgnc_complete_set.txt`. Steps 3, 5, 6 and 7 call Jev and need `TYPESAFE_API_KEY`: fill it from a secret store for
the one command (see `../README.md`). `uv run` installs the SDK from each script's inline metadata. Set `F` to the
family:

```bash
F=gene_gene   # or chem_gene
python extract_sentences.py $F edges.tsv.gz nodes.tsv.gz data/${F}_sentences.jsonl              # 1. sentences
python build_pairs.py $F edges.tsv.gz nodes.tsv.gz data/${F}_pairs.jsonl                        # 2. distinct pairs
uv run score.py --family $F pairs data/${F}_pairs.jsonl data/${F}_pair_scores.jsonl pair_errors.jsonl
uv run score.py --family $F relation data/${F}_sentences.jsonl data/${F}_relation_scores.jsonl rel_errors.jsonl
python repair_candidates.py $F data/${F}_pairs.jsonl data/${F}_pair_scores.jsonl \
    hgnc_complete_set.txt data/${F}_repair_candidates.jsonl                                     # 4. candidates
python normalize_candidates.py data/${F}_repair_candidates.jsonl \
    data/${F}_repair_candidates_norm.jsonl hgnc_complete_set.txt
uv run choose_repairs.py data/${F}_repair_candidates_norm.jsonl data/${F}_pairs.jsonl \
    data/${F}_repair_choices.jsonl                                                              # 6. repair choices
uv run choose_cpt1.py data/${F}_sentences.jsonl data/${F}_cpt1_choices.jsonl                    # 7. CPT1
cp edges.tsv.gz data/ && python build_repairs.py $F data                                        # 8. decide and write
```

- **Steps 1, 2 and 8 are deterministic.** Given the same Jev answers and candidates, step 8 reproduces the published
  files exactly.
- **Step 4 depends on the live NameRes service,** and Jev's answers vary slightly between runs. In a 71-call check
  against the original run, 70 gave the same answer. So a full rerun gives nearly, not exactly, the published files.
- **Only the three ingest files are published.** The original run's Jev answers and repair candidates aren't; the
  answers this directory's validation needs are in `validation/`.

**Cost.** Jev charges $0.042 per million input tokens, and output is free.

- chem_gene: about $63, $2.12 for the pair questions and $60.81 for the relation questions.
- gene_gene: about $26, $2.24 for the pair questions and $25.51 for the relation questions.
- The repair choices, CPT1 choices and the validation runs added well under a dollar each.

## Not covered here

TMKP's chemical → disease `contributes_to` edges (122,391 edges) aren't filtered yet.
