# TMKP treats filter

The TMKP source (the 2024-09-07 KGX release) has 336,369 `treats_or_applied_or_studied_to_treat` edges. Many are
misreadings of the sentence they came from. This filter drops the treats edges that none of their supporting
sentences support.

The ingest does not call any model. It downloads a fixed list of edge ids,
`tmkp_treats_jev_v3_dropped_edges.txt.gz` (110,469 ids, hosted at
`https://stars.renci.org/var/translator/ingests/tmkp/`), and `transform_tmkp_edge` drops any edge whose source `id`
is on it. This directory holds everything used to make that list and to check it.

| File | What it does |
|---|---|
| `prompt.py` | The exact question, answer options, model version and threshold, and how an answer is read (`p_bad`) |
| `extract_sentences.py` | Step 1: pull every supporting sentence of every treats edge out of `edges.tsv.gz` |
| `score_sentences.py` | Step 2: ask Jev about each sentence; resumable |
| `build_drop_list.py` | Step 3: roll sentence answers up to edges and write the drop list |
| `sample_sentences.py` | Draw a stratified validation sample of sentences |
| `evaluate.py` | Compare Jev's answers on a sample with hand labels |
| `validation/` | The two validation samples, their hand labels, and Jev's answers on them |

## Method

Every TMKP treats edge carries its supporting sentences: `has_supporting_study_result` attributes with
`supporting_text` and the character spans of the drug and condition mentions. There are 873,268 of them.

1. **One question per sentence.** Each sentence was put to [Jev](https://typesafe.ai/blog/introducing-system-one-models-and-jev),
   TypeSafe's typed-decision model (`jev-1.13.0`), as one multiple-choice (`Choice`) question. Jev doesn't generate
   text: it returns a probability for each option. The state it judges is the sentence, the drug mention followed by
   the concept it was mapped to (e.g. `cephalexin [Cephalexin]`), and the same for the condition. The instructions
   and the six options, each with a description and examples, are in `prompt.py`:

   | Option | In short | Counts as bad |
   |---|---|---|
   | other | Not said to be used or tested against the condition itself: the condition is background, the drug is a reagent or excipient, or the two are only listed together | yes |
   | cell_line | The only evidence is in vitro (cells, cell lines, cultures) | yes |
   | causes | The drug causes or worsens the condition, or the condition is an adverse effect or contraindication | yes |
   | no_effect | Tried against the condition, no benefit | no |
   | studied | Tested or proposed in patients or live animals (trial, case, cohort, animal model, xenograft) | no |
   | treats | Used for, approved for, or effective in patients | no |

   The options that drop a sentence are listed first, because Jev leans toward earlier options.
2. **Sentence rule.** `p_bad` = P(other) + P(cell_line) + P(causes). A sentence is bad when `p_bad ≥ 0.7`.
3. **Edge rule.** An edge is dropped when every one of its sentences is bad. A single supporting sentence keeps it.

Policy decision: evidence that is only in vitro (a drug applied to cells or cell lines from the disease) does not
support a treats edge. Live-animal models, including xenografts, do. "no_effect" sentences are kept, since a drug
that was tried is still "studied to treat".

## Reproducing the list

Run from this directory. Steps 1 and 3 use only the standard library. Step 2 needs a TypeSafe API key and `uv`,
which installs the SDK (`typesafe-sdk==0.7.2`) into a throwaway environment from the script's inline metadata, so the
repo doesn't depend on it.

```bash
curl -O https://storage.googleapis.com/translator-text-workflow-dev-public/kgx/UniProt/edges.tsv.gz
curl -O https://storage.googleapis.com/translator-text-workflow-dev-public/kgx/UniProt/nodes.tsv.gz
python extract_sentences.py edges.tsv.gz nodes.tsv.gz treats_sentences.jsonl
TYPESAFE_API_KEY=... uv run score_sentences.py treats_sentences.jsonl scores.jsonl errors.jsonl --concurrency 32
python build_drop_list.py treats_sentences.jsonl scores.jsonl treats_edges.jsonl tmkp_treats_jev_v3_dropped_edges.txt.gz
```

- Step 1 writes 873,268 sentences.
- Step 2 took about an hour at 32 concurrent calls, used about 1,050 input tokens per sentence, and cost $38 (Jev
  charges $0.042 per million input tokens; output is free). If calls fail, rerun the same command; it skips
  sentences already scored and retries the rest.
- Step 3 also writes `treats_edges.jsonl`, one line per edge with its lowest `p_bad`, so another threshold can be
  tried without asking Jev again (`--threshold`).

**Rerunning step 2 gives a nearly, not exactly, identical list.** Jev's answers vary a little between calls. On a
check of 60 sentences, the top answer matched the original run for 59, probabilities moved by up to 0.13, and all
60 fell on the same side of the threshold.

The original run's answers are the authoritative record, at
`https://stars.renci.org/var/translator/ingests/tmkp/tmkp_treats_jev_v3_scores.jsonl.gz` (54 MB; one line per
sentence with its edge and sentence ids, Jev's top choice and the probability of each option). Running step 3 on
them reproduces the published list exactly, without a TypeSafe key:

```bash
curl -O https://stars.renci.org/var/translator/ingests/tmkp/tmkp_treats_jev_v3_scores.jsonl.gz
gunzip tmkp_treats_jev_v3_scores.jsonl.gz
python build_drop_list.py treats_sentences.jsonl tmkp_treats_jev_v3_scores.jsonl treats_edges.jsonl tmkp_treats_jev_v3_dropped_edges.txt.gz
```

## Validation

The prompt went through three versions, checked against two hand-labeled samples. Each sample is 300 sentences, one
per edge, 100 from each evidence stratum (1, 2–4, 5+). Sample B leaves out sample A's edges.

- Version 1 had a single "other" option for anything that wasn't treats or studied.
- Version 2 sharpened "other" and added a question about whether mentions were mapped to the right concept. That
  question didn't help and was dropped.
- Version 3 adds the `cell_line` option after the in-vitro policy decision. Both samples were relabeled to that
  policy.

Sample B was drawn after version 1 and used to compare versions 2 and 3, so it is held out from version 1's
development only. Labels are by one person.

```bash
python evaluate.py validation/sample_a.jsonl validation/labels_a.json validation/jev_v3_a.jsonl
python evaluate.py validation/sample_b.jsonl validation/labels_b.json validation/jev_v3_b.jsonl
```

| Sample | Unsupported sentences | Precision at 0.7 | Recall at 0.7 |
|---|---|---|---|
| B | 108 / 300 | 0.97 | 0.87 |
| A | 140 / 300 | 0.93 | 0.87 |

Labels in `validation/labels_*.json` map a sample row `n` to one of: T treats, S studied (patients or live animals),
N tried with no effect, L in vitro only, C causes, O other. L, C and O are unsupported.

What the filter still misses:
- misgrounded concepts ("dermatofibrosarcoma protuberans" grounded to dermatofibroma)
- drug-class sentences ("agents that modulate norepinephrine treat fibromyalgia" read as norepinephrine treats
  fibromyalgia)
- funding or diagnostic-test sentences

What it wrongly flags: a few real treatments where the drug is an adjunct (G-CSF in mucormycosis).

## Result

| Jev answer | Sentences |
|---|---|
| studied | 386,414 |
| other | 197,039 |
| cell_line | 130,435 |
| treats | 125,992 |
| causes | 22,786 |
| no_effect | 10,602 |

110,469 of 336,369 treats edges (33%) are dropped. Edges with more evidence survive more often:

| Evidence count | Edges kept |
|---|---|
| 1 | 53% |
| 2–4 | 70% |
| 5–9 | 81% |
| 10+ | 88% |

## Other TMKP edge types

The same approach could cover TMKP's other edges for about $140 in Jev calls. 300-sentence pilots, labeled
independently, found:

| Edge type | Edges | Sentences | Unsupported sentences | Pilot precision / recall |
|---|---|---|---|---|
| chemical → protein (`affects`, increased/decreased) | 683,406 | 1,502,246 | 56% | 0.96 / 0.68 |
| protein → protein (`affects`, gene regulation) | 343,983 | 589,036 | 93% | 1.00 / 0.92 |
| chemical → disease or phenotype (`contributes_to`) | 122,391 | 234,717 | 37% | 0.94 / 0.66 |

For the protein edges, wrong gene grounding is a large part of the problem (e.g. "Rac1" grounded to pancreatic
ribonuclease, "Sox2" to sulfhydryl oxidase 1). Those pilots used a regulation question, a grounding question and an
HGNC rule (a mention that is exactly an approved gene symbol must be grounded to that gene). Their prompts aren't in
this directory, and their filters are not applied.
