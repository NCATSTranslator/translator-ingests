# TMKP treats filter

The TMKP source (the 2024-09-07 KGX release) has 336,369 `treats_or_applied_or_studied_to_treat` edges. Many are
misreadings of the sentence they came from. This filter drops the treats edges that none of their supporting
sentences support. The edge ids to drop are in `tmkp_treats_jev_v3_dropped_edges.txt.gz` (110,469 ids), which the
ingest downloads (see `download.yaml`) and applies in `transform_tmkp_edge`.

## How the list was made

Every treats edge carries its supporting sentences (`has_supporting_study_result` attributes with
`supporting_text` and the character spans of the two mentions). There are 873,268 of them.

1. **Score every sentence.** Each sentence was put to [Jev](https://typesafe.ai/blog/introducing-system-one-models-and-jev)
   (TypeSafe's typed-decision model, `jev-1.13.0`) as one multiple-choice question. Jev answers with a probability
   for each option instead of generating text. The question and options are below.
2. **Call a sentence bad** when P(other) + P(cell_line) + P(causes) ≥ 0.7.
3. **Drop an edge** when all of its sentences are bad. One supporting sentence is enough to keep an edge.

Policy decision: evidence that is only in vitro (a drug applied to cells, cell lines or cultures from the disease)
does not support a treats edge. Live-animal models, including xenografts, do.

### The question

The state given to Jev for each sentence is the sentence (`text`), the drug words and the concept they were mapped to
(`drug`, e.g. `cephalexin [Cephalexin]`), and the same for the condition (`condition`).

Instructions: *In `text`, is `drug` used, given or tested to treat or prevent `condition` itself? `drug` and
`condition` give the words used in `text` and, in brackets, the concept they were mapped to. Judge only what `text`
says. A drug applied to cells or animals as a tool to study a mechanism is not being tested as a treatment.*

| Option | Meaning given to Jev | Bad? |
|---|---|---|
| other | `text` does not say `drug` is used or tested against `condition` itself: `condition` is only background (the patients have it, but `drug` treats something else, such as a complication, a symptom, a co-infection, side effects or transplant conditioning), `drug` is a lab reagent, solvent, culture ingredient, inducer, carrier or excipient, or the two are only listed together | yes |
| cell_line | The only evidence is in vitro: `drug` was applied to cells, cell lines, primary cultures or tissue samples of `condition`, with no patients and no live animals | yes |
| causes | `drug` causes, induces or worsens `condition`, or `condition` is an adverse effect, contraindication or reason for caution | yes |
| no_effect | `drug` was tried against `condition` but showed no or minimal benefit | no |
| studied | `drug` is tested or proposed as a treatment of `condition` in patients or live animals: a trial, case, cohort, animal model or xenograft, or a stated proposal | no |
| treats | `text` states `drug` is used for, approved for, or effective at treating `condition` in patients | no |

Each option also had two or three short examples. "no_effect" sentences are kept, since a drug that was tried is
still "studied to treat".

### Validation

The prompt was developed over three versions against hand-labeled samples. Each sample was 300 sentences, one per edge,
stratified by evidence count (1, 2–4, 5+). The second sample excluded the first sample's edges. Results for the
final version, calling a sentence bad at ≥ 0.7:

| Sample | Unsupported sentences | Precision | Recall |
|---|---|---|---|
| Second sample (held out from the first two prompt versions) | 108 / 300 | 0.97 | 0.87 |
| First sample | 140 / 300 | 0.93 | 0.87 |

What it still misses: misgrounded concepts (e.g. "dermatofibrosarcoma protuberans" grounded to dermatofibroma),
drug-class sentences ("agents that modulate norepinephrine treat fibromyalgia" read as norepinephrine treats
fibromyalgia), and funding or diagnostic-test sentences. What it wrongly flags: a few real treatments where the drug
is an adjunct (G-CSF in mucormycosis).

### Result

| Jev answer | Sentences |
|---|---|
| studied | 386,414 |
| other | 197,039 |
| cell_line | 130,435 |
| treats | 125,992 |
| causes | 22,786 |
| no_effect | 10,602 |

110,469 of 336,369 treats edges (33%) are dropped. Edges with more evidence survive more often: 53% of edges with an
evidence count of 1 are kept, against 88% of edges with 10 or more. The full run cost $38 in Jev calls and took about
an hour.

## Other TMKP edge types

The same approach could cover TMKP's other edges for about $140 in Jev calls. 300-sentence pilots, labeled
independently, found:

| Edge type | Edges | Sentences | Unsupported sentences | Filter precision / recall |
|---|---|---|---|---|
| chemical → protein (`affects`, increased/decreased) | 683,406 | 1,502,246 | 56% | 0.96 / 0.68 |
| protein → protein (`affects`, gene regulation) | 343,983 | 589,036 | 93% | 1.00 / 0.92 |
| chemical → disease or phenotype (`contributes_to`) | 122,391 | 234,717 | 37% | 0.94 / 0.66 |

For the protein edges, wrong gene grounding is a large part of the problem (e.g. "Rac1" grounded to pancreatic
ribonuclease, "Sox2" to sulfhydryl oxidase 1). The pilots used a second question about grounding plus an HGNC rule:
a mention that is exactly an approved gene symbol must be grounded to that gene. These filters are not applied yet.
