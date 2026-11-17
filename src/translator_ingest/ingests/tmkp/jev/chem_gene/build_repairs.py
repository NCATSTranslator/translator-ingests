"""
Decide every TMKP chemical -> protein sentence (keep as mapped, repair, or drop), then write:

- repaired edges, in TMKP's edges.tsv column layout (no header), so the ingest's existing reader can parse them
- a sidecar (JSON lines) recording which original edges and sentences each repaired edge came from
- the drop list for original edges (edge id, direction), tab-separated: edges none of whose sentences survive as
  originally mapped
- per-sentence decisions (JSON lines), for auditing

Sentence decision, in order:
1. A mention span missing or past the end of the text: drop.
2. Hard rules on either mention (rules.py): protein family or complex, chemical fragment, lipid class mapped to
   one species, unlabeled mention mapped to an isotope-labeled compound: drop.
3. Each mention's concept: a CPT1 protein mention takes its per-sentence isoform choice (P >= 0.7) or drops; a pair
   the HGNC symbol rule flags is kept, repaired or dropped by Jev's choice at P >= 0.7; any other pair judged wrong
   (P(wrong) >= 0.9) is repaired at P >= 0.9 or dropped; every other pair keeps its concept.
4. The relation-only answer must support the edge's direction (P >= 0.7), else drop.

A repaired sentence's edge: a gene product as regulator (UniProtKB, PR, NCBIGene) makes it gene -> gene regulation
(predicate biolink:regulates, relation biolink:GeneRegulatoryRelationship); a chemical regulator keeps
biolink:affects (biolink:ChemicalToGeneAssociation). Both keep qualified_predicate causes, aspect
activity_or_abundance and the original direction.

    python build_repairs.py <data dir>
"""
import gzip
import hashlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

from rules import FAMILY_NAMES, ISOTOPE, LIPID_SPECIES, fragment, norm

RELATION_THRESHOLD = 0.7
PAIR_WRONG = 0.9
REPAIR_ACCEPT = 0.9
HINT_ACCEPT = 0.7
CPT1_ACCEPT = 0.7
CPT1_KEYS = {"cpt1", "cpt1a", "cpt1b", "cpt1c"}
GENE_PREFIXES = ("UniProtKB:", "PR:", "NCBIGene:")
COLUMNS = ["subject", "predicate", "object", "qualified_predicate", "subject_aspect_qualifier",
           "subject_direction_qualifier", "subject_part_qualifier", "subject_form_or_variant_qualifier",
           "object_aspect_qualifier", "object_direction_qualifier", "object_part_qualifier",
           "object_form_or_variant_qualifier", "anatomical_context_qualifier", "id", "relation", "confidence_score",
           "supporting_study_results", "supporting_publications", "_attributes"]


def hard_rule(s: dict) -> str | None:
    """The first hard rule that fires on either mention, or None. (The HGNC symbol rule is a hint, not here.)"""
    for role in ("subject", "object"):
        curie, mention = s[role], s[f"{role}_mention"]
        if curie.startswith("UniProtKB:"):
            if norm(mention) in FAMILY_NAMES:
                return "family"
        else:
            name = s[f"{role}_name"] or ""
            if LIPID_SPECIES.search(name) and not LIPID_SPECIES.search(mention or ""):
                return "lipid_species"
            if ISOTOPE.search(name) and not ISOTOPE.search(mention or ""):
                return "isotope"
            if fragment(s["text"], mention, s[f"{role}_span"]):
                return "fragment"
    return None


def read_jsonl(path: Path) -> list[dict]:
    """All records of a JSON lines file."""
    with open(path) as f:
        return [json.loads(line) for line in f]


def load_answers(d: Path) -> dict:
    """Jev's answers the decision needs: pair verdicts, repair choices, CPT1 choices, relation answers."""
    def load(name: str) -> list[dict]:
        return read_jsonl(d / name)

    return {
        "pair_wrong": {(r["role"], r["key"], r["curie"]): r["probabilities"]["wrong"]
                       for r in load("chem_gene_pair_scores.jsonl")},
        "choices": {(r["role"], r["key"], r["curie"]): r for r in load("chem_gene_repair_choices.jsonl")},
        "cpt1": {(r["edge_id"], r["sentence_id"]): r for r in load("chem_gene_cpt1_choices.jsonl")},
        "relation": {(r["edge_id"], r["sentence_id"]): r["relation"]["probabilities"]
                     for r in load("chem_gene_relation_scores.jsonl")},
    }


def concept(s: dict, role: str, answers: dict) -> tuple[str | None, str]:
    """(final concept or None to drop, how it was decided) for one mention of a sentence."""
    curie, key = s[role], norm(s[f"{role}_mention"])
    if role == "object" and key in CPT1_KEYS:
        c = answers["cpt1"][(s["edge_id"], s["sentence_id"])]
        ok = c["curie"] and c["probability"] >= CPT1_ACCEPT
        return (c["curie"], "cpt1") if ok else (None, "cpt1_unresolved")
    pair = (role, key, curie)
    r = answers["choices"].get(pair)
    if r and r.get("symbol_hint"):
        if r["probability"] < HINT_ACCEPT or r["choice"] == "none":
            return None, "symbol_unresolved"
        return (curie, "symbol_keep") if r["choice"] == "keep" else (r["chosen"]["curie"], "symbol_repair")
    if answers["pair_wrong"][pair] >= PAIR_WRONG:
        if r and r.get("chosen") and r["probability"] >= REPAIR_ACCEPT:
            return r["chosen"]["curie"], "pair_repair"
        return None, "pair_wrong"
    return curie, "as_mapped"


def decide(s: dict, relation: dict, answers: dict) -> tuple[str, str, str | None, str | None]:
    """
    One sentence's (status, reason, final subject, final object): status is keep (supports its edge as mapped),
    repair (supports an edge between corrected concepts) or drop. relation is the sentence's relation answer.
    """
    if any(s[f"{r}_span"] is None or s[f"{r}_span"][1] > len(s["text"]) for r in ("subject", "object")):
        return "drop", "bad_span", None, None
    if rule := hard_rule(s):
        return "drop", f"rule:{rule}", None, None
    (subj, how_s), (obj, how_o) = concept(s, "subject", answers), concept(s, "object", answers)
    want = "increases" if s["object_direction"] == "increased" else "decreases"
    if subj is None or obj is None:
        return "drop", how_s if subj is None else how_o, None, None
    if relation[want] < RELATION_THRESHOLD:
        return "drop", "relation", subj, obj
    if subj == s["subject"] and obj == s["object"]:
        return "keep", f"{how_s}/{how_o}", subj, obj
    if not obj.startswith(GENE_PREFIXES):
        # a repaired edge's target must be a gene product (the other side was mapped to e.g. a drug)
        return "drop", "repaired_object_not_gene_product", subj, obj
    return "repair", f"{how_s}/{how_o}", subj, obj


def main(data: str) -> None:
    """Decide sentences, gather evidence for repaired ones, and write the outputs."""
    d = Path(data)
    answers = load_answers(d)

    outcomes, reasons = Counter(), Counter()
    kept_edges, repaired, not_kept = set(), defaultdict(list), []
    with open(d / "chem_gene_sentences.jsonl") as f, open(d / "chem_gene_sentence_decisions.jsonl", "w") as out:
        for s in map(json.loads, f):
            key = (s["edge_id"], s["sentence_id"])
            direction = s["object_direction"]
            status, reason, subj, obj = decide(s, answers["relation"][key], answers)
            outcomes[status] += 1
            reasons[(status, reason)] += 1
            if status == "keep":
                kept_edges.add((s["edge_id"], direction))
            else:
                not_kept.append((s["edge_id"], direction, s["sentence_id"]))
                if status == "repair":
                    repaired[(subj, obj, direction)].append(key)
            out.write(json.dumps({"edge_id": s["edge_id"], "sentence_id": s["sentence_id"], "direction": direction,
                                  "status": status, "reason": reason, "subject": subj, "object": obj}) + "\n")

    needed = {k for keys in repaired.values() for k in keys}
    evidence, all_edges = {}, set()
    with gzip.open(d / "edges.tsv.gz", "rt") as f:
        for line in f:
            p = line.rstrip("\n").split("\t")
            if p[14] != "biolink:ChemicalToGeneAssociation" or p[1] == "biolink:treats_or_applied_or_studied_to_treat":
                continue
            all_edges.add((p[13], p[9]))
            for a in json.loads(p[18]):
                if a["attribute_type_id"] == "biolink:has_supporting_study_result" and (p[13], a["value"]) in needed:
                    evidence[(p[13], a["value"])] = a

    n_regulates = 0
    with (gzip.open(d / "tmkp_chem_gene_repaired_edges.tsv.gz", "wt") as tsv,
          open(d / "tmkp_chem_gene_repaired_sources.jsonl", "w") as side):
        for (subj, obj, direction), keys in sorted(repaired.items()):
            results = [evidence[k] for k in keys]
            nested = [{n["attribute_type_id"]: n["value"] for n in r["attributes"]} for r in results]
            pubs = sorted({x["biolink:publications"] for x in nested if x.get("biolink:publications")})
            confidence = max(float(x.get("biolink:extraction_confidence_score") or 0) for x in nested)
            gene_regulator = subj.startswith(GENE_PREFIXES)
            n_regulates += gene_regulator
            predicate = "biolink:regulates" if gene_regulator else "biolink:affects"
            relation_type = "biolink:GeneRegulatoryRelationship" if gene_regulator else "biolink:ChemicalToGeneAssociation"
            edge_id = "tmkp-repair:" + hashlib.sha256(f"{subj}|{predicate}|{obj}|{direction}".encode()).hexdigest()
            attributes = [
                {"attribute_type_id": "biolink:knowledge_level", "value": "not_provided"},
                {"attribute_type_id": "biolink:agent_type", "value": "text_mining_agent"},
                {"attribute_type_id": "biolink:primary_knowledge_source", "value": "infores:text-mining-provider-targeted"},
                {"attribute_type_id": "biolink:supporting_data_source", "value": "infores:pubmed"},
                {"attribute_type_id": "biolink:evidence_count", "value": len(results)},
                {"attribute_type_id": "biolink:extraction_confidence_score", "value": confidence},
                {"attribute_type_id": "biolink:publications", "value": pubs},
                *results]
            row = {c: "" for c in COLUMNS} | {
                "subject": subj, "predicate": predicate, "object": obj, "qualified_predicate": "biolink:causes",
                "object_aspect_qualifier": "activity_or_abundance", "object_direction_qualifier": direction,
                "id": edge_id, "relation": relation_type, "confidence_score": str(confidence),
                "supporting_study_results": "|".join(r["value"] for r in results),
                "supporting_publications": "|".join(pubs), "_attributes": json.dumps(attributes)}
            tsv.write("\t".join(row[c] for c in COLUMNS) + "\n")
            side.write(json.dumps({"id": edge_id, "subject": subj, "predicate": predicate, "object": obj,
                                   "direction": direction, "sentences": [list(k) for k in keys],
                                   "original_edges": sorted({k[0] for k in keys})}) + "\n")

    dropped = sorted(all_edges - kept_edges)
    with gzip.open(d / "tmkp_chem_gene_dropped_edges.txt.gz", "wt") as f:
        f.write("".join(f"{edge_id}\t{direction}\n" for edge_id, direction in dropped))
    # Kept edges lose the sentences that don't support them as mapped (dropped, or moved to a repaired edge)
    pruned = sorted(k for k in not_kept if k[:2] in kept_edges)
    with gzip.open(d / "tmkp_chem_gene_pruned_sentences.txt.gz", "wt") as f:
        f.write("".join(f"{edge_id}\t{direction}\t{sentence_id}\n" for edge_id, direction, sentence_id in pruned))

    total = sum(outcomes.values())
    print(f"sentences: {total}")
    for status in ("keep", "repair", "drop"):
        print(f"  {status:7} {outcomes[status]:9} ({outcomes[status] / total:.1%})")
    for (status, reason), n in sorted(reasons.items(), key=lambda kv: (kv[0][0], -kv[1])):
        print(f"      {status:7} {reason:32} {n:9}")
    print(f"original edges: {len(all_edges)}; kept {len(kept_edges)}; dropped {len(dropped)} ({len(dropped) / len(all_edges):.1%})")
    print(f"sentences pruned from kept edges: {len(pruned)}")
    print(f"repaired edges: {len(repaired)} ({n_regulates} gene -> gene regulates, {len(repaired) - n_regulates} chemical "
          f"-> gene affects), from {len(needed)} sentences")


if __name__ == "__main__":
    main(*sys.argv[1:])
