"""
Candidate replacement concepts for (mention, concept) pairs judged wrong (pair P(wrong) >= 0.9).

Sources, in order: a fixed table of processed peptides (PEPTIDES, the only Protein Ontology ids offered); exact HGNC
matches of the mention (approved symbol, then alias or previous symbol), giving the gene's reviewed UniProt protein;
then NameRes lookups restricted to human gene products (UniProtKB, NCBIGene) and, for chemical-side mentions,
ChEBI / DrugBank / PubChem.
A chemical-side mention gets gene-product candidates too, since many errors are proteins mapped to protein drugs.

NameRes failures after retries are recorded in the output (nameres_error), not hidden.

    python repair_candidates.py chem_gene pairs.jsonl pair_scores.jsonl hgnc.txt out.jsonl
"""
import csv
import json
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

from families import FAMILIES
from rules import load_hgnc_symbols, norm

NAMERES = "https://name-resolution-sri.renci.org/lookup"
MAX_CANDIDATES = 6

# Processed peptides get Protein Ontology ids (Chris, 2026-11-13), and only these: PR's gene-level protein classes
# (PR:000001391 interleukin-4) are not merged with their genes in NodeNorm, so every other protein uses human UniProt.
# Keys are normalized mention spellings (rules.norm). Bradykinin, substance P and GLP-1 have no peptide
# entry in PR; endothelin-1's PR entry is a gene-level class, so it maps to EDN1 like any protein.
_PEPTIDES = {
    ("PR:000036008", "angiotensin I"): ["angi", "angiotensini", "angiotensin1"],
    ("PR:000036009", "angiotensin II"): ["angii", "angiotensinii", "angiotensin2", "ang2", "aii"],
    ("PR:000036010", "angiotensin III"): ["angiii", "angiotensiniii", "angiotensin3"],
    ("PR:000036011", "angiotensin IV"): ["angiv", "angiotensiniv", "angiotensin4"],
    ("PR:000036013", "angiotensin 1-7"): ["ang17", "angiotensin17"],
    ("PR:000036012", "angiotensin 1-9"): ["ang19", "angiotensin19"],
    ("PR:000036193", "amyloid beta peptide"): ["aβ", "abeta", "amyloidbeta", "amyloidβ", "βamyloid", "betaamyloid",
                                               "aβ42", "aβ40", "aβ142", "aβ140", "abeta42", "abeta40",
                                               "amyloidbetapeptide", "aβpeptide"],
}
PEPTIDES = {key: concept for concept, keys in _PEPTIDES.items() for key in keys}


def load_hgnc(path: str) -> tuple[dict, dict, dict]:
    """symbol key -> gene, alias/previous key -> genes, and gene info (symbol, name, uniprot)."""
    symbol, alias, info = {}, defaultdict(list), {}
    with open(path) as f:
        for row in csv.DictReader(f, delimiter="\t"):
            ups = [u for u in row["uniprot_ids"].split("|") if u]
            if not ups:
                continue
            hgnc_id = row["hgnc_id"]
            info[hgnc_id] = {"curie": f"UniProtKB:{ups[0]}", "label": f'{row["symbol"]} {row["name"]}',
                             "type": "human gene product"}
            symbol[norm(row["symbol"])] = hgnc_id
            for a in (row["alias_symbol"] + "|" + row["prev_symbol"]).split("|"):
                if a:
                    alias[norm(a)].append(hgnc_id)
    return symbol, alias, info


def nameres(query: str, **params) -> list[dict]:
    """NameRes lookup with retries; raises after the last failure."""
    url = f"{NAMERES}?{urllib.parse.urlencode({'string': query, 'limit': 5, 'autocomplete': 'false', **params}, doseq=True)}"
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=30) as resp:
                return json.load(resp)
        except OSError:
            if attempt == 2:
                raise
            time.sleep(2 * (attempt + 1))
    return []


def candidates_for(pair: dict, symbol: dict, alias: dict, info: dict, chemical_regulator: bool) -> dict:
    """Up to MAX_CANDIDATES replacement concepts for one pair, excluding its current concept."""
    found, seen, error = [], {pair["curie"]}, None

    def add(curie, label, kind, source):
        if curie not in seen and len(found) < MAX_CANDIDATES:
            seen.add(curie)
            found.append({"curie": curie, "label": label, "type": kind, "source": source})

    key = pair["key"]
    if key in PEPTIDES:
        curie, label = PEPTIDES[key]
        add(curie, label, "processed peptide", "peptide_list")
    if key in symbol:
        g = info[symbol[key]]
        add(g["curie"], g["label"], g["type"], "hgnc_symbol")
    for hgnc_id in alias.get(key, [])[:3]:
        g = info[hgnc_id]
        add(g["curie"], g["label"], g["type"], "hgnc_alias")
    queries = [dict(biolink_type="GeneOrGeneProduct", only_taxa="NCBITaxon:9606", only_prefixes="UniProtKB|NCBIGene")]
    if pair["role"] == "subject" and chemical_regulator:
        queries.append(dict(biolink_type="ChemicalEntity", only_prefixes="CHEBI|DRUGBANK|PUBCHEM.COMPOUND"))
    try:
        for params in queries:
            for hit in nameres(pair["mention"], **params)[:3]:
                kind = ", ".join(t.removeprefix("biolink:") for t in hit.get("types", [])[:1])
                add(hit["curie"], hit["label"], kind, "nameres")
    except OSError as e:
        error = f"{type(e).__name__}: {e}"
    return {"role": pair["role"], "key": pair["key"], "curie": pair["curie"], "name": pair["name"],
            "mention": pair["mention"], "count": pair["count"], "candidates": found, "nameres_error": error}


def symbol_hint(pair: dict, symbols: dict[str, set[str]]) -> bool:
    """True when the mention is exactly an approved HGNC symbol of a gene other than the protein it maps to."""
    ups = symbols.get(pair["key"])
    return pair["curie"].startswith("UniProtKB:") and bool(ups) and pair["curie"] not in ups


def main(family: str, pairs_path: str, scores_path: str, hgnc_path: str, out_path: str) -> None:
    """Candidates for every pair with P(wrong) >= 0.9, and for every pair the HGNC symbol rule flags (a hint, not
    a verdict: common aliases like PDK1 for PDPK1 collide with another gene's symbol, so those pairs also get a
    "keep the current mapping" option downstream)."""
    with open(scores_path) as f:
        wrong = {(r["role"], r["key"], r["curie"]) for r in map(json.loads, f) if r["probabilities"]["wrong"] >= 0.9}
    hgnc_symbols = load_hgnc_symbols(hgnc_path)
    with open(pairs_path) as f:
        pairs = [{**p, "symbol_hint": symbol_hint(p, hgnc_symbols)} for p in map(json.loads, f)]
    pairs = [p for p in pairs if p["symbol_hint"] or (p["role"], p["key"], p["curie"]) in wrong]
    symbol, alias, info = load_hgnc(hgnc_path)
    with ThreadPoolExecutor(max_workers=8) as pool, open(out_path, "w") as out:
        for result in pool.map(lambda p: {**candidates_for(p, symbol, alias, info, FAMILIES[family]["chemical_regulator"]),
                           "symbol_hint": p["symbol_hint"]},
                                pairs):
            out.write(json.dumps(result) + "\n")
    print(f"candidates for {len(pairs)} pairs written to {out_path}")


if __name__ == "__main__":
    main(*sys.argv[1:])
