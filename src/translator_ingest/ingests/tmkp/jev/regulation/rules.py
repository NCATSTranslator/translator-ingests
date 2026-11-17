"""
Deterministic checks for TMKP chemical -> protein sentences.

Hard rules (a sentence that fails one is dropped; build_repairs.py):
- family: a mention that names a protein family or complex can't map to one member
- fragment: a chemical mention cut out of a longer name ("chloride" from lithium chloride, "iodo" from 4-iodo-...)
- lipid_species: a generic lipid class mapped to one molecular species ("diacylglycerol" -> diacylglycerol (18:0/...))
- isotope: an unlabeled mention mapped to an isotope-labeled compound ("nicotine" -> [3H]nicotine)

A hint, not a rule: a protein mention that is exactly an approved HGNC symbol of a different gene than it maps to.
Common aliases collide with other genes' symbols (PDK1 usually means PDPK1), so these pairs go to Jev with a "keep the
current mapping" option (repair_candidates.py, choose_repairs.py). check() still reports it, for the evaluation.
"""
import csv
import re
from collections import defaultdict

FAMILY_NAMES = {
    "nfkb", "nfκb", "nfkappab", "nfb", "ap1", "p38", "p38mapk", "p38mapkinase", "hsp70", "hsp90", "pkc",
    "proteinkinasec", "rhokinase", "rock", "pi3k", "jnk", "erk12", "mapk", "ampk", "camkii", "pp2a", "mao",
    "monoamineoxidase", "pfk1", "hsd17", "hsd17b", "histoneh1", "akt", "proteinkinaseb", "pkb", "hdac", "mmp",
    "cox", "cyp450", "p450", "integrin", "caspase", "histone",
}
COUNTER_IONS = {"chloride", "sulfate", "sulphate", "acetate", "nitrate", "bromide", "iodide", "hydrochloride",
                "mesylate", "maleate", "tartrate", "fumarate", "succinate", "citrate", "phosphate", "carbonate"}
CATIONS = {"lithium", "sodium", "potassium", "magnesium", "zinc", "calcium", "mercuric", "mercury", "ammonium",
           "copper", "cupric", "iron", "ferrous", "ferric", "cobalt", "nickel", "cadmium", "barium", "aluminum",
           "aluminium", "manganese", "strontium", "silver", "gold", "platinum", "lead", "tin", "chromium"}
HYPHEN_OK = {"dependent", "independent", "resistant", "sensitive", "responsive", "free", "rich", "specific", "like",
             "related", "based", "naive", "na", "abstinent", "deficient", "high", "low", "rich", "only"}
LIPID_SPECIES = re.compile(r"\(\d+:\d+")
ISOTOPE = re.compile(r"\[\d+[A-Z][a-z]?\]")


def norm(s: str | None) -> str:
    """Lowercase and keep only letters and digits (Greek letters survive; a garbled '?' is dropped)."""
    return re.sub(r"[^\w]|_", "", (s or "").lower())


def load_hgnc_symbols(path: str) -> dict[str, set[str]]:
    """Approved symbol (normalized) -> UniProt CURIEs of that gene."""
    symbols: dict[str, set[str]] = defaultdict(set)
    with open(path) as f:
        for row in csv.DictReader(f, delimiter="\t"):
            symbols[norm(row["symbol"])] |= {f"UniProtKB:{u}" for u in row["uniprot_ids"].split("|") if u}
    return symbols


def mention_span(text: str, mention: str, span: tuple[int, int] | None) -> tuple[int, int] | None:
    """The mention's character span: the given one, else its first occurrence in the text."""
    if span:
        return span
    i = text.find(mention or "")
    return (i, i + len(mention)) if mention and i >= 0 else None


def fragment(text: str, mention: str, span: tuple[int, int] | None) -> bool:
    """True when the chemical mention is part of a longer name in the text."""
    s = mention_span(text, mention, span)
    if not s:
        return False
    start, end = s
    before = text[start - 1] if start > 0 else " "
    after = text[end] if end < len(text) else " "
    if before.isalnum() or before == "-" or after.isalnum():
        return True
    if after == "-":
        # "H2O2-treated", "BPA-induced", "insulin-dependent" name the chemical itself; "acetate-CoA" doesn't
        following = re.match(r"-([a-z]+)", text[end:])
        return not (following and (following.group(1).endswith(("ed", "ing")) or following.group(1) in HYPHEN_OK))
    if norm(mention) in COUNTER_IONS:
        previous = re.findall(r"[\w-]+", text[:start])
        return bool(previous) and (previous[-1].lower() in CATIONS or previous[-1][-1:].isdigit())
    return False


def check(row: dict, symbols: dict[str, set[str]]) -> str | None:
    """Reason the sentence's mapping is wrong, or None. row: subject/object ids, names, mentions, text, spans."""
    for role in ("subject", "object"):
        curie, mention = row[role], row[f"{role}_mention"]
        if curie.startswith("UniProtKB:"):
            ups = symbols.get(norm(mention))
            if ups and curie not in ups:
                return f"symbol:{role}"
            if norm(mention) in FAMILY_NAMES:
                return f"family:{role}"
        else:
            name = row[f"{role}_name"] or ""
            if LIPID_SPECIES.search(name) and not LIPID_SPECIES.search(mention or ""):
                return f"lipid_species:{role}"
            if ISOTOPE.search(name) and not ISOTOPE.search(mention or ""):
                return f"isotope:{role}"
            if fragment(row["text"], mention, row.get(f"{role}_span")):
                return f"fragment:{role}"
    return None
