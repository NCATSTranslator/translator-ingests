"""
STRING per-taxon EDA for the qa-string-ingest branch.

Streams each {taxid}.protein.links.full.v12.0.txt.gz once and computes:
  1. per-channel summary stats (nonzero, max, mean, median, p25/75/90/95/99)
  2. Pearson correlation matrix across all 14 evidence channels + combined_score
  3. predicate-channel co-firing (ambiguity) counts at a sweep of thresholds,
     separating molecular-driving vs gene-driving channels
  4. median / IQR of scores among rows where >1 predicate channel is active
     (dispersion for "ambiguous" terms)
  5. combined_score vs its dominant component (over/underrepresentation)
  6. edge counts per predicate WITH vs WITHOUT the combined_score>500 gate
"""
import os
import csv
import sys
import gzip
import numpy as np
import pandas as pd

COLS = [
    "protein1", "protein2",
    "neighborhood", "neighborhood_transferred",
    "fusion", "cooccurence", "homology",
    "coexpression", "coexpression_transferred",
    "experiments", "experiments_transferred",
    "database", "database_transferred",
    "textmining", "textmining_transferred",
    "combined_score",
]
NUMERIC = COLS[2:]                       # 14 channels + combined_score
CH_IDX = {c: i for i, c in enumerate(NUMERIC)}
NCH = len(NUMERIC)

# predicate-driving native channels -> (predicate, family)
PRED_CHANNELS = {
    "neighborhood": ("genetic_neighborhood_of", "gene"),
    "cooccurence":  ("genetically_interacts_with", "gene"),
    "fusion":       ("gene_fusion_with", "molecular"),
    "experiments":  ("physically_interacts_with", "molecular"),
    "textmining":   ("interacts_with", "molecular"),
    "coexpression": ("coexpressed_with", "coexpression"),
}
THRESHOLDS = [150, 300, 400, 500, 600, 700, 750]
COMBINED_GATE = 500
CHANNEL_HIGH = 750


def analyze(fh):
    n = 0
    col_sum = np.zeros(NCH, dtype=np.float64)
    col_sq = np.zeros(NCH, dtype=np.float64)
    col_max = np.zeros(NCH, dtype=np.int64)
    col_nz = np.zeros(NCH, dtype=np.int64)
    hist = np.zeros((NCH, 1002), dtype=np.int64)     # value 0..1000
    XtX = np.zeros((NCH, NCH), dtype=np.float64)

    # predicate firing counts at each threshold (single channel)
    fire = {c: {t: 0 for t in THRESHOLDS} for c in PRED_CHANNELS}
    # pairwise co-firing among predicate channels, per threshold
    pred_list = list(PRED_CHANNELS)
    cofire = {t: np.zeros((len(pred_list), len(pred_list)), dtype=np.int64) for t in THRESHOLDS}
    # rows with >=2 predicate channels active (nonzero) -> collect scores for dispersion
    multi_active_rows = 0
    # combined vs dominant component
    dom_count = np.zeros(NCH, dtype=np.int64)
    combined_minus_max_sum = 0.0
    combined_gt_gate = 0

    reader = pd.read_csv(
        fh, sep=" ", header=0, usecols=list(range(2, 16)),
        names=COLS, dtype=np.int32, chunksize=1_000_000,
    )
    if True:
        for chunk in reader:
            X = chunk.to_numpy(dtype=np.int64)          # (m, NCH)
            m = X.shape[0]
            n += m
            col_sum += X.sum(0)
            col_sq += (X.astype(np.float64) ** 2).sum(0)
            col_max = np.maximum(col_max, X.max(0))
            col_nz += (X > 0).sum(0)
            for j in range(NCH):
                bc = np.bincount(X[:, j], minlength=1002)
                hist[j, :len(bc)] += bc
            Xf = X.astype(np.float64)
            XtX += Xf.T @ Xf

            # predicate firing + cofiring
            pred_cols = np.array([CH_IDX[c] for c in pred_list])
            P = X[:, pred_cols]                          # (m, npred)
            for t in THRESHOLDS:
                B = (P > t)
                for k, c in enumerate(pred_list):
                    fire[c][t] += int(B[:, k].sum())
                cofire[t] += B.T.astype(np.int64) @ B.astype(np.int64)
            # multi active (nonzero) among predicate channels
            active = (P > 0).sum(1)
            multi_active_rows += int((active >= 2).sum())

            # combined vs dominant component (exclude combined col itself)
            comp = X[:, :NCH - 1]                        # drop combined_score
            dom = comp.argmax(1)
            for d in np.bincount(dom, minlength=NCH - 1).nonzero()[0]:
                dom_count[d] += int((dom == d).sum())
            cmax = comp.max(1)
            combined = X[:, CH_IDX["combined_score"]]
            combined_minus_max_sum += float((combined - cmax).sum())
            combined_gt_gate += int((combined > COMBINED_GATE).sum())

    def pct(j, q):
        c = np.cumsum(hist[j])
        target = q / 100.0 * n
        return int(np.searchsorted(c, target))

    mean = col_sum / n
    std = np.sqrt(np.maximum(col_sq / n - mean ** 2, 0))
    # correlation from XtX
    cov = XtX / n - np.outer(mean, mean)
    denom = np.outer(std, std)
    with np.errstate(divide="ignore", invalid="ignore"):
        corr = np.where(denom > 0, cov / denom, 0.0)

    return dict(
        n=n, mean=mean, std=std, col_max=col_max, col_nz=col_nz,
        median=np.array([pct(j, 50) for j in range(NCH)]),
        p90=np.array([pct(j, 90) for j in range(NCH)]),
        p99=np.array([pct(j, 99) for j in range(NCH)]),
        corr=corr, fire=fire, cofire=cofire, pred_list=pred_list,
        multi_active_rows=multi_active_rows, dom_count=dom_count,
        combined_minus_max_sum=combined_minus_max_sum,
        combined_gt_gate=combined_gt_gate,
    )


def report(tax, r):
    n = r["n"]
    print(f"\n################## TAXON {tax}  (rows={n:,}) ##################")
    print("\n-- per-channel stats (nonzero / max / mean / median / p90 / p99) --")
    print(f"{'channel':28s} {'%nonzero':>9s} {'max':>5s} {'mean':>7s} {'med':>5s} {'p90':>5s} {'p99':>5s}")
    for j, c in enumerate(NUMERIC):
        print(f"{c:28s} {100*r['col_nz'][j]/n:8.2f}% {r['col_max'][j]:5d} "
              f"{r['mean'][j]:7.2f} {r['median'][j]:5d} {r['p90'][j]:5d} {r['p99'][j]:5d}")

    print("\n-- correlation matrix (channels x channels, x100) --")
    hdr = [c[:6] for c in NUMERIC]
    print("        " + " ".join(f"{h:>6s}" for h in hdr))
    for j, c in enumerate(NUMERIC):
        print(f"{c[:7]:7s} " + " ".join(f"{100*r['corr'][j,k]:6.0f}" for k in range(NCH)))

    print("\n-- predicate-channel firing counts at thresholds --")
    print(f"{'channel(predicate/family)':40s} " + " ".join(f"{t:>7d}" for t in THRESHOLDS))
    for c in r["pred_list"]:
        pred, fam = PRED_CHANNELS[c]
        label = f"{c}({pred[:18]}/{fam})"
        print(f"{label:40s} " + " ".join(f"{r['fire'][c][t]:7d}" for t in THRESHOLDS))

    print("\n-- co-firing (rows where BOTH channels > threshold) --")
    pl = r["pred_list"]
    for t in THRESHOLDS:
        M = r["cofire"][t]
        offdiag = M.copy(); np.fill_diagonal(offdiag, 0)
        if offdiag.sum() == 0:
            print(f"  t={t}: no co-firing pairs")
            continue
        print(f"  t={t}:")
        for a in range(len(pl)):
            for b in range(a + 1, len(pl)):
                if M[a, b] > 0:
                    fa = PRED_CHANNELS[pl[a]][1]; fb = PRED_CHANNELS[pl[b]][1]
                    tag = "GENE-vs-MOL" if {fa, fb} == {"gene", "molecular"} else f"{fa}/{fb}"
                    print(f"      {pl[a]:14s} & {pl[b]:14s}: {M[a,b]:8d}  [{tag}]")

    print(f"\n-- rows with >=2 predicate channels active (nonzero): {r['multi_active_rows']:,} "
          f"({100*r['multi_active_rows']/n:.2f}%) --")

    print("\n-- dominant component of combined_score (argmax over channels) --")
    comp_names = NUMERIC[:-1]
    order = np.argsort(-r["dom_count"])
    for d in order:
        if r["dom_count"][d] == 0:
            continue
        print(f"   {comp_names[d]:28s} dominant in {r['dom_count'][d]:10d} rows "
              f"({100*r['dom_count'][d]/n:5.2f}%)")
    print(f"   mean(combined - max_component) = {r['combined_minus_max_sum']/n:.2f}")

    print(f"\n-- combined_score > {COMBINED_GATE}: {r['combined_gt_gate']:,} rows "
          f"({100*r['combined_gt_gate']/n:.2f}%) --")


def write_csvs(tax, r, outdir):
    """Emit the report tables as machine-readable CSVs under outdir.

    Produces four files per taxon — per_channel_stats, correlation,
    predicate_firing, dominant_component — so the EDA tables can be loaded
    directly into spreadsheets/notebooks without re-parsing the text report.
    """
    os.makedirs(outdir, exist_ok=True)
    n = r["n"]

    with open(f"{outdir}/per_channel_stats_{tax}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["channel", "nonzero_pct", "max", "mean", "median", "p90", "p99"])
        for j, c in enumerate(NUMERIC):
            w.writerow([c, f"{100*r['col_nz'][j]/n:.4f}", int(r["col_max"][j]),
                        f"{r['mean'][j]:.4f}", int(r["median"][j]),
                        int(r["p90"][j]), int(r["p99"][j])])

    with open(f"{outdir}/correlation_{tax}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([""] + NUMERIC)
        for j, c in enumerate(NUMERIC):
            w.writerow([c] + [f"{r['corr'][j, k]:.4f}" for k in range(NCH)])

    with open(f"{outdir}/predicate_firing_{tax}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["channel", "predicate", "family"] + [f"gt_{t}" for t in THRESHOLDS])
        for c in r["pred_list"]:
            pred, fam = PRED_CHANNELS[c]
            w.writerow([c, pred, fam] + [r["fire"][c][t] for t in THRESHOLDS])

    comp_names = NUMERIC[:-1]
    with open(f"{outdir}/dominant_component_{tax}.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["channel", "dominant_rows", "dominant_pct"])
        for d in np.argsort(-r["dom_count"]):
            w.writerow([comp_names[d], int(r["dom_count"][d]),
                        f"{100*r['dom_count'][d]/n:.4f}"])


if __name__ == "__main__":
    # Stream a gzip file from stdin: `curl -s URL | python eda.py <taxlabel> [csv_outdir]`
    tax = sys.argv[1] if len(sys.argv) > 1 else "stdin"
    csv_outdir = sys.argv[2] if len(sys.argv) > 2 else None
    with gzip.open(sys.stdin.buffer, "rt") as fh:
        r = analyze(fh)
    report(tax, r)
    if csv_outdir:
        write_csvs(tax, r, csv_outdir)
