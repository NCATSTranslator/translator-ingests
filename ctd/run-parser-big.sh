#!/usr/bin/env bash
set -o nounset -o pipefail -o errexit

./transform_ctd.py \
    https://ctdbase.org/reports/CTD_chemicals_diseases.tsv.gz \
    ctd-output-nodes.jsonl \
    ctd-output-edges.jsonl
