#!/usr/bin/env bash
set -o nounset -o pipefail -o errexit

./transform_ctd.py \
    CTD_chemicals_diseases_small.tsv \
    ctd-output-nodes.jsonl \
    ctd-output-edges.jsonl
