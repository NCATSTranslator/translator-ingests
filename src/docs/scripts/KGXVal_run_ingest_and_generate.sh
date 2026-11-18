#!/usr/bin/env bash
# Usage within a suitable shell (bash?):
#
#    GIT_BRANCH=my-ingest-dev-branch INGEST=my-ingest-name ./script.sh

#These two variables need to be set for this script
# to work correctly, but default to the template ingest

GIT_BRANCH="${GIT_BRANCH:-main}"
INGEST="${INGEST:-_ingest_template}"

#Clone the translator-ingests repo only for the branch we're changing.
git clone --single-branch --branch $GIT_BRANCH "https://github.com/NCATSTranslator/translator-ingests"

#Move into the cloned translator-ingests repo (with only the branch specific changes)
cd translator-ingests || exit

#Run the translator-ingests build pipeline, but only run it on the specific source we're interested in.
SOURCES=$INGEST bash -c "make run"

cd ..

TRANSFORMED_NODES=$(find translator-ingests/data/ -name "normalized_nodes.jsonl")
TRANSFORMED_EDGES=$(find translator-ingests/data/ -name "normalized_edges.jsonl")

#We need to set up the data to look like how KGXVal wants it to be.
#This is as follows ROOTDIR -> SOURCE_NAME -> normed -> nodes.jsonl + edges.jsonl
KGXVALDIR="$(mktemp -d)"
mkdir -p "$KGXVALDIR/$INGEST/normed"
cp "$TRANSFORMED_NODES" "$KGXVALDIR"/$INGEST/normed
cp "$TRANSFORMED_EDGES" "$KGXVALDIR"/$INGEST/normed

#We execute KGXVal, this will take generate a summary for each source in the working directory
(cd $KGXVALDIR && uvx --python 3.13 --from 'git+https://github.com/monarch-initiative/kgxval' ingest_summary "$KGXVALDIR")

xlsx="$(find "$KGXVALDIR/data/output" -name '*.xlsx' | head -1)"
KGX_SUMMARY_FILE=$INGEST"_kgxval_summary.xlsx"
cp "$xlsx" $KGX_SUMMARY_FILE
echo "Wrote $KGX_SUMMARY_FILE"