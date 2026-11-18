#!/usr/bin/env bash
# Usage within a suitable shell (bash?):
#
#    GIT_BRANCH=my-ingest-dev-branch INGEST=my-ingest-name ./script.sh

#These two variables need to be set for this script
# to work correctly, but default to the template ingest

GIT_BRANCH="${GIT_BRANCH:-main}"
INGEST="${INGEST:-_ingest_template}"

REPO_DIR="translator-ingests"

#Clone the translator-ingests repo only for the branch we're changing.
# If a clone is already present, reset it to a clean copy of the latest
# commit on $GIT_BRANCH instead (discarding any local changes).
if [ -d "$REPO_DIR/.git" ]; then
  echo "Resetting existing '$REPO_DIR' clone to a clean copy of the latest 'origin/$GIT_BRANCH'."
  # Explicit refspec is needed since a --single-branch clone only tracks its original branch
  git -C "$REPO_DIR" fetch origin "+refs/heads/$GIT_BRANCH:refs/remotes/origin/$GIT_BRANCH" || exit 1
  # -f discards local modifications; -B creates or resets the local branch to the remote one
  git -C "$REPO_DIR" checkout -f -B "$GIT_BRANCH" "origin/$GIT_BRANCH" || exit 1
  # Remove untracked files and directories (gitignored files, e.g. data/, are kept)
  git -C "$REPO_DIR" clean -fd || exit 1
else
  git clone --single-branch --branch "$GIT_BRANCH" "https://github.com/NCATSTranslator/translator-ingests" "$REPO_DIR"
fi

#Move into the cloned translator-ingests repo (with only the branch specific changes)
cd "$REPO_DIR" || exit

#Run the translator-ingests build pipeline, but only run it on the specific source we're interested in.
SOURCES=$INGEST bash -c "make run"

cd ..

#Only search the current ingest's data folder. It may hold output from several
# (source or transform) versions, since the data/ folder survives 'git clean -fd',
# so take the most recently written file.
INGEST_DATA_DIR="$REPO_DIR/data/$INGEST"
TRANSFORMED_NODES=$(find "$INGEST_DATA_DIR" -name "normalized_nodes.jsonl" -type f -exec ls -t {} + | head -1)
TRANSFORMED_EDGES=$(find "$INGEST_DATA_DIR" -name "normalized_edges.jsonl" -type f -exec ls -t {} + | head -1)

if [ -z "$TRANSFORMED_NODES" ] || [ -z "$TRANSFORMED_EDGES" ]; then
  echo "Error: normalized_nodes.jsonl and/or normalized_edges.jsonl not found under '$INGEST_DATA_DIR'." >&2
  exit 1
fi

#We need to set up the data to look like how KGXVal wants it to be.
#This is as follows ROOTDIR -> SOURCE_NAME -> normed -> nodes.jsonl + edges.jsonl
KGXVALDIR="$(mktemp -d)"
mkdir -p "$KGXVALDIR/$INGEST/normed"
cp "$TRANSFORMED_NODES" "$KGXVALDIR"/$INGEST/normed
cp "$TRANSFORMED_EDGES" "$KGXVALDIR"/$INGEST/normed

#We execute KGXVal, this will take generate a summary for each source in the working directory
(cd $KGXVALDIR && uvx --python 3.13 --from 'git+https://github.com/monarch-initiative/kgxval' ingest_summary "$KGXVALDIR")

xlsx="$(find "$KGXVALDIR/data/output" -name '*.xlsx' | head -1)"
KGX_SUMMARY_FILE="${REPO_DIR}/data/${INGEST}/${INGEST}_kgxval_summary.xlsx"
cp "$xlsx" $KGX_SUMMARY_FILE
echo "Wrote $KGX_SUMMARY_FILE"