#These two variables need to be set for this script to work correctly.

MY_BRANCH="convert-affinity-measurements-to-study-results"
MY_SOURCE="bindingdb"

########

#Clone the translator-ingests repo only for the branch we're changing.
git clone --single-branch --branch $MY_BRANCH "https://github.com/NCATSTranslator/translator-ingests"

#Move into the cloned translator-ingests repo (with only the branch specific changes)
cd translator-ingests

#Run the translator-ingests build pipeline, but only run it on the specific source we're interested in.
SOURCES=$MY_SOURCE bash -c "make run"

cd ..

TRANSFORMED_NODES=$(find translator-ingests/data/ -name "normalized_nodes.jsonl")
TRANSFORMED_EDGES=$(find translator-ingests/data/ -name "normalized_edges.jsonl")

#We need to set up the data to look like how KGXVal wants it to be.
#This is as follows ROOTDIR -> SOURCE_NAME -> normed -> nodes.jsonl + edges.jsonl
KGXVALDIR="$(mktemp -d)"
mkdir -p "$KGXVALDIR/$MY_SOURCE/normed"
cp $TRANSFORMED_NODES $KGXVALDIR/$MY_SOURCE/normed
cp $TRANSFORMED_EDGES $KGXVALDIR/$MY_SOURCE/normed

#We execute KGXVal, this will take generate a summary for each source in the working directory
(cd $KGXVALDIR && uvx --python 3.13 --from 'git+https://github.com/monarch-initiative/kgxval' ingest_summary "$KGXVALDIR")

xlsx="$(find "$KGXVALDIR/data/output" -name '*.xlsx' | head -1)"
cp "$xlsx" $MY_SOURCE"_kgxval_summary.xlsx"
echo "Wrote $MY_SOURCE_kgxval_summary.xlsx"