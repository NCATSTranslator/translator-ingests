#!/usr/bin/env bash
# Run the translator-ingests build pipeline for one ingest on a given branch of
# this (already cloned) translator-ingests project, then generate a KGXVal
# summary spreadsheet of the ingest's normalized nodes and edges.
#
# Usage, from any directory, within a bash shell (e.g. Git Bash on Windows):
#
#    GIT_BRANCH=my-ingest-dev-branch INGEST=my-ingest-name path/to/KGXVal_run_ingest_and_generate.sh
#
#  GIT_BRANCH - an existing branch (local, or as origin/GIT_BRANCH) of this project;
#               defaults to the currently checked out branch. If it differs from the
#               current branch, the script switches to it (after confirmation) and
#               switches back to the original branch when it exits.
#  INGEST     - the ingest (source) to run; defaults to "_ingest_template".
#
# The summary is written to <project root>/data/<INGEST>/<INGEST>_kgxval_summary.xlsx

#Ask the user a yes/no question, repeating it until a valid answer is given.
# Arguments: $1 - the question to display.
# Returns 0 for 'yes', 1 for 'no'; aborts the script if no answer can be read.
confirm() {
  local reply
  while true; do
    read -r -p "$1 [yes/no]: " reply || [ -n "$reply" ] || { echo; echo "No answer given; aborting." >&2; exit 1; }
    case "$reply" in
      [Yy] | [Yy][Ee][Ss]) return 0 ;;
      [Nn] | [Nn][Oo]) return 1 ;;
      *) echo "Please answer 'yes' or 'no'." ;;
    esac
  done
}

#Clean up when the script exits, however it exits: remove the temporary KGXVal
# working directory (if created) and switch back to the original git ref (if the
# script switched away from it).
cleanup() {
  if [ -n "$KGXVALDIR" ]; then
    rm -rf "$KGXVALDIR"
  fi
  if [ -n "$SWITCHED" ]; then
    echo "Switching '$REPO_DIR' back to '$ORIGINAL_REF'."
    git -C "$REPO_DIR" switch $ORIGINAL_SWITCH_OPTS "$ORIGINAL_REF" \
      || echo "Warning: could not switch '$REPO_DIR' back to '$ORIGINAL_REF'; do so manually." >&2
  fi
}

#The whole script runs inside main(), so that bash has parsed all of it before
# anything runs: 'git switch' may rewrite this very file on disk mid-run.
main() {
  #Absolute path of the directory holding this script (symlinks resolved)
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)" || exit 1
  #Root of the translator-ingests project that contains this script
  REPO_DIR="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)" \
    || { echo "Error: '$SCRIPT_DIR' is not inside a git checkout of translator-ingests." >&2; exit 1; }

  trap cleanup EXIT

  #Remember where the project is now, so we can switch back to it at the end.
  # On a detached HEAD there is no current branch, so remember the commit instead.
  ORIGINAL_REF="$(git -C "$REPO_DIR" branch --show-current)" || exit 1
  ORIGINAL_SWITCH_OPTS=""
  if [ -z "$ORIGINAL_REF" ]; then
    ORIGINAL_REF="$(git -C "$REPO_DIR" rev-parse HEAD)" || exit 1
    ORIGINAL_SWITCH_OPTS="--detach"
  fi

  GIT_BRANCH="${GIT_BRANCH:-$ORIGINAL_REF}"
  INGEST="${INGEST:-_ingest_template}"

  echo "Project: '$REPO_DIR' (currently on '$ORIGINAL_REF'); branch: '$GIT_BRANCH'; ingest: '$INGEST'."

  #Switch to GIT_BRANCH if we're not already on it. The branch must already exist,
  # locally or as origin/GIT_BRANCH: nothing is cloned, fetched, pulled or reset.
  if [ "$GIT_BRANCH" != "$ORIGINAL_REF" ]; then
    git -C "$REPO_DIR" rev-parse --verify --quiet "refs/heads/$GIT_BRANCH" >/dev/null \
      || git -C "$REPO_DIR" rev-parse --verify --quiet "refs/remotes/origin/$GIT_BRANCH" >/dev/null \
      || { echo "Error: branch '$GIT_BRANCH' not found locally or as 'origin/$GIT_BRANCH'." >&2; exit 1; }
    #Uncommitted changes are carried over by the switch (or block it, if they conflict)
    if [ -n "$(git -C "$REPO_DIR" status --porcelain --untracked-files=no)" ]; then
      echo "Warning: '$REPO_DIR' has uncommitted changes on '$ORIGINAL_REF'; they will be carried over to '$GIT_BRANCH'." >&2
    fi
    confirm "Switch '$REPO_DIR' from '$ORIGINAL_REF' to branch '$GIT_BRANCH'?" \
      || { echo "Aborted by user."; exit 1; }
    git -C "$REPO_DIR" switch "$GIT_BRANCH" || exit 1
    SWITCHED=1
  fi

  #Run the translator-ingests build pipeline (from the project root), but only on the
  # specific source we're interested in, and only if the user approves. If skipped,
  # any output already present under data/$INGEST is used for the KGXVal summary.
  if confirm "(Re-)run the translator-ingests build pipeline ('make run') for ingest '$INGEST'?"; then
    (cd "$REPO_DIR" && SOURCES="$INGEST" make run) \
      || { echo "Error: 'make run' failed for ingest '$INGEST'." >&2; exit 1; }
  else
    echo "Skipping the build pipeline; using existing output under '$REPO_DIR/data/$INGEST'."
  fi

  #Only search the current ingest's data folder. It may hold output from several
  # (source or transform) versions, since the (gitignored) data/ folder persists
  # across runs and branch switches, so take the most recently written file.
  INGEST_DATA_DIR="$REPO_DIR/data/$INGEST"
  #(find's errors are hidden: a missing data folder is reported clearly just below)
  TRANSFORMED_NODES=$(find "$INGEST_DATA_DIR" -name "normalized_nodes.jsonl" -type f -exec ls -t {} + 2>/dev/null | head -1)
  TRANSFORMED_EDGES=$(find "$INGEST_DATA_DIR" -name "normalized_edges.jsonl" -type f -exec ls -t {} + 2>/dev/null | head -1)

  if [ -z "$TRANSFORMED_NODES" ] || [ -z "$TRANSFORMED_EDGES" ]; then
    echo "Error: normalized_nodes.jsonl and/or normalized_edges.jsonl not found under '$INGEST_DATA_DIR'." >&2
    exit 1
  fi

  #We need to set up the data to look like how KGXVal wants it to be.
  #This is as follows ROOTDIR -> SOURCE_NAME -> normed -> nodes.jsonl + edges.jsonl
  #(The temporary directory is removed by cleanup() on exit.)
  KGXVALDIR="$(mktemp -d)" || exit 1
  mkdir -p "$KGXVALDIR/$INGEST/normed"
  cp "$TRANSFORMED_NODES" "$TRANSFORMED_EDGES" "$KGXVALDIR/$INGEST/normed/" || exit 1

  #We execute KGXVal, this will generate a summary for each source in the working directory
  (cd "$KGXVALDIR" && uvx --python 3.13 --from 'git+https://github.com/monarch-initiative/kgxval' ingest_summary "$KGXVALDIR") \
    || { echo "Error: KGXVal ingest_summary failed for ingest '$INGEST'." >&2; exit 1; }

  xlsx="$(find "$KGXVALDIR/data/output" -name '*.xlsx' -type f 2>/dev/null | head -1)"
  if [ -z "$xlsx" ]; then
    echo "Error: KGXVal failed to generate a KGX .xlsx summary file." >&2
    exit 1
  fi
  KGX_SUMMARY_FILE="${REPO_DIR}/data/${INGEST}/${INGEST}_kgxval_summary.xlsx"
  mv -f "$xlsx" "$KGX_SUMMARY_FILE" || exit 1
  echo "Wrote $KGX_SUMMARY_FILE"
}

#Keep 'exit' on the same line, so nothing after it is read from a possibly rewritten file
main "$@"; exit
