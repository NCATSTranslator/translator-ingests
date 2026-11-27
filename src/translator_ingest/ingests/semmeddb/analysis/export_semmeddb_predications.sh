#!/bin/bash
# Export one PREDICATION_ID range of SemMedDB predications with offsets and sentence text.
#
# Provenance for the chunk_NNN.tsv.zst inputs of build_text_locations.py. The chunks were made on
# the KG2 build host, against its SemMedDB VER43 2023 MySQL database, by export_chunk.sh; a copy
# of it sits next to the chunks with header.tsv and starts.txt. This script is equivalent to it:
# the same SELECT list, joins, WHERE clause and mysql | tee | zstd pipeline. It differs only in
# quoting its variables, naming the range size RANGE_SIZE (export_chunk.sh writes 4999999 and
# 5000000 inline), ending with exit "$status" (export_chunk.sh exits with the status of its last
# echo), and these comments. Run from the directory that is to hold the chunks; header.tsv and
# starts.txt are what these two commands write (seq -f %.0f prints integers with BSD seq too):
#
#     printf 'PREDICATION_ID\tSENTENCE_ID\tPMID\tPREDICATE\tSUBJECT_CUI\tSUBJECT_NAME\tSUBJECT_SEMTYPE\tOBJECT_CUI\tOBJECT_NAME\tOBJECT_SEMTYPE\tSENT_TYPE\tSENT_NUMBER\tSENT_START_INDEX\tSENT_END_INDEX\tSUBJECT_TEXT\tSUBJECT_START_INDEX\tSUBJECT_END_INDEX\tSUBJECT_SCORE\tINDICATOR_TYPE\tPREDICATE_START_INDEX\tPREDICATE_END_INDEX\tOBJECT_TEXT\tOBJECT_START_INDEX\tOBJECT_END_INDEX\tOBJECT_SCORE\tSENTENCE\n' > header.tsv
#     seq -f %.0f 10000000 5000000 205000000 > starts.txt
#     xargs -P 8 -I{} ./export_semmeddb_predications.sh {} < starts.txt
#
# header.tsv names the columns of the SELECT list below, in order; the build requires it. Each run
# writes chunk_NNN.tsv.zst (mysql -B -N output: no header, no quoting, backslash escapes \t \n \\
# \0 inside fields, NULL written as NULL), chunk_NNN.rows (line count), chunk_NNN.status (pipeline
# exit status) and chunk_NNN.err (mysql stderr), and exits with the pipeline status. chunk_NNN
# holds PREDICATION_IDs NNN * RANGE_SIZE to (NNN + 1) * RANGE_SIZE - 1; the build's
# PREDICATION_ID_RANGE_SIZE must equal RANGE_SIZE. The build's checks guarantee completeness:
# every status 0, no .part file left, the .rows files summing to the table's COUNT(*)
# (--expected-rows), and every chunk's ids unique and inside its range. That count is
# SELECT COUNT(*) FROM semmeddb.PREDICATION on the same database, during the export; it is kept next
# to the chunks as predication_count.txt, with comment lines giving this provenance. For example:
#
#     mysql --defaults-extra-file="$HOME/kg2-build/mysql-config.conf" -B -N \
#         -e "SELECT COUNT(*) FROM semmeddb.PREDICATION" > predication_count.txt
set -o pipefail
RANGE_SIZE=5000000
start=$1
end=$((start + RANGE_SIZE - 1))
name=chunk_$(printf "%03d" $((start / RANGE_SIZE)))
mysql --defaults-extra-file="$HOME/kg2-build/mysql-config.conf" -B -N --quick -e "
SELECT p.PREDICATION_ID, p.SENTENCE_ID, p.PMID, p.PREDICATE, p.SUBJECT_CUI, p.SUBJECT_NAME, p.SUBJECT_SEMTYPE, p.OBJECT_CUI, p.OBJECT_NAME, p.OBJECT_SEMTYPE,
       s.TYPE, s.NUMBER, s.SENT_START_INDEX, s.SENT_END_INDEX,
       a.SUBJECT_TEXT, a.SUBJECT_START_INDEX, a.SUBJECT_END_INDEX, a.SUBJECT_SCORE, a.INDICATOR_TYPE, a.PREDICATE_START_INDEX, a.PREDICATE_END_INDEX,
       a.OBJECT_TEXT, a.OBJECT_START_INDEX, a.OBJECT_END_INDEX, a.OBJECT_SCORE, s.SENTENCE
FROM semmeddb.PREDICATION p
LEFT JOIN semmeddb.PREDICATION_AUX a ON a.PREDICATION_ID = p.PREDICATION_ID
LEFT JOIN semmeddb.SENTENCE s ON s.SENTENCE_ID = p.SENTENCE_ID
WHERE p.PREDICATION_ID BETWEEN $start AND $end" 2> "$name.err" \
  | tee >(wc -l > "$name.rows") | zstd -q -10 -T2 > "$name.tsv.zst.part"
status=$?
if [ "$status" -eq 0 ]; then mv "$name.tsv.zst.part" "$name.tsv.zst"; fi
echo "$status" > "$name.status"
exit "$status"
