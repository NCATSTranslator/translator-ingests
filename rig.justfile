
# Usage: just --ingest <ingest_dir_name> mkg-to-rig
# Populate target_info.(node|edge)_type_info using the contents of a
# TRAPI-style Meta Knowledge Graph JSON file to RIG
mkg-to-rig:
	{{run}} python src/docs/scripts/mkg_to_rig.py --ingest "$(ingest)"
