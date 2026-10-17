# Directories under tests/unit/ingests/ to exclude from test collection entirely.
#
# Use this only when a whole ingest package fails at *import* time (so a
# `@pytest.mark.skip` in the test module itself can't help, since pytest never
# gets past `import`), and the failure is a known/tracked upstream issue rather
# than something to be silently ignored.
#
# bindingdb: src/translator_ingest/ingests/bindingdb/bindingdb.py imports
# `AffinityMeasurement` from biolink_model.datamodel.pydanticmodel_v2, which does
# not exist in the currently pinned Biolink Model release (4.4.4). This breaks
# both the ingest itself and its unit test.
# TODO: remove this exclusion once AffinityMeasurement ships in Biolink Model
# (or bindingdb.py is updated to not depend on it), and re-enable the test.
collect_ignore = ["bindingdb"]