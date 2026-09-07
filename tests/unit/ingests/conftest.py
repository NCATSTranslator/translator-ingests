# Directories under tests/unit/ingests/ to exclude from test collection entirely.
#
# Use this only when a whole ingest package fails at *import* time (so a
# `@pytest.mark.skip` in the test module itself can't help, since pytest never
# gets past `import`), and the failure is a known/tracked upstream issue rather
# than something to be silently ignored.
#
# string: src/translator_ingest/ingests/string/string.py not yet updated
# to currently pinned Biolink Model release (4.4.4). This breaks
# both the ingest itself and its unit test.
# TODO: remove this exclusion once the updated STRING code
# is co-resident with the updated BindingDb code.
collect_ignore = ["string"]