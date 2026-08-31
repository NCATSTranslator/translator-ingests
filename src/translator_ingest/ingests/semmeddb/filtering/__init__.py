"""SemMedDB post-normalization filter package.

The pipeline's optional source-filter stage discovers ``filter_normalized_kgx`` here
(see ``translator_ingest.pipeline.get_source_filter``). This subpackage is intentionally
separate from the ingest's top-level files so editing the filter does not change the
content-hash transform version.
"""

from translator_ingest.ingests.semmeddb.filtering.pmid_filter import filter_normalized_kgx

__all__ = ["filter_normalized_kgx"]
