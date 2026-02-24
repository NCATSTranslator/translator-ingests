# Centralized Deterministic Association IDs

## Summary

Replaces random UUID generation for association edge IDs with deterministic, content-based hashing. This uses biolink-model's new `AssociationIdConfig` feature (branch `issue-1707-deterministic-association-ids`) which generates SHA-256-based IDs from association field values via a Pydantic `model_validator(mode='before')`. The same edge data always produces the same ID, eliminating non-determinism across pipeline runs.

All 29 ingests have been updated: callers no longer pass `id=` when constructing associations, and the biolink model's validator auto-generates IDs from the association's content.

## Architecture

### Singleton auto-configure

The module `src/translator_ingest/util/association_id.py` uses a singleton pattern to configure the ID strategy automatically at import time:

```python
from biolink_model.datamodel.pydanticmodel_v2 import AssociationIdConfig, IdStrategy

_association_id_strategy_configured = False

def configure_association_ids(
    strategy: IdStrategy = IdStrategy.ALL_FIELDS,
    custom_fields: list[str] | None = None,
    force: bool = False,
) -> None:
    global _association_id_strategy_configured
    if _association_id_strategy_configured and not force:
        return
    AssociationIdConfig.strategy = strategy
    if custom_fields is not None:
        AssociationIdConfig.custom_fields = custom_fields
    _association_id_strategy_configured = True

# Apply default configuration on first import
configure_association_ids()
```

This is the single place to change the ID strategy for the entire pipeline. The `ALL_FIELDS` strategy hashes all non-None fields on the association to produce a `uuid:` prefixed deterministic ID.

The singleton guard ensures the configuration runs exactly once — at module import time — regardless of how the code is invoked (via `pipeline.py`, Koza directly, or in tests). `pipeline.py` uses a bare `import translator_ingest.util.association_id` to trigger the singleton. The `force=True` parameter allows tests to reconfigure with different strategies.

### How it works

The biolink-model `Association` base class (and all subclasses) now has a `model_validator(mode='before')` that runs before Pydantic field validation. When no `id` is provided (or `id` is `None`), the validator:

1. Collects all non-None field values from the association data
2. Builds a canonical string representation
3. Computes a SHA-256 hash
4. Prefixes it with `uuid:` to produce the ID

When an explicit `id` IS provided, the validator leaves it untouched.

## How ingests changed

### Pattern A: `id=entity_id()` (23 ingests)

The most common pattern. The `entity_id()` call (which generated a random UUID) was removed from all association constructors:

```python
# Before (ctd.py):
association = ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation(
    id=entity_id(),
    subject=chemical.id,
    predicate="biolink:related_to",
    object=disease.id,
    ...
)

# After:
association = ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation(
    subject=chemical.id,
    predicate="biolink:related_to",
    object=disease.id,
    ...
)
```

The `entity_id` import was cleaned up from files where it was no longer used. Files that still use `entity_id()` for non-Association objects (e.g., `Study` in tmkp.py, `edge_id` in icees.py for `get_edge_class`) retain the import.

### Pattern B: `id=str(uuid.uuid4())` (3 ingests — gtopdb, go_cam, _ingest_template)

Same as Pattern A but using stdlib `uuid` instead of `entity_id()`:

```python
# Before (gtopdb.py):
association = ChemicalAffectsGeneAssociation(
    id=str(uuid.uuid4()),
    subject=...,
    ...
)

# After:
association = ChemicalAffectsGeneAssociation(
    subject=...,
    ...
)
```

The `import uuid` was removed from files where it was no longer needed.

### Pattern C: Source ID passthrough (3 ingests — ctkp, dakp, geneticskp)

These ingests prefer the source's own ID when present, falling back to a random UUID when absent. The fallback now produces `None` instead, which triggers deterministic generation:

```python
# Before (ctkp.py):
edge_props = {
    "id": record.get("id", str(uuid.uuid4())),
    "subject": subject_id,
    ...
}

# After:
edge_props = {
    "id": record.get("id"),  # None triggers deterministic ID generation
    "subject": subject_id,
    ...
}
```

Source-provided IDs are still preserved when they exist.

### What was NOT changed

- **RetrievalSource** objects — `RetrievalSource` is not an Association subclass and doesn't have the deterministic ID validator. These continue using `entity_id()` or `str(uuid.uuid4())`:
  - `util/biolink.py:knowledge_sources_from_trapi()` — `entity_id()` for RetrievalSource
  - `ctkp.py`, `dakp.py` — `str(uuid.uuid4())` for RetrievalSource
  - `build_association_knowledge_sources()` from bmt — uses `entity_id()` internally for RetrievalSource
- **Study / TextMiningStudyResult** objects (tmkp.py, semmeddb.py) — NamedThing subclasses, not Associations
- **`edge_id` in icees.py / cohd.py** — icees uses `edge_id = entity_id()` for `get_edge_class()` and `get_icees_supporting_study()`, not for association IDs

## Biolink-model branch compatibility fixes

Updating to the `issue-1707-deterministic-association-ids` branch introduced two additional changes beyond deterministic IDs:

1. **semmeddb.py** — `TextMiningStudyResult` and `Study` (NamedThing subclasses) now require an explicit `id`. Added `id=entity_id()` to both constructors and restored the `entity_id` import.

2. **hpoa.py** — `CorrelatedGeneToDiseaseAssociation` no longer accepts `biolink:associated_with` as a predicate. Changed to `biolink:correlated_with` (the semantically appropriate replacement for the "contributes_to" qualified predicate case). Updated the corresponding test expectation.

## Test changes

### New test file: `tests/test_association_ids.py`

10 tests covering the deterministic ID infrastructure:

| Test | What it verifies |
|------|-----------------|
| `test_configure_sets_all_fields_strategy` | `configure_association_ids()` sets `IdStrategy.ALL_FIELDS` |
| `test_configure_sets_custom_strategy_with_fields` | Custom strategy with explicit field list |
| `test_deterministic_id_generated_without_explicit_id` | Associations get `uuid:` prefixed IDs |
| `test_same_inputs_produce_same_id` | Identical inputs → identical ID (determinism) |
| `test_different_inputs_produce_different_ids` | Different inputs → different ID (no collisions) |
| `test_explicit_id_preserved` | Passing `id="my-explicit-id"` is not overwritten |
| `test_deterministic_id_on_association_subclass` | Works on subclasses like `ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation` |
| `test_deterministic_ids_vary_by_content` (3x) | Parametrized across subject/object combinations |

Tests use an `autouse` fixture to reset `AssociationIdConfig` after each test, preventing state leakage.

### Updated test: `tests/unit/ingests/hpoa/test_hpoa.py`

Updated the expected predicate for `CorrelatedGeneToDiseaseAssociation` from `biolink:associated_with` to `biolink:correlated_with` to match the model change.

## Files changed

| File | Change |
|------|--------|
| `pyproject.toml` | biolink-model → `issue-1707-deterministic-association-ids` branch |
| `src/translator_ingest/util/association_id.py` | **New** — centralized ID configuration |
| `src/translator_ingest/pipeline.py` | Call `configure_association_ids()` at startup |
| `src/translator_ingest/ingests/alliance/alliance.py` | Remove `id=entity_id()` from 3 constructors |
| `src/translator_ingest/ingests/bgee/bgee.py` | Remove `id=entity_id()` from 1 constructor |
| `src/translator_ingest/ingests/bindingdb/bindingdb.py` | Remove `id=entity_id()` from 1 constructor |
| `src/translator_ingest/ingests/chembl/chembl.py` | Remove `id=entity_id()` from 4 constructors |
| `src/translator_ingest/ingests/cohd/cohd.py` | Remove `edge_id` variable and `id=edge_id` |
| `src/translator_ingest/ingests/ctd/ctd.py` | Remove `id=entity_id()` from 7 constructors |
| `src/translator_ingest/ingests/ctkp/ctkp.py` | Change `record.get("id", uuid)` → `record.get("id")` |
| `src/translator_ingest/ingests/dakp/dakp.py` | Change `record.get("id", uuid)` → `record.get("id")` |
| `src/translator_ingest/ingests/dgidb/dgidb.py` | Remove `id=entity_id()` from 3 constructors |
| `src/translator_ingest/ingests/diseases/diseases.py` | Remove `id=entity_id()` from 2 constructors |
| `src/translator_ingest/ingests/drug_rep_hub/drug_rep_hub.py` | Remove `id=entity_id()` from 3 constructors |
| `src/translator_ingest/ingests/drugcentral/drugcentral.py` | Remove `id=entity_id()` from 1 constructor |
| `src/translator_ingest/ingests/gene2phenotype/gene2phenotype.py` | Remove `id=entity_id()` from 1 constructor |
| `src/translator_ingest/ingests/geneticskp/geneticskp.py` | Change `record.get("id", uuid)` → `record.get("id")` |
| `src/translator_ingest/ingests/go_cam/go_cam.py` | Remove `id=str(uuid.uuid4())` from 1 constructor |
| `src/translator_ingest/ingests/goa/goa.py` | Remove `id=entity_id()` from 2 constructors |
| `src/translator_ingest/ingests/gtopdb/gtopdb.py` | Remove `id=str(uuid.uuid4())` from 11 constructors |
| `src/translator_ingest/ingests/hpoa/hpoa.py` | Remove `id=entity_id()` from 4 constructors; fix predicate |
| `src/translator_ingest/ingests/icees/icees.py` | Remove `id=entity_id()` from 1 constructor |
| `src/translator_ingest/ingests/intact/intact.py` | Remove `id=entity_id()` from 1 constructor |
| `src/translator_ingest/ingests/panther/panther.py` | Remove `id=entity_id()` from 3 constructors |
| `src/translator_ingest/ingests/pathbank/pathbank.py` | Remove `id=entity_id()` from 15 constructors |
| `src/translator_ingest/ingests/semmeddb/semmeddb.py` | Remove `id=entity_id()` from 3 assoc constructors; add to Study/TextMiningStudyResult |
| `src/translator_ingest/ingests/sider/sider.py` | Remove `id=entity_id()` from 1 constructor |
| `src/translator_ingest/ingests/signor/signor.py` | Remove `id=entity_id()` from 8 constructors |
| `src/translator_ingest/ingests/tmkp/tmkp.py` | Remove `"id": entity_id()` from assoc_kwargs dict |
| `src/translator_ingest/ingests/ttd/ttd.py` | Remove `id=entity_id()` from 4 constructors |
| `src/translator_ingest/ingests/ubergraph/ubergraph.py` | Remove `id=entity_id()` from 1 constructor |
| `src/translator_ingest/ingests/_ingest_template/_ingest_template.py` | Remove `id=` from 3 template constructors |
| `tests/test_association_ids.py` | **New** — 10 tests for deterministic ID infrastructure |
| `tests/unit/ingests/hpoa/test_hpoa.py` | Update expected predicate for model change |
