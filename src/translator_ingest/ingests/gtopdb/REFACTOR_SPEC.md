# GtoPdb refactor specification

Status: implementation-ready plan; no GtoPdb behavior changes authorized by this specification.

## Objective and baseline

Refactor [gtopdb.py](gtopdb.py) to follow the structure of the completed
[SIGNOR refactor](../signor/signor.py): source-local declarative mappings, small
typed helpers, one-record graph construction, and a thin batch wrapper. Preserve
GtoPdb's own scientific interpretation, output shape, and failure behavior.

The inspected GtoPdb baseline is commit `72f6588723b6d0d0403041badecbe4e54549f38f`,
with source blob `4bcf458f969d4e02edb33978ceef5a9ffe2264dc`. Its main transform spans
962 lines. The existing [unit tests](../../../../tests/unit/ingests/gtopdb/test_gtopdb.py)
check association-model serialization, not the transform's mapping decisions.
If implementation starts from another version, characterize that version first
and reconcile differences with this specification.

Consistency means matching SIGNOR's boundaries and testing discipline, not
importing SIGNOR's private helpers or forcing both sources into one mapping model.

## Scope

In scope:

- Add real functional tests before restructuring production code.
- Extract Type/Action mapping and endogenous context selection.
- Consolidate node construction, association construction, and publication assignment.
- Extract `_transform_record(record) -> KnowledgeGraph | None`.
- Preserve the existing decorated batch entry point and preparation contract.
- Add helper docstrings/doctests and remove obsolete comments around refactored code.

Out of scope:

- Changes to scientific mappings, filtering policy, or unknown-value policy.
- Changes to downloads, `get_latest_version`, reader configuration, or dependencies.
- Rewriting pandas preparation, switching to streaming output, or adding deduplication.
- A shared SIGNOR/GtoPdb edge factory, cross-ingest mapping framework, or global diagnostics system.
- Applying GtoPdb's publications policy to other ingests or modifying SIGNOR again.

Expected implementation changes are limited to `gtopdb.py`, its unit tests, and
small local test fixtures. Keep mapping tables in the module initially; a separate
private mapping module is optional only if it materially improves readability.

## Compatibility contract

### Entry points, ordering, and nodes

- Keep `@koza.prepare_data(tag="gtopdb_interaction_parsing")` and
  `@koza.transform(tag="gtopdb_interaction_parsing")` and their public signatures.
- `transform_ingest_all` consumes an iterable and returns one `KnowledgeGraph`
  inside an iterable, including for empty and entirely filtered input.
- Preserve input-record order, subject-before-object node order, primary-before-
  interaction edge order, and repeated nodes/edges across repeated prepared records.
- Subjects remain `ChemicalEntity(id="PUBCHEM.COMPOUND:" + subject_id, name=subject_name)`;
  objects remain `Protein(id="UniProtKB:" + object_id, name=object_name)`.
- Do not trim, normalize, or coerce these prepared identifiers differently.
- Allocate a fresh `entity_id()` for every emitted edge. Never place a generated
  ID in a common attributes dictionary reused by multiple edges.
- Preserve validation/access order. Unlike SIGNOR, the current GtoPdb transform
  constructs both nodes before reading publications, endogenous status, or mapping
  labels. Moving filtering ahead of construction could hide existing errors.

### Context and mapping

- Only `Endogenous == "TRUE"` selects `biolink:regulates` and the
  `upregulated`/`downregulated` direction pair. Other currently accepted values use
  `biolink:affects` and `increased`/`decreased`. Do not substitute generic truthiness.
- The exact `(Type, Action)` pair determines the rule; an Action does not have a
  universal meaning independent of Type.
- Preserve `ChemicalAffectsGeneAssociation` for the current primary effect edges,
  even when their predicate is `biolink:regulates`.
- Preserve the generic `Association` with `biolink:related_to` for the literal-string
  pair `("None", "None")`.
- Preserve qualified predicates, aspects, directions, mechanisms, explicit omissions,
  and the presence or absence of the additional physical-interaction edge.
- Additional edges remain `PairwiseMolecularInteraction` with
  `biolink:directly_physically_interacts_with`. They do not inherit the primary's
  causal, aspect, direction, or mechanism qualifiers. This differs from SIGNOR.
- Do not apply SIGNOR's fail-on-unknown mapping policy. GtoPdb currently has
  type-specific defaults, intentional skips, and exceptional paths.

### Evidence and provenance

- Retain `GTOPDB_SOURCES`, `knowledge_assertion`, and `manual_agent` on every edge.
- Parse publications exactly as today: split a truthy `PubMed ID` on `|`, prefix
  each token with `PMID:`, and attach the list to all emitted edges.
- Do not newly strip whitespace, remove empty tokens, deduplicate, or validate
  publication strings in the transform. Falsey publications remain unassigned.
- Do not add SIGNOR-specific confidence scores, supporting text, anatomy, or species context.
- Ensure absent publications in one record cannot inherit publications from a prior record.

### Preparation

Keep preparation behavior unchanged and cover it with local-file tests:

- Read `ligands.csv` from `koza.input_files_dir`, skipping its metadata row and
  preserving the existing identifier dtypes and whitespace handling.
- Preserve duplicate ligand-key behavior, string conversion of CSV values, missing
  mappings, and current treatment of blank or missing PubChem CIDs. Do not clean
  these up as part of the transform refactor.
- Retain the selected columns, initial duplicate removal, missing-ID filtering,
  grouping keys, and `dropna=False` treatment of missing grouping values.
- Preserve the ordered unique aggregation of publication field strings. This is
  not a new deduplication of individual PMID tokens inside those strings.
- Retain column renaming, ligand-to-PubChem mapping, final filtering, and output order.
- Characterize actual pandas-produced missing values separately from Python `None`
  and the literal source string `"None"`.

## Proposed implementation

### 1. Source-local mapping data

Move action lists and repeated qualifier choices out of the per-record loop into
module-level tables. Encode a single rule per exact Type/Action combination,
with explicit type-specific defaults where the original code has them.

A rule must express the primary association kind, predicate selection, qualified
predicate, aspect, direction selection, mechanism, and whether to emit a physical
interaction. Represent positive/negative direction symbolically until endogenous
context is applied; an omitted direction must stay distinct from either sign.

Distinguish explicit skips from missing table entries so a default cannot override
an intentional exclusion. Do not maintain a separate extra-edge action list that
can drift out of sync with the qualifier table.

### 2. Pure resolution helpers

Use private, documented, typed helpers for endogenous context and interaction
resolution. The resolver produces a resolved mapping or an intentional skip.
A small frozen `_InteractionMapping` dataclass is appropriate for the resolved
fields; do not return a long positional tuple or introduce an inheritance hierarchy.

Suggested resolved fields: `association_class`, `predicate`,
`qualified_predicate`, `object_aspect_qualifier`, `object_direction_qualifier`,
`causal_mechanism_qualifier`, and `emit_physical_interaction`.

Keep raw-record access at the boundary. Helper annotations and handling must reflect
the missing-value forms observed from real preparation, not assume all values are
`str | None` without tests. No helper should mutate shared mapping data.

Exceptional legacy paths are not intentional skips. Keep their compatibility
handling explicit at the record boundary and tested; do not hide errors with broad
`try/except`, `dict.get` defaults, or automatic conversion to a generic edge.

### 3. Single-record assembly

`_transform_record(record: dict[str, Any]) -> KnowledgeGraph | None` owns node
construction, ordered source-field access, mapping resolution, and graph assembly.
Build common subject/object/provenance attributes once. Construct the primary
edge once, construct the optional interaction edge once, then assign publications
in one place. Keep the different primary/interaction qualifier policies visible.

Use small helpers only where they clarify a policy or remove meaningful repetition;
a publication-assignment loop need not become a cross-ingest utility. Return `None`
only for a characterized skip, not for an unexpected exception.

### 4. Batch orchestration

Follow SIGNOR's `transform_ingest_all`: iterate records, call `_transform_record`,
extend node and edge lists for returned graphs, then return one aggregate graph.
No mapping rules, evidence rules, deduplication, or per-record mutable state belong
in this wrapper.

## Characterization targets requiring special care

These are observed baseline behaviors, not recommendations about correct biology.
The table is a minimum regression set, not the complete mapping inventory.

| Input or family | Baseline behavior to retain |
| --- | --- |
| Activator: Agonist, Binding, Full agonist, Partial agonist | Effect edge plus physical interaction; type-specific mechanisms and positive direction. |
| Activator: unrecognized action | One positive effect edge, with no mechanism; not a skip or new unknown-label error. |
| Inhibitor: unrecognized action | One negative effect edge, with no mechanism. Feedback inhibition has its own mechanism. |
| Agonist: Inverse agonist | Negative direction despite the Type label. |
| Antagonist: Partial agonist; Fusion protein: Binding; Subunit-specific: Mixed | Skip. |
| Allosteric modulator: Binding, Biphasic, Mixed | Two edges; primary has activity and a mechanism but no direction or qualified predicate. |
| Allosteric modulator: Neutral or literal `"None"` | Skip. The literal `"None"` does not pass the current outer action-list guard. |
| Allosteric modulator: Python `None` | Enters the outer guard but leaves predicate as the string `"None"`; current construction raises `ValidationError`. |
| Antibody: Binding or literal `"None"`; Channel blocker: literal `"None"` or Pore blocker; Gating inhibitor: literal `"None"` | Preserve each branch's absent direction/qualified predicate and its specific mechanism, including a missing mechanism where applicable. |
| Fusion protein: Inhibition | Two edges; primary mechanism is `molecular_channel_blockage`, not generic inhibition. |
| Literal Type `"None"`: Binding or Competitive | Skip. |
| Literal Type `"None"`: literal Action `"None"` | One generic `related_to` edge. |
| Literal Type `"None"`: Potentiation | One positive qualified effect edge. |
| Literal Type `"None"`: unsupported action | With publications, raises `AttributeError` on the unconstructed association; without publications, skips. |
| Subunit-specific: unsupported action | Current construction raises `ValidationError` for predicate string `"None"`. |
| Python Type `None` or an unrecognized Type | No matching route; still subject to earlier field access and node validation. |

Do not silently repair the exceptional rows while extracting the mapping. Record
them as named regression cases. A desired correction requires a separate behavior-
change decision and patch; otherwise preserve the baseline. Do not add blanket
`xfail` or weaken an assertion to get parity.

## Tests-first plan

1. Add tests that call the current transform using real Koza contexts and
   `JSONLWriter` with `tmp_path`. No `MockKozaTransform`, mocked writer, patched
   constructors, or stubbed mapping functions. Retain model round-trip tests as
   supplementary checks.
2. Inventory all eleven Type labels and the union of Action literals from both
   branch guards and branch bodies, including actions only present in fallback
   branches. Explicitly enumerate expected cases independently of production tables.
3. Parameterize those combinations across `Endogenous` values `"TRUE"`, `"FALSE"`,
   `""`, `None`, booleans, case variants, and actual preparation-produced missing
   values. At minimum, all named branches and type-specific defaults must be covered
   for both endogenous direction pairs.
4. Assert node classes/IDs/names, association classes, endpoint order, predicates,
   all qualifier fields and omissions, provenance, publications, edge cardinality,
   and distinct generated edge IDs. Test `None`, `"None"`, `"Unknown"`, empty,
   unrecognized, whitespace, and case-variant labels separately.
5. Test publication parsing with absent, empty, repeated, whitespace-containing,
   and empty-token values. Test missing required keys and malformed inputs on both
   emitted and filtered routes to protect access/validation order. Preserve exception
   classes and stable details such as missing-key names; do not assert tracebacks or
   generated UUIDs embedded in validation diagnostics.
6. Test mixed batches, repeated records, empty iterators, all-filtered iterators,
   and consecutive records with different mapping/evidence outcomes to detect leakage.
7. Check in tiny `ligands.csv` and interaction fixtures. Exercise real preparation
   followed by the real transform, including evidence aggregation, missing grouping
   values, duplicate mappings, and unmapped ligands. No live downloads are needed.
8. Run new characterization tests against the untouched baseline before editing
   production behavior. Then implement incrementally, keeping them green.

Also run a development-only differential comparison against the pinned baseline:
compare complete node/edge serialization and model classes, excluding only generated
edge UUID values. Preserve list order and duplicate multiplicity. Include the full
Type/Action matrix, evidence variants, missing values, and exceptional outcomes.
Do not leave permanent tests dependent on a moving `HEAD`, an external temporary
file, or expected values computed from the new production mapping tables. Reviewed
literal expectations or checked-in output fixtures must remain useful in CI.

## Implementation sequence and acceptance

Deliver in reviewable steps: characterization tests; mapping/context helpers;
single-record assembly and consolidated construction; thin batch wrapper; final
cleanup and parity verification. If a scientific or failure-policy change is
desired, separate it from these steps.

Acceptance criteria:

- Every named mapping branch, explicit skip, type-specific fallback, and known
  exceptional path is characterized before its implementation changes.
- Permanent tests and original-versus-refactored differential checks pass without
  suppressing differences or weakening conditions.
- Batch shape, ordering, duplicate behavior, node identity, per-edge qualifier
  placement, publications, and failure behavior match the baseline.
- Mapping rules live in source-local data/resolvers; the batch wrapper contains
  orchestration only, and edge constructors are not repeated for each Action.
- New helpers have type hints and docstrings; simple pure mappings have doctests.
- No GtoPdb/SIGNOR shared framework, SIGNOR edits, dependency updates, or unrelated
  ingest changes are introduced.
- Run the focused GtoPdb tests/doctests, the entire ingest unit suite, formatting,
  lint, spelling, and whitespace checks. Report any pre-existing failures or skips.

Run from the repository root using `uv`:

```sh
uv run pytest tests/unit/ingests/gtopdb --doctest-modules src/translator_ingest/ingests/gtopdb/gtopdb.py -q
uv run pytest tests/unit/ingests -q
uv run black --check src/translator_ingest/ingests/gtopdb/gtopdb.py tests/unit/ingests/gtopdb
uv run ruff check src/translator_ingest/ingests/gtopdb/gtopdb.py tests/unit/ingests/gtopdb
uv run codespell src/translator_ingest/ingests/gtopdb tests/unit/ingests/gtopdb --ignore-words=.codespellignore
git diff --check
```

If mapping helpers move into another module, include it in doctest collection and
format/lint checks. Differential checks are an additional development verification,
not a replacement for the permanent suite.
