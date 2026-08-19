# Guide to Pharmacology (GtoPdb) ingest design decisions

This document records the current ingest boundary and modelling decisions. It
is intentionally specific about what the source asserts, what the current
Biolink projection can represent, and what remains deferred.

## Interaction semantics

GtoPdb provides two source dimensions for an interaction: `Type` and `Action`.
The ingest represents their finite combination matrix as a direct nested Python
dictionary in `rules.py`:

```python
RULES[source_type][source_action] -> InteractionRule
```

Shared immutable rules (for example `AGONISM`, `ANTAGONISM`, and `INHIBITION`)
make repeated semantics visible. `dataclasses.replace()` records only a local
semantic difference. The rule table selects source semantics; the transform
separately projects endogenous interactions to `biolink:regulates` and
non-endogenous interactions to `biolink:affects`.

Known source pairs that should not generate graph assertions use explicit
`SKIP` rules. Unknown pairs remain distinct from explicit skips. The historical
broad fallbacks for unlisted `Activator` and `Inhibitor` actions are retained
for compatibility and are tested; changing them is a separate source-policy
decision.

## Target identity and composite targets

A GtoPdb target is a source-level entity, identified by its GtoPdb Target ID.
It is not necessarily a single UniProt protein. The source can provide
pipe-delimited subunit IDs, gene symbols, or UniProt accessions, for example:

```text
Target ID:          378
Target UniProt ID:  P46098|O95264
```

The list contains two component identifiers. It must **not** be serialized as
one CURIE such as:

```text
UniProtKB:P46098|O95264
```

That string is not a valid UniProt CURIE and does not identify either the GtoPdb
target or an individual protein.

### Current policy

The ingest preserves GtoPdb target metadata through preparation and detects
composite targets before graph construction. It emits valid single-protein
target records normally. It explicitly excludes unsupported composite target
records and writes one aggregate warning, rather than emitting malformed CURIEs
or silently relying on downstream normalization to discard them.

This is a deliberate integrity boundary, not a claim that composites lack
biological meaning. It prevents a target-level pharmacological assertion from
being incorrectly duplicated as an independent assertion about every component.

## Future composite-target modelling

The intended direction is a source-defined target node plus explicit component
relationships:

```text
GtoPdb source target
  ├─ source identity: GtoPdb Target ID
  ├─ category: protein, complex, family, or unresolved group
  └─ component relationships: evidence-dependent `has_part` or `has_member`
```

The GtoPdb interaction should attach to that target node. It should not be
split into ligand-to-component pharmacology edges unless GtoPdb itself asserts
those component-level interactions.

### Considering an external complex mapping

The executable [complex-target mapping EDA](gtopdb_complex_target_eda.ipynb)
quantifies the current release's composite-target scope, compares policy
consequences, and generates a candidate-mapping curation worklist.

An external complex resource can be useful to **validate or enrich** a GtoPdb
composite target, including finding a stable complex CURIE. It must not replace
the source target identity by assumption.

Before accepting an external mapping, require at least:

1. an auditable cross-reference or reproducible match from GtoPdb Target ID,
   name, species, and component set to the external complex;
2. compatible species/taxon and component semantics;
3. an explicit resolution policy for one-to-many, many-to-one, and no-match
   cases;
4. provenance on the mapping and membership edges; and
5. a decision about whether the external identifier is an exact equivalence,
   a database cross-reference, or merely supporting evidence.

A conservative first implementation should retain a GtoPdb-derived target
CURIE as the primary graph identity, attach the external complex identifier as
a cross-reference only after validation, and emit component edges only when the
source/external evidence supports their intended Biolink meaning. Directly
substituting a third-party complex CURIE risks conflation: a GtoPdb target may
represent a family, an alternative subunit composition, a species-specific
realization, or another source grouping that does not exactly equal the external
complex record.

## Regression boundary

The ingest has source-derived characterization coverage for all observed
GtoPdb 2026.2 `Type`/`Action` pairs under both endogenous modes. Behavioural
refactors must preserve that coverage, full-source counts, and the invariant
that no node identifier contains a pipe-delimited UniProt value.
