## GOA (Gene Ontology Annotations) Ingest — Design and Rationale

This document explains the modeling choices and rationale implemented in the GOA ingest.

## Best of Breed Implementation

This ingest is a "best of breed" implementation, inspired by and combining ideas from several existing GOA ingest implementations:

- **[Monarch GOA Ingest](https://github.com/monarch-initiative/go-ingest/tree/afc03f3331642d83989f07eef06e1b2c483e7118/src/go_ingest)**
- **[Orion GOA Parser](https://github.com/RobokopU24/ORION/tree/e4d4eb54b4cffb19bd06b0f83558515f752e3b28/parsers/GOA/src)**
- **[RTX-KG2 GOA Ingest](https://github.com/RTXteam/RTX-KG2/blob/master/convert/go_gpa_to_kg_jsonl.py)**

### Run GOA Ingest
```bash
make transform SOURCE_ID=goa
```
- KGX files `goa_nodes.jsonl` and `goa_edges.jsonl` will be generated in the root `/data` directory

### Scope
- Processes GOA GAF (2.2) files for human and mouse.
- Includes both manually curated and electronically inferred annotations.
- No record-level filtering beyond species; negated annotations are retained and represented explicitly.

### Inputs
- Human: `goa_human.gaf.gz`
- Mouse: `mgi.gaf.gz`
- Rat: `rgd.gaf.gz`
- Mechanism: file download
- Format: tab-delimited GAF with 17 columns

### Entity Typing (DB-driven)
The database source determines the entity type to preserve semantic accuracy:
- UniProtKB → Protein
- MGI, SGD, RGD, ZFIN, FB, WB, TAIR → Gene
- ComplexPortal → MacromolecularComplex
- RNAcentral → RNAProduct

Rationale: GOA aggregates from multiple providers with distinct identifier semantics; typing subjects by their source maintains fidelity without post‑hoc inference.

### GO Term Typing (Aspect-driven)
GO aspect is mapped to the most specific Biolink class:
- P → BiologicalProcess
- F → MolecularActivity
- C → CellularComponent

Rationale: Using specific GO-domain classes improves downstream reasoning and alignment with Biolink’s semantic hierarchy.

### Predicate Selection
1. Prefer qualifier-based predicates when present, covering standard and upstream-effect relations (e.g., enables, contributes_to, participates_in, located_in, is_active_in, colocalizes_with, acts_upstream_of, acts_upstream_of_or_within, and their positive/negative effect variants).
2. If a qualifier is absent or unrecognized, fall back to aspect-based defaults:
   - P → participates_in
   - F → enables
   - C → located_in

Rationale: Qualifiers encode the intended semantic relationship; aspect fallback ensures broad coverage when qualifiers are missing.

### Negation Handling
- GOA encodes negation via the NOT qualifier. Negated assertions are included and represented via an explicit `negated` flag on the association.
- No pre-filtering of NOT annotations is performed at the configuration level.

Rationale: Retaining negation preserves essential scientific context and avoids false-positive assertions.

### Publications and Evidence
- Publications: only valid PMIDs are retained. Items that are not PMIDs (e.g., GO_REF, ISBN, database pages) are excluded. Bare numeric PMIDs are normalized to the PMID CURIE form.
- Evidence codes are mapped to knowledge metadata:
  - Experimental and high-confidence (e.g., EXP, IDA, IPI, IMP, IGI, IEP, HTP, HDA, HMP, HGI, HEP): knowledge_assertion + manual_agent
  - Phylogenetic/computational groups (e.g., IBA, IBD, IRD, ISS, ISO, ISA, ISM, IGC, RCA): typically prediction + manual_agent
  - Electronic (IEA): prediction + automated_agent
  - No data (ND): not_provided + not_provided
  - Unknown/other: defaults to not_provided

Rationale: Prioritizing PMIDs improves citation quality; mapping evidence to knowledge level/agent type supports trust and ranking in Translator.

### Association Types
- Gene subjects use the specific `GeneToGoTermAssociation`.
- Protein, MacromolecularComplex, and RNAProduct subjects use the generic `Association` (until subject-specific association classes become available).

Rationale: Prefer specific association classes where defined by Biolink; otherwise use the generic form to remain valid and extensible.

### Taxon Modeling
- Taxon information is recorded on subject nodes (`in_taxon`) using `NCBITaxon:` CURIEs.
- Taxon is not set on associations.

Rationale: The specific gene→GO association class does not carry the taxon mixin; node-level modeling preserves species context without violating Biolink constraints.

### Provenance
- Primary knowledge source: `infores:goa`
- Aggregator knowledge source: `infores:biolink`

Note: The RIG lists a target infores identifier for produced files (`translator-goa-kgx`). If required, aggregator labeling can be updated to reflect that downstream packaging choice.

### Operational Characteristics
- Record-by-record transformation for memory efficiency and fault isolation.
- Deterministic identifier normalization: DB_Object_IDs are CURIE-formed with DB prefix when missing; GO IDs are used directly; taxon identifiers are normalized to `NCBITaxon:`.

### Outputs
- Nodes file: `goa_nodes.jsonl`
- Edges file: `goa_edges.jsonl`

### Alignment with the RIG
- Species scope, predicate repertoire (including upstream-effect family), negation, evidence/knowledge mapping, and GO-domain typing are aligned with the RIG.
- Minor variance: aggregator infores naming may differ depending on deployment; see Provenance.

### Future Considerations
- Extend to additional taxa and/or GPAD/GPI inputs for richer qualifier context.
- Consider inclusion of `supporting_data_provider` where GO-CAM-derived annotations are detected.
- Revisit predicate selection if Biolink evolves recommended mappings for GO qualifiers.

### Ingest Contributors (o)
- **Adilbek Bazarkulov**: code author
- **Evan Morris**: code support
- **Adilbek Bazarkulov**: code support, domain expertise
- **Sierra Moxon**: data modeling, domain expertise
- **Matthew Brush**: data modeling, domain expertise

