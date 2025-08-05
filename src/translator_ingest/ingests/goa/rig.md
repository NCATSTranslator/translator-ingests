# Gene Ontology Annotation (GOA) Reference Ingest Guide (RIG)

---

## Source Information

### Infores

* **`infores:goa`**

### Description

The Gene Ontology Annotation (GOA) project provides high-quality, manually curated associations between gene products and GO terms—covering molecular functions, biological processes, and cellular components—with evidence codes and literature references.

### Terms of Use

Gene Ontology's annotations are made available under the Creative Commons Attribution 4.0 International License (CC BY 4.0). See: [https://geneontology.org/docs/go-citation-policy/](https://geneontology.org/docs/go-citation-policy/)

CC BY 4.0: [https://creativecommons.org/licenses/by/4.0/](https://creativecommons.org/licenses/by/4.0/)

### Data Access Locations

GOA files, with embedded release dates in headers (e.g. `!Generated: 2025-06-01`), can be downloaded from the Gene Ontology FTP or HTTP release site:

* **Catalog page:** [https://current.geneontology.org/products/pages/downloads.html](https://current.geneontology.org/products/pages/downloads.html)
* **Bulk annotation directory:** [https://current.geneontology.org/annotations/](https://current.geneontology.org/annotations/)
* **Human GAF (2025-06-01 release):**

  * `goa_human.gaf.gz` (993,520 annotations)
* **Mouse GAF:**

  * `mgi.gaf` (mouse GOA GAF 2.2)

### Provision Mechanisms and Formats

* **Mechanism:** File download (GAF, GPAD, GPI) or API access (REST endpoints)
* **Formats:**

  * **GAF 2.2** (`.gaf`, `.gaf.gz`): Tab-delimited 17-column format. Header lines start with `!`.
  * **GPAD 2.0** (`.gpad`, `.gpad.gz`)
  * **GPI 2.0** (`.gpi`, `.gpi.gz`)

### Releases and Versioning

* **Release cadence:** Approximately every four weeks, synchronized with UniProtKB.
* **Versioning:** Each GAF header includes a `!Generated: YYYY-MM-DD` line.
* **Release notes:** [https://geneontology.org/docs/download-go-annotations/](https://geneontology.org/docs/download-go-annotations/) and [https://geneontology.org/docs/go-annotation-file-gaf-format-2.2/](https://geneontology.org/docs/go-annotation-file-gaf-format-2.2/)

---

## Ingest Information

### Utility

GOA provides the definitive manually curated and electronically inferred associations between human and mouse gene products (UniProtKB entries and MGI identifiers) and GO terms, with evidence codes and literature references.

### Scope

* **Primary ingest:** human (`goa_human.gaf.gz`) and mouse (`mgi.gaf`) GOA GAF 2.2 files, capturing all three annotation aspects (P, F, C) with manual (e.g., IDA, IMP) and electronic (IEA) evidence codes, including the `Taxon` column to differentiate species via the `in_taxon` slot.
* **Excluded:** GPAD and GPI formats; multi-species GAF (`goa_uniprot_all.gaf.gz`) earmarked for future broader taxonomic coverage.

#### Relevant Files

| File               | Description                                          |
| ------------------ | ---------------------------------------------------- |
| `goa_human.gaf.gz` | Human gene-product to GO term associations (GAF 2.2) |
| `mgi.gaf`          | Mouse gene-product to GO term associations (GAF 2.2) |

#### Included Content

| File                           | Included Content                                             | Columns Mapped (->)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ------------------------------ | ------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `goa_human.gaf.gz` / `mgi.gaf` | All human and mouse annotations with evidence and provenance | `DB` -> determines biolink class (Gene/Protein/Complex/RNA)<br>`DB Object ID` -> `Entity.id` (dynamic biolink class)<br>`DB Object Symbol` -> `name`<br>`DB Object Name` -> `description`<br>`Qualifier` -> maps to `negated` slot (e.g., 'NOT' sets `negated=true`)<br>`GO ID` -> `GO_Term.id` (dynamic biolink class based on aspect)<br>`DB:Reference(s)` -> `publications`<br>`Evidence Code` -> `has_evidence`<br>`Aspect` -> maps to `predicate` slot: `biolink:participates_in` / `biolink:enables` / `biolink:located_in`<br>`Taxon` -> `in_taxon`<br>`Annotation Extension` -> qualifier slots if needed<br>`Gene Product Form ID` -> form/variant qualifiers (future) |

#### Excluded Content

| File      | Excluded Content                                                                                                                                                               | Rationale                                                                                    |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------- |
| GAF files | Header comment lines (`!gaf-version`, `!generated-by`, etc.);<br>`DB Object Synonym`;<br>`DB Object Type`;<br>`With (or) From`;<br>`Assigned By`;<br>`Date` | Non-core provenance or redundant context; optional context handled via qualifiers if needed. |

#### Future Considerations

| File                     | Content                   | Rationale                                  |
| ------------------------ | ------------------------- | ------------------------------------------ |
| `goa_human.gpad.gz`      | Detailed qualifiers       | Enables modeling of annotation extensions  |
| `goa_human.gpi.gz`       | Gene product metadata     | Enriches node attributes (names, synonyms) |
| `goa_uniprot_all.gaf.gz` | Multi-species annotations | Broadens taxonomic coverage                |

---

## Target Information

### Edge Types

| # | Association                      | Biolink MetaEdge          | Slots                                                 | UI Explanation                                                                   |
| - | -------------------------------- | ------------------------- | ----------------------------------------------------- | -------------------------------------------------------------------------------- |
| 1 | Gene -> BiologicalProcess | biolink\:participates\_in | `negated`, `has_evidence`, `publications`, `knowledge_level`, `agent_type` | Captures that a gene **participates in** a biological process (Aspect P) |
| 2 | Gene -> MolecularFunction | biolink\:enables          | `negated`, `has_evidence`, `publications`, `knowledge_level`, `agent_type` | Indicates a gene **enables** a molecular function (Aspect F)             |
| 3 | Gene -> CellularComponent | biolink\:located\_in      | `negated`, `has_evidence`, `publications`, `knowledge_level`, `agent_type` | Specifies a gene is **located in** a cellular component (Aspect C)       |
| 4 | Protein -> BiologicalProcess | biolink\:participates\_in | `negated`, `has_evidence`, `publications`, `knowledge_level`, `agent_type` | Captures that a protein **participates in** a biological process (Aspect P) |
| 5 | Protein -> MolecularFunction | biolink\:enables          | `negated`, `has_evidence`, `publications`, `knowledge_level`, `agent_type` | Indicates a protein **enables** a molecular function (Aspect F)             |
| 6 | Protein -> CellularComponent | biolink\:located\_in      | `negated`, `has_evidence`, `publications`, `knowledge_level`, `agent_type` | Specifies a protein is **located in** a cellular component (Aspect C)       |
| 7 | MacromolecularComplex -> BiologicalProcess | biolink\:participates\_in | `negated`, `has_evidence`, `publications`, `knowledge_level`, `agent_type` | Captures that a complex **participates in** a biological process (Aspect P) |
| 8 | MacromolecularComplex -> MolecularFunction | biolink\:enables          | `negated`, `has_evidence`, `publications`, `knowledge_level`, `agent_type` | Indicates a complex **enables** a molecular function (Aspect F)             |
| 9 | MacromolecularComplex -> CellularComponent | biolink\:located\_in      | `negated`, `has_evidence`, `publications`, `knowledge_level`, `agent_type` | Specifies a complex is **located in** a cellular component (Aspect C)       |
| 10 | RNAProduct -> BiologicalProcess | biolink\:participates\_in | `negated`, `has_evidence`, `publications`, `knowledge_level`, `agent_type` | Captures that an RNA product **participates in** a biological process (Aspect P) |
| 11 | RNAProduct -> MolecularFunction | biolink\:enables          | `negated`, `has_evidence`, `publications`, `knowledge_level`, `agent_type` | Indicates an RNA product **enables** a molecular function (Aspect F)             |
| 12 | RNAProduct -> CellularComponent | biolink\:located\_in      | `negated`, `has_evidence`, `publications`, `knowledge_level`, `agent_type` | Specifies an RNA product is **located in** a cellular component (Aspect C)       |

### Node Types

| Biolink Category       | Source ID Type(s)                   | Notes                                   |
| ---------------------- | ----------------------------------- | --------------------------------------- |
| biolink\:Gene          | MGI, SGD, RGD, ZFIN, FB, WB, TAIR primary IDs | Represents genes with full biolink validation. |
| biolink\:Protein       | UniProtKB accession                 | Represents proteins with full biolink validation. |
| biolink\:MacromolecularComplex | ComplexPortal IDs              | Represents protein complexes with full biolink validation. |
| biolink\:RNAProduct    | RNAcentral IDs                     | Represents RNA products with full biolink validation. |
| biolink\:BiologicalProcess | GO term IDs (Aspect P)         | Represents biological processes. Uses specific biolink class instead of generic NamedThing. |
| biolink\:MolecularActivity | GO term IDs (Aspect F)        | Represents molecular functions. Uses specific biolink class instead of generic NamedThing. |
| biolink\:CellularComponent | GO term IDs (Aspect C)       | Represents cellular components. Uses specific biolink class instead of generic NamedThing. |

---

## Implementation Notes

### Framework Integration
- **Koza Framework**: Uses `@koza.transform_record()` decorator for record-by-record processing
- **Biolink Pydantic Model**: Leverages biolink pydantic models for validation and structure
- **Dynamic Class Selection**: Uses database source to determine appropriate biolink class for entities
- **Dynamic GO Term Categorization**: Maps GO aspects to specific biolink classes for semantic precision

### Database Source Mapping
- **Dynamic Selection**: Database source determines biolink class:
  - `UniProtKB` -> `Protein`
  - `MGI`, `SGD`, `RGD`, `ZFIN`, `FB`, `WB`, `TAIR` -> `Gene`
  - `ComplexPortal` -> `MacromolecularComplex`
  - `RNAcentral` -> `RNAProduct`
- **Extensibility**: Easy to add new database sources and their corresponding biolink classes

### GO Aspect Mapping
- **Dynamic Categorization**: GO aspects map to specific biolink classes:
  - `P` (Process) -> `BiologicalProcess`
  - `F` (Function) -> `MolecularActivity`
  - `C` (Component) -> `CellularComponent`
- **Semantic Precision**: Uses most appropriate biolink class for each GO aspect

### Evidence Code Mapping
- **Hardcoded Mapping**: Uses hardcoded evidence code to knowledge level/agent type mapping for simplicity and performance
- **Biolink Enums**: Leverages `KnowledgeLevelEnum` and `AgentTypeEnum` for type safety and validation
- **Fallback Values**: Unknown evidence codes default to `not_provided` for both knowledge level and agent type
- **Evidence Formatting**: Evidence codes are formatted as ECO CURIEs (e.g., `ECO:IEA`)

### Association Selection
- **Dynamic Association**: Uses `GeneToGoTermAssociation` for Gene entities, generic `Association` for others
- **Biolink Compliance**: Uses specific association when available, falls back to generic Association
- **Extensibility**: Can easily add specific associations for Protein, MacromolecularComplex, etc. when they become available

### Qualifier Handling
- **Primary Mapping**: Uses `QUALIFIER_TO_PREDICATE` mapping for specific GO qualifiers (e.g., `part_of`, `contributes_to`, `colocalizes_with`)
- **NOT Qualifier Support**: Handles `NOT|` prefix in qualifiers by extracting base qualifier and setting `negated=true`
- **Fallback Logic**: Falls back to `ASPECT_TO_PREDICATE` mapping when qualifier is not recognized
- **Logging**: Logs when fallback predicates are used for transparency

### Entity ID Creation
- **Double Prefix Prevention**: Checks if `DB_Object_ID` already contains database prefix to prevent double prefixes (e.g., `MGI:MGI:101760`)
- **Conditional Logic**: Uses `DB_Object_ID` directly if it already has prefix, otherwise prepends database source
- **Examples**: 
  - `MGI:101757` (already has prefix) → `MGI:101757`
  - `A0A024RBG1` (no prefix) → `UniProtKB:A0A024RBG1`

### Taxon Modeling
- **Node-Level Only**: `in_taxon` is only set on entity nodes, not on associations
- **Framework Constraint**: GeneToGoTermAssociation doesn't include the 'thing with taxon' mixin in the biolink model
- **Inference**: Taxon information can be inferred from the subject node's `in_taxon` property

---

## Ingest Contributors

* **Adilbek Bazarkulov**
