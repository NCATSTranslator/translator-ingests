# Gene Ontology Annotation (GOA) Reference Ingest Guide (RIG)

---

## Source Information

### Infores

* **`infores:goa`**

### Description

The Gene Ontology Annotation (GOA) project provides high-quality, manually curated associations between gene products and GO terms—covering molecular functions, biological processes, and cellular components—with evidence codes and literature references.

### Terms of Use

Gene Ontology’s annotations are made available under the Creative Commons Attribution 4.0 International License (CC BY 4.0). See: [https://geneontology.org/docs/go-citation-policy/](https://geneontology.org/docs/go-citation-policy/)

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
| `goa_human.gaf.gz` / `mgi.gaf` | All human and mouse annotations with evidence and provenance | `DB Object ID` -> `GeneProduct.id` (biolink\:GeneProduct)<br>`DB Object Symbol` -> `symbol`<br>`Relation` -> maps to `predicate` or `negated` slots (e.g., 'NOT' sets `negated=true`; 'contributes\_to' refines `predicate`)<br>`GO ID` -> `OntologyClass.id` (biolink\:OntologyClass)<br>`DB:Reference(s)` -> `publications`<br>`Evidence Code` -> `has_evidence`<br>`Aspect` -> maps to `predicate` slot: `biolink:participates_in` / `biolink:enables` / `biolink:located_in`<br>`Date` -> `creation_date`<br>`Taxon` -> `in_taxon`<br>`Annotation Extension` -> qualifier slots if needed<br>`Gene Product Form ID` -> form/variant qualifiers (future) |

#### Excluded Content

| File      | Excluded Content                                                                                                                                                               | Rationale                                                                                    |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------- |
| GAF files | Header comment lines (`!gaf-version`, `!generated-by`, etc.);<br>`DB`;<br>`DB Object Name`;<br>`DB Object Synonym`;<br>`DB Object Type`;<br>`With (or) From`;<br>`Assigned By` | Non-core provenance or redundant context; optional context handled via qualifiers if needed. |

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
| 1 | GeneProduct -> BiologicalProcess | biolink\:participates\_in | `negated`, `has_evidence`, `publications`, `in_taxon` | Captures that a gene product **participates in** a biological process (Aspect P) |
| 2 | GeneProduct -> MolecularFunction | biolink\:enables          | `negated`, `has_evidence`, `publications`, `in_taxon` | Indicates a gene product **enables** a molecular function (Aspect F)             |
| 3 | GeneProduct -> CellularComponent | biolink\:located\_in      | `negated`, `has_evidence`, `publications`, `in_taxon` | Specifies a gene product is **located in** a cellular component (Aspect C)       |

### Node Types

| Biolink Category       | Source ID Type(s)                   | Notes                                   |
| ---------------------- | ----------------------------------- | --------------------------------------- |
| biolink\:GeneProduct   | UniProtKB accession, MGI primary ID | Represents proteins and gene products.  |
| biolink\:OntologyClass | GO term ID                          | Represents GO terms across all aspects. |

---

## Ingest Contributors

* **Adilbek Bazarkulov**
