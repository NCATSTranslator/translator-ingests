# Gene Ontology Annotation (GOA) Reference Ingest Guide (RIG)

---

## Source Information


### Infores

* **`infores:goa`**

### Description

The Gene Ontology Annotation (GOA) project provides high-quality, manually curated associations between gene products and GO terms—covering molecular functions, biological processes, and cellular components—with evidence codes and literature references.

### Terms of Use

Gene Ontology’s annotations are made available under the Creative Commons Attribution 4.0 International License (CC BY 4.0). See: https://geneontology.org/docs/go-citation-policy/

CC BY 4.0: https://creativecommons.org/licenses/by/4.0/

### Data Access Locations

GOA files, with embedded release dates in headers. As for 2025-06-01. can be downloaded from: https://current.geneontology.org/products/pages/downloads.html

Human GAF (all, filtered):
https://current.geneontology.org/annotations/goa_human.gaf.gz (993,520 annotations) As for 2025-06-01 release.

Complete annotation directory:
https://current.geneontology.org/annotations/

- Key files:

    - goa_human.gaf.gz (human GOA GAF 2.2)

    - goa_uniprot_all.gaf.gz (multi-species GOA GAF 2.2)

(Full listing available at: https://current.geneontology.org/annotations/)

### Provision Mechanisms and Formats

* **Mechanism(s):** File download or API access
* **Formats:** 
    - Downloadable files:
        - The Gene Association File **GAF 2.2** (.gaf, .gaf.gz)
        - The Gene Product Association Data **GPAD 2.0** (.gpad, .gpad.gz)
        - The Gene Product Information **GPI 2.0** (.gpi, .gpi.gz)
* **API access instructions: https://geneontology.org/docs/tools-guide/**

### Releases and Versioning

* **Release cadence:**
    - GOA files are released approximately every four weeks (monthly), coordinated with UniProtKB releases 
* **Versioning:**
    - Each GAF file header contains a !Generated: YYYY-MM-DD line indicating the precise build date
* **Release Notes**: Detailed release information is available at https://geneontology.org/docs/download-go-annotations/ and changes for GAF files https://geneontology.org/docs/go-annotation-file-gaf-format-2.2/

---

## Ingest Information

### Utility
- GOA Human provides the definitive, manually curated and electronically inferred associations between human gene products (UniProtKB entries) and Gene Ontology terms, ensuring comprehensive coverage of molecular functions, biological processes, and cellular components.

### Scope

- Primary ingest: the human GOA GAF 2.2 file (goa_human.gaf.gz), capturing all three annotation aspects (P, F, C) and both manual (e.g. IDA, IMP) and electronic (IEA) evidence codes.

- Excluded: GPAD (.gpad) and GPI (.gpi) formats, as well as multi‐species GAF (goa_uniprot_all.gaf.gz)—the latter is earmarked for future ingestion if broader taxonomic coverage is needed.

#### Relevant Files

| File                     | Description                                                 |
| ------------------------ | ----------------------------------------------------------- |
| `goa_human.gaf.gz`       | Human gene-product to GO term associations (GAF 2.2)        |
| `goa_uniprot_all.gaf.gz` | Multi-species GOA associations (GAF 2.2; not primary focus) |

#### Included Content

| File                     | Included Content                                                                                | Columns Used                                                                                                                                                                                                                                                                                                                 |
| ------------------------ | ----------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `goa_human.gaf.gz`       | Human gene-product -> GO term associations with evidence and provenance                         | `DB Object ID` -> `GeneProduct.id`; `DB Object Symbol` -> `symbol`; `GO ID` -> `OntologyClass.id`; `DB:Reference(s)` -> `publications`; `Evidence Code` -> `evidence_code`; `With (or) From` -> `related_to`; `Aspect` -> `participates_in`/`enables`/`occurs_in`; `Date` -> `creation_date`; `Assigned By` -> `provided_by` |
| `goa_uniprot_all.gaf.gz` | Multi-species gene-product -> GO term associations with evidence and provenance (future ingest) | Same as `goa_human.gaf.gz`                                                                                                                                                                                                                                                                                                   |
                                                                                                                                                                                                                                                                                                                |

#### Excluded Content

| File                     | Excluded Content                                                                                                                                                                     | Rationale                                                                                                                         |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| `goa_human.gaf.gz`       | Header comment lines (`!gaf-version`, `!generated-by`, etc.); `DB`; `DB Object Name`; `DB Object Synonym`; `DB Object Type`; `Taxon`; `Annotation Extension`; `Gene Product Form ID` | File-level metadata and non-core fields outside the subject–predicate–object model; handled via Biolink provenance or qualifiers. |
| `goa_uniprot_all.gaf.gz` | Header comment lines; `DB`; `DB Object Name`; `DB Object Synonym`; `DB Object Type`; `Taxon`; `Annotation Extension`; `Gene Product Form ID`                                         | Same exclusions apply for the multi-species file when it’s ingested in the future.                                                |

#### Future Considerations

| File                     | Content                                          | Rationale                                                                                       |
| ------------------------ | ------------------------------------------------ | ----------------------------------------------------------------------------------------------- |
| `goa_human.gpad.gz`      | GPAD 1.2 associations with annotation extensions | Might enable modeling of complex qualifiers and annotation extensions not captured in GAF.           |
| `goa_human.gpi.gz`       | GPI gene product index entries                   | Provides enriched gene product metadata (names, synonyms, types) might be used node normalization. |
| `goa_uniprot_all.gaf.gz` | Multi-species GOA GAF                            | Broadens coverage beyond human, supporting cross-species analyses in later ingests.             |

---

## Target Information

### Infores

* **`infores:goa`**

### Edge Types

| # | Association Type                             | Biolink MetaEdge | Qualifier Types | AT / KL                                 | UI Explanation                                                                    |
| - | -------------------------------------------- | ---------------- | --------------- | --------------------------------------- | --------------------------------------------------------------------------------- |
| 1 | GeneProduct -> BiologicalProcess association | participates\_in | negated         | manual/electronic, knowledge\_assertion | Captures that a gene product participates in the specified biological process.    |
| 2 | GeneProduct -> MolecularFunction association | enables          | negated         | manual/electronic, knowledge\_assertion | Indicates that a gene product enables the specified molecular function.           |
| 3 | GeneProduct -> CellularComponent association | located\_in      | negated         | manual/electronic, knowledge\_assertion | Specifies that a gene product is located within the specified cellular component. |

**Rationale**

1. GO Aspect “P” (Biological Process) -> participates_in maps a continuant (gene product) to the process it participates in.

2. GO Aspect “F” (Molecular Function) -> enables reflects a physical entity enabling a function.

3. GO Aspect “C” (Cellular Component) -> located_in denotes the location of a material entity.

### Node Types

| Biolink Category | Source Identifier Type(s) | Notes                                   |
| ---------------- | ------------------------- | --------------------------------------- |
| GeneProduct      | UniProtKB accession       | Represents proteins and gene products.  |
| OntologyClass    | GO term                   | Represents GO terms across all aspects. |

---

## Ingest Contributors

* **Adilbek Bazarkulov**
