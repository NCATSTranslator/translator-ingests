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

GOA files, with embedded release dates in headers. As for 2025-06-01. can be downloaded from (Catalog): https://current.geneontology.org/products/pages/downloads.html

Complete annotation directory all files (Bulk download): https://current.geneontology.org/annotations/

Human GAF (Bulk download):
https://current.geneontology.org/annotations/goa_human.gaf.gz (993,520 annotations) As for 2025-06-01 release.

- Key files:

    - goa_human.gaf.gz (human GOA GAF 2.2)

    - goa_uniprot_all.gaf.gz (multi-species GOA GAF 2.2)

### Provision Mechanisms and Formats

- **Mechanism(s):** File download or API access  
- **Formats:**  
  - **GAF 2.2** (`.gaf`, `.gaf.gz`): a tab-delimited, 17-column format (essentially a TSV). Header comment lines start with `!`. Each row corresponds to one annotation. Columns are:  
    - `DB`  
    - `DB Object ID`  
    - `DB Object Symbol`  
    - `Qualifier`  
    - `GO ID`  
    - `DB:Reference(s)`  
    - `Evidence Code`  
    - `With (or) From`  
    - `Aspect`  
    - `DB Object Name`  
    - `DB Object Synonym`  
    - `DB Object Type`  
    - `Taxon`  
    - `Date`  
    - `Assigned By`  
    - `Annotation Extension`  
    - `Gene Product Form ID`  
  - **GPAD 2.0** (`.gpad`, `.gpad.gz`)  
  - **GPI 2.0** (`.gpi`, `.gpi.gz`) 

API access instructions: https://geneontology.org/docs/tools-guide/

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

#### Included Content

| File               | Included Content                                                     | Columns Used                                                                                                                                                                                                                                                   |
| ------------------ | -------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `goa_human.gaf.gz` | All human annotations with evidence and provenance                  | `DB Object ID` -> `GeneProduct.id` (target type `biolink:GeneProduct`)<br>`DB Object Symbol` -> `symbol`<br>`GO ID` -> `OntologyClass.id` (target type `biolink:OntologyClass`)<br>`DB:Reference(s)` -> `publications`<br>`Evidence Code` -> `biolink:has_evidence`<br>`With/From` -> `biolink:related_to`<br>`Aspect` -> `biolink:predicate` (`participates_in`/`enables`/`located_in`)<br>`Date` -> `creation_date`<br>`Assigned By` -> `biolink:provided_by` |

                                                                                                                                                                                      

#### Excluded Content


| File(s)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | Excluded Content                                                                                                                                                                     | Rationale                                                                                                                                                    |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `goa_human.gaf.gz`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Header comment lines (`!gaf-version`, `!generated-by`, etc.);<br> `DB`; <br>`DB Object Name`;<br> `DB Object Synonym`;<br> `DB Object Type`;<br> `Taxon`;<br> `Annotation Extension`;<br> `Gene Product Form ID` | File-level metadata and non-core fields outside the subject–predicate–object model; handled via Biolink provenance or qualifiers.                            |
                                                                                                                                                                                                                                                                                                    

### Future Considerations

| File                     | Content                                                | Rationale                                                                          |
| ------------------------ | ------------------------------------------------------ | ---------------------------------------------------------------------------------- |
| `goa_human.gpad.gz`      | Detailed qualifiers/annotation extensions               | Enables modeling of complex contextual qualifiers                                  |
| `goa_human.gpi.gz`       | Gene product metadata                                  | Enriches node attributes (names, synonyms, types)                                  |
| `goa_uniprot_all.gaf.gz` | Multi-species annotations                              | Broadens taxonomic coverage                                                         |


---

## Target Information

### Infores

* **`infores:goa`**

### Edge Types

| # | Association Type                             | Biolink MetaEdge                   | Qualifier Types          | Evidence & Provenance                                         | UI Explanation                                                                                        |
| - | -------------------------------------------- | ---------------------------------- | ------------------------ | -------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| 1 | GeneProduct -> BiologicalProcess             | `biolink:participates_in`          | `biolink:negated`        | manual/electronic, `biolink:has_evidence`, `biolink:provided_by` | Captures that a gene product **participates in** a biological process (GO Aspect P).                    |
| 2 | GeneProduct -> MolecularFunction             | `biolink:enables`                  | `biolink:negated`        | manual/electronic, `biolink:has_evidence`, `biolink:provided_by` | Indicates a gene product **enables** a molecular function (GO Aspect F).                               |
| 3 | GeneProduct -> CellularComponent             | `biolink:located_in`               | `biolink:negated`        | manual/electronic, `biolink:has_evidence`, `biolink:provided_by` | Specifies a gene product is **located in** a cellular component (GO Aspect C).                          |

**Rationale**
1. **Evidence provenance:** We preserve GOA’s manual (IDA, IMP, etc.) vs. electronic (IEA) evidence codes by populating `biolink:has_evidence` and `biolink:provided_by` on each edge, enabling users to filter by confidence level.

2. **Negation:** GO “NOT” annotations are captured via the standard `biolink:negated` qualifier, ensuring negative assertions remain computable.

3. **Predicate semantics:**
   - **`participates_in`:** maps Aspect P (process involvement) of a continuant gene product.
   - **`enables`:** reflects the mechanistic capacity for Aspect F.
   - **`located_in`:** denotes a material entity’s location for Aspect C.

4. **OntologyClass consolidation:** All GO terms map to `biolink:OntologyClass`, simplifying node types and ensuring uniform treatment with other ontology-based sources.

5. **Qualifiers from “With/From”:** Contextual details (e.g. cofactors) are preserved via the `biolink:related_to` qualifier, future-proofing richer edge modeling.

## Node Types

| Biolink Category    | Source Identifier Type(s) | Notes                                      |
| ------------------- | ------------------------- | ------------------------------------------ |
| GeneProduct         | UniProtKB accession       | Represents proteins and gene products.     |
| OntologyClass       | GO term                   | Represents GO terms across all aspects.    |

---

## Ingest Contributors

* **Adilbek Bazarkulov**
