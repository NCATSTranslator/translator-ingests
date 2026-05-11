# Pharos Target Development Level (TDL) Data Dump

This directory contains a tabular export of **Pharos Target Development Level (TDL)** annotations.
Each row represents a **single protein target**, integrating identifiers, annotations, and quantitative evidence used to assign TDL classifications.

Pharos is a publicly available, target-centric knowledgebase originally developed under the NIH Common Fund’s *Illuminating the Druggable Genome (IDG)* program and is now maintained by NCATS.
***NOTE*** Pharos 2.0 expands target coverage by incorporating isoform-aware UniProt mappings, increasing the protein space from 20,412 canonical UniProt targets in the original Pharos release to 227,210 human UniProt proteins (reviewed and unreviewed), enabling more comprehensive representation of alternative splicing and protein diversity.
---

## File Overview

**TDL target dump (TSV/CSV)**
Provides harmonized target identifiers, UniProt annotations, IDG family assignments, and evidence counts used in TDL computation.

---

## Target Development Levels (TDLs)

Each target is assigned one of the following categories:

* **Tclin** – Targets of approved drugs with known mechanism of action
* **Tchem** – Targets with small-molecule ligands meeting defined activity thresholds
* **Tbio** – Targets with substantial biological annotation but no known drug or qualifying chemical ligand
* **Tdark** – Targets with limited functional annotation or experimental characterization

TDLs are computed using a **graph-based backend** that integrates multiple biomedical knowledge sources and applies standardized evidence thresholds.

---

## Data Dictionary

### Identifier & Naming Fields

| Column         | Description                                                                            |
| -------------- | -------------------------------------------------------------------------------------- |
| **id**         | Stable internal Pharos target identifier used in the graph backend.                    |
| **uniprot_id** | UniProt accession for the target protein.                                              |
| **symbol**     | Approved HGNC gene symbol, when available.                                             |
| **name**       | Recommended protein name from UniProt.                                                 |
| **xref**       | Pipe-delimited cross-references (e.g., UniProt, RefSeq, Ensembl, Pharos internal IDs). |
| **ncbi_id**    | NCBI Gene identifier associated with the target.                                       |
| **ensembl_id** | Ensembl gene identifier associated with the target.                                    |

---

### UniProt Annotation Fields

| Column                      | Description                                                                                             |
| --------------------------- | ------------------------------------------------------------------------------------------------------- |
| **uniprot_reviewed**        | Indicates whether the UniProt entry is Swiss-Prot reviewed (`TRUE`) or TrEMBL unreviewed (`FALSE`).     |
| **uniprot_annotationScore** | UniProt annotation score (1–5) reflecting curation depth and biological knowledge.                      |
| **uniprot_function**        | Functional description of the protein from UniProt.                                                     |
| **uniprot_canonical**       | UniProt canonical protein sequence identifier used by Pharos.                                           |
| **uniprot_isoform**         | UniProt isoform identifier(s) associated with the target, when applicable (pipe-delimited if multiple). |

---

### IDG Classification

| Column         | Description                                                                                   |
| -------------- | --------------------------------------------------------------------------------------------- |
| **tdl**        | Target Development Level classification: `Tclin`, `Tchem`, `Tbio`, or `Tdark`.                |
| **idg_family** | IDG protein family classification (e.g., GPCR, Kinase, Ion Channel, Nuclear Receptor, Other). |

---

### Quantitative Evidence Fields (TDL Inputs)

These fields capture evidence used during TDL computation.

| Column                 | Description                                                                        |
| ---------------------- | ---------------------------------------------------------------------------------- |
| **tdl_ligand_count**   | Number of ligands with measured bioactivity associated with the target.            |
| **tdl_drug_count**     | Number of approved or investigational drugs targeting the protein.                 |
| **tdl_go_term_count**  | Number of Gene Ontology (GO) **leaf-node** annotations associated with the target. |
| **tdl_generif_count**  | Number of GeneRIF annotations linked to the target gene.                           |
| **tdl_pm_score**       | PubMed-based publication score reflecting literature support.                      |
| **tdl_antibody_count** | Number of antibodies reported to target the protein.                               |

---

## Notes & Caveats

* TDL assignments and evidence counts may change as underlying source databases are updated.
* Canonical protein selection follows UniProt conventions; isoform information is retained where relevant.
* This dataset is intended for **analysis, benchmarking, and reproducibility**, not as a static reference.
