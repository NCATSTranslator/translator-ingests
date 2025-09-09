# Human Phenotype Ontology Annotations

## Source Information

**InfoRes ID:** infores:hpo-annotations

**Description:** The [Human Phenotype Ontology (HPO)](https://hpo.jax.org/) provides a standard vocabulary of phenotypic abnormalities encountered in human disease. Each term in the HPO describes a phenotypic abnormality, such as Atrial septal defect. The HPO is currently being developed using the medical literature, Orphanet, DECIPHER, and OMIM. HPO currently contains over 18,000 terms and over 156,000 annotations to hereditary diseases. The HPO project and others have developed software for phenotype-driven differential diagnostics, genomic diagnostics, and translational research.
The Human Phenotype Ontology group curates and assembles over 115,000 HPO-related annotations ("HPOA") to hereditary diseases using the HPO ontology. Here we create Biolink associations between diseases and phenotypic features, together with their evidence, and age of onset and frequency (if known). Disease annotations here are also cross-referenced to the MONarch Disease Ontology (MONDO) (https://mondo.monarchinitiative.org/).
There are four HPOA ingests ('disease-to-phenotype' (includes capture of disease modes of inheritance, 'gene-to-phenotype' and 'gene-to-disease') that parse out records from the HPO Phenotype Annotation File (http://purl.obolibrary.org/obo/hp/hpoa/phenotype.hpoa).

**Citations:**
- https://doi.org/10.1093/nar/gkaa1043

**Terms of Use:** Bespoke 'terms of use' are described here: https://hpo.jax.org/license

**Data Access Locations:**
- https://hpo.jax.org/data/annotations

**Data Provision Mechanisms:** file_download, api_endpoint

**Data Formats:** tsv, other

**Data Versioning and Releases:** GitHub managed releases at https://github.com/obophenotype/human-phenotype-ontology/releases No consistent cadence for releases. Versioning is based on the month and year of the release

**Additional Notes:** None

## Ingest Information

**Ingest Categories:** primary_knowledge_provider

**Utility:** The HPO and associated annotations are a flagship product of the Monarch Initiative (https://monarchinitiative.org/), an NIH-supported international consortium dedicated to semantic integration of biomedical and model organism data with the ultimate goal of improving biomedical research. The human phenotype/disease/gene knowledge integration aligns well with the general mission of the Biomedical Data Translator.  As a consequence, several members of the Monarch Initiative are direct participants in the Biomedical Data Translator, with Monarch data forming one primary knowledge source contributing to Translator knowledge graphs.

**Scope:** Covers curated Disease, Phenotype and Genes relationships annotated with Human Phenotype Ontology terms.

### Relevant Files

| File Name | Location | Description |
| --- | --- | --- |
| phenotype.hpoa | https://hpo.jax.org/data/annotations | disease to HPO phenotype annotations, including inheritance information |
| genes_to_disease.txt | https://hpo.jax.org/data/annotations | gene to HPO disease annotations |
| genes_to_phenotype.txt | https://hpo.jax.org/data/annotations | gene to HPO phenotype annotations |

### Included Content

| File Name | Included Records | Fields Used |
| --- | --- | --- |
| phenotype.hpoa | Disease to Phenotype relationships (i.e., rows with 'aspect' == 'P') | database_id, qualifier, hpo_id, reference, evidence, onset, frequency, sex, aspect |
| phenotype.hpoa | Disease "Mode of Inheritance" relationships (i.e., rows with 'aspect' == 'I') represented as node properties rather than edges | database_id, qualifier, hpo_id, reference, evidence, onset, frequency, sex, aspect |
| genes_to_disease.txt | Mendelian Gene to Disease relationships (i.e., rows with 'association_type' == 'MENDELIAN') | ncbi_gene_id, gene_symbol, association_type, disease_id, source |
| genes_to_disease.txt | Polygenic Gene to Disease relationships (i.e., rows with 'association_type' == 'POLYGENIC') | ncbi_gene_id, gene_symbol, association_type, disease_id, source |
| genes_to_disease.txt | General Gene Contributions to Disease relationships (i.e., rows with 'association_type' == 'UNKNOWN') | ncbi_gene_id, gene_symbol, association_type, disease_id, source |
| genes_to_phenotype.txt | Records where we determine that the reported G-P association was inferred over a G-D associated type with the value "MENDELIAN" | ncbi_gene_id, gene_symbol, hpo_id, hpo_name, frequency, disease_id |

### Filtered Content

| File Name | Filtered Records | Rationale |
| --- | --- | --- |
| genes_to_phenotype.txt | Records where we determine that the reported G-P association was inferred over a G-D associated type with the value "POLYGENIC" or "UNKNOWN" | HPO will infer a Gene-Phenotype association G1-P1 in cases where G1 causes, contributes_to, or is associated with D1, and D1 is associated with a Phenotype P1. This logic holds for Mendelian disease where a single gene is causal and thus responsible for all associated phenotypes. It does not necessarily hold for Polygenic or Unknown diseases where the gene may be one of many contributing factors, and thus does not necessarily contribute to or have an association with each phenotype of the disease. |

### Future Content Considerations

**edge_content:** Consider bringing back G-P associations based on inferences over Polygenic or Unknown Diseases if we establish a confidence annotation paradigm that lets us indicate these inferences to be weaker than those inferred over Mendelian diseases where the Gene is individually causal for the disease and all of its phenotypes.
  - Relevant files: genes_to_phenotype.txt

**Additional Notes:** None

## Target Information

**Target InfoRes ID:** infores:translator-hpo-annotations-kgx

### Edge Types

| Subject Categories | Predicate | Object Categories | Knowledge Level | Agent Type | UI Explanation |
| --- | --- | --- | --- | --- | --- |
| biolink:Disease | biolink:has_phenotype | biolink:PhenotypicFeature | knowledge_assertion | manual_agent | HPO curators manually review clinical data and published evidence to determine phenotypes that manifest in a Disease, which are reported using the has_phenotype predicate. |
| biolink:Gene | biolink:associated_with | biolink:Disease | knowledge_assertion | manual_agent | HPOA aggregates manually curated Gene-Disease associations from sources like Orphanet and MIM2Gene and DECIPHER. For Mendelian diseases with a single causal gene, we report that a genetic variant form of the gene 'causes' the disease. |
| biolink:Gene | biolink:associated_with | biolink:Disease | knowledge_assertion | manual_agent | HPOA aggregates manually curated Gene-Disease associations from sources like Orphanet and MIM2Gene. For polygenic diseases with multiple contributing genes, we report that a genetic variant form of the gene 'contributes to' the disease. |
| biolink:Gene | biolink:associated_with | biolink:Disease | knowledge_assertion | manual_agent | HPOA aggregates manually curated Gene-Disease associations from sources like Orphanet and MIM2Gene. When the genetic etiology of the diseases is not sufficiently specified, the relationship is reported using the 'associated_with' predicate. |
| biolink:Gene | biolink:has_phenotype | biolink:PhenotypicFeature | logical_entailment | automated_agent | HPOA provides direct Gene-Phenotype associations between genes with variants causing or contributing to a disease, and each phenotype associated with the disease. For Mendelian diseases with a single causal gene, we report that a genetic variant form of the gene 'causes' each of the phenotypes associated with the disease. |

### Node Types

| Node Category | Source Identifier Types | Additional Notes |
| --- | --- | --- |
| biolink:Disease | OMIM, ORPHANET, DECIPHER | None |
| biolink:PhenotypicFeature | HP | None |
| biolink:Gene | NCBIGene | None |
| biolink:GeneticInheritance | HP | None |

### Future Modeling Considerations

**spoq_pattern:** Consider alternate patterns for representing G-causes-D and G-contributes_to-D associations where we place more semantics into predicates, per https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues/22 Should we consider creating support paths in our data/graphs, for the G-D-P hops over which HPO infers G-P associations (e.g. `GENE1 -causes-> DISEASE1 -has_phenotype-> PHENO1 ---->  GENE1 -causes-> PHENO1`)?

Note that the values of the **`disease_context_qualifier`** in the Gene-to-Phenotype **Associations** are not currently being remapped to MONDO, as was attempted in the Monarch Initiative ingest.

## Provenance Information

**Contributors:**
- Richard Bruskiewich - data modeling, domain expertise, code author
- Kevin Schaper - code author
- Sierra Moxon - data modeling, domain expertise, code support
- Matthew Brush - data modeling, domain expertise

**Artifacts:**
- Ingest Survey (https://docs.google.com/spreadsheets/d/1R9z-vywupNrD_3ywuOt_sntcTrNlGmhiUWDXUdkPVpM/edit?gid=0#gid=0)
- Ingest Ticket (https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues/24)
- Modeling Ticket (https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues/22)

