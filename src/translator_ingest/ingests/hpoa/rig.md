#  Human Phenotype Ontology Annotations (HPOA) Reference Ingest Guide (RIG)

---------------

## Source Information

### Infores
 - [infores:hpo-annotations](https://w3id.org/information-resource-registry/hpo-annotations)

### Description (o)
 
The [Human Phenotype Ontology (HPO)](https://hpo.jax.org/) provides standard vocabulary of phenotypic abnormalities encountered in human disease. Each term in the HPO describes a phenotypic abnormality, such as Atrial septal defect. The HPO is currently being developed using the medical literature, Orphanet, DECIPHER, and OMIM. HPO currently contains over 18,000 terms and over 156,000 annotations to hereditary diseases. The HPO project and others have developed software for phenotype-driven differential diagnostics, genomic diagnostics, and translational research. 

The Human Phenotype Ontology group curates and assembles over 115,000 HPO-related annotations ("HPOA") to hereditary diseases using the HPO ontology. Here we create Biolink associations between diseases and phenotypic features, together with their evidence, and age of onset and frequency (if known).  Disease annotations here are also cross-referenced to the [**MON**arch **D**isease **O**ntology (MONDO)](https://mondo.monarchinitiative.org/).

There are four HPOA ingests ('disease-to-phenotype', 'disease-to-mode-of-inheritance', 'gene-to-phenotype' and 'gene-to-disease') that parse out records from the [HPO Phenotype Annotation File](http://purl.obolibrary.org/obo/hp/hpoa/phenotype.hpoa).
   
### Source Category(ies)
Use terms from the enumerated list [here](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/rig-specification.md#source-categoryies).

- Primary Knowledge Provider
  
### Citation (o)
- https://doi.org/10.1093/nar/gkaa1043

### Terms of Use
 - Bespoke 'terms of use' are described here: https://hpo.jax.org/license

### Data Access Locations
- https://hpo.jax.org/data/annotations

### Provision Mechanisms and Formats
- Mechanism(s): File download.
- Formats: Text files, with formats described here: https://hpo.jax.org/data/annotation-format
   
### Releases and Versioning
 - GitHub managed releases: https://github.com/obophenotype/human-phenotype-ontology/releases
 - No consistent cadence for releases.
 - Versioning is based on the month and year of the release

---------------- 

## Ingest Information
    
### Utility
The HPO and associated annotations are a flagship product of the [Monarch Initiative](https://monarchinitiative.org/), an NIH-supported international consortium dedicated to semantic integration of biomedical and model organism data with the ultimate goal of improving biomedical research. The human phenotype/disease/gene knowledge integration aligns well with the general mission of the Biomedical Data Translator.  As a consequence, several members of the Monarch Initiative are direct participants in the Biomedical Data Translator, with Monarch data forming one primary knowledge source contributing to Translator knowledge graphs. 

### Scope
Covers curated Disease, Phenotype and Genes relationships annotated with Human Phenotype Ontology terms. 

  #### Relevant Files:
  Source files with content we aim to ingest.
  
  | File      |       Location            |   Description   |
  |-----------|---------------------------|-----------------|
  | phenotype.hpoa | https://hpo.jax.org/data/annotations  | disease to HPO phenotype annotations, including inheritance information |
  | genes_to_disease.txt   | https://hpo.jax.org/data/annotations | gene to HPO disease annotations   | 
  | genes_to_phenotype.txt  |  https://hpo.jax.org/data/annotations  | gene to HPO phenotype annotations  |

  #### Included Content / Records:
  Records from the relevant files that are included, and optionally a list of fields in the data that are part of or inform the ingest. 

  | File   | Included Content   | Fields Used  |
  |--------|--------------|-----------------|
  | phenotype.hpoa   | Disease to Phenotype relationships (i.e., rows with 'aspect' == 'P') | database_id, qualifier, hpo_id, reference, evidence, onset, frequency, sex, aspect |
  | phenotype.hpoa    | Disease "Mode of Inheritance" relationships (i.e., rows with 'aspect' == 'I')  - represented as node properties rather than edges | database_id, hpo_id, reference, evidence, aspect |
  | genes_to_disease.txt  | Mendelian Gene to Disease relationships (i.e., rows with 'association_type' == 'MENDELIAN')  | ncbi_gene_id, gene_symbol, association_type, disease_id, source |
  | genes_to_disease.txt  | Polygenic Gene to Disease relationships (i.e., rows with 'association_type' == 'POLYGENIC')  | ncbi_gene_id, gene_symbol, association_type, disease_id, source |
  | genes_to_disease.txt  | General Gene Contributions to Disease relationships (i.e., rows with 'association_type' == 'UNKNOWN')  | ncbi_gene_id, gene_symbol, association_type, disease_id, source |
  | genes_to_phenotype.txt | Records where we determine that the reported G-P association was inferred over a G-D associated type with the value "MENDELIAN"  | ncbi_gene_id, gene_symbol, hpo_id, hpo_name, frequency, disease_id |


### Filtered Content / Records
Records from relevant files that are not included in the ingest.

  | File | Filtered Records | Rationale |
  |----------|----------|----------|
  | genes_to_phenotype.txt  | Records where we determine that the reported G-P association was inferred over a G-D associated type with the value "POLYGENIC" or "UNKNOWN" | HPO will infer a Gene-Phenotype association G1-P1 in cases where G1 causes, contributes_to, or is associated with D1, and D1 is associated with a Phenotype P1. This logic holds for Mendelian disease where a single gene is causal and thus responsible for all associated phenotypes.  It does not necessarily hold for Polygenic or Unknown diseases where the gene may be one of many contributing factors, and thus does not necessarily contribute to or have an association with each phenotype of the disease. |

### Future Content Considerations (o)
Content addditions/changes to consider for future iterations (consider edge content node property content, and edge property/EPC content)

- **Edges**
   - Consider bringing back G-P associations based on inferences over Polygenic or Unknown Diseases if we establish a confidence annotation paradigm that lets us indicate these inferences to be weaker than those inferred over Mendelian diseases where the Gene is individually causal for the disease and all of its phenotypes.  

- **Node Properties**
  - n/a
    
- **Edge Properties/EPC Metadata**
  - n/a
    
-----------------

##  Target Information

### Infores:
 - infores:translator-hpo-annotations-kgx
   
### Edge Types

|  Subject Category |  Predicate | Object Category | Qualifier Types |  AT / KL  | Edge Properties | UI Explanation |
|----------|----------|----------|----------|----------|---------|----------|
| Disease  | has_phenotype  | PhenotypicFeature  | negated, onset_qualifier, frequency_qualifier, sex_qualifier  | manual agent, knowledge assertion   | has_count, has_total, has_percentage, has_quotient, publications, has_evidence | HPO curators manually review clinical data and published evidence to determine phenotypes that manifest in a Disease, which are reported using the has_phenotype predicate. |
| Gene  | associated_with | Disease  |  subject_form_or_variant_qualifier: genetic_variant_form, qualified_predicate: causes  | manual agent, knowledge assertion  | n/a  | HPOA aggregates manually curated Gene-Disease associations from sources like Orphanet and MIM2Gene and DECIPHER. For Mendelian diseases with a single causal gene, we report that a genetic variant form of the gene 'causes' the disease. |
| Gene  | associated_with  | Disease |  subject_form_or_variant_qualifier: genetic_variant_form, qualified_predicate: contributes_to | manual agent, knowledge assertion  | n/a  |  HPOA aggregates manually curated Gene-Disease associations from sources like Orphanet and MIM2Gene. For polygenic diseases with multiple contributing genes, we report that a genetic variant form of the gene 'contributes to' the disease. |
| Gene  | associated_with | Disease  | n/a | manual agent, knowledge assertion  | n/a  | HPOA aggregates manually curated Gene-Disease associations from sources like Orphanet and MIM2Gene. When the genetic etiology of the diseases is not sufficiently specified, the relationship is reported using the 'associated_with' predicate. |
| Gene  | associated_with | Phenotypic Feature | subject_form_or_variant_qualifier: genetic_variant_form, qualified_predicate: causes, frequency_qualifier, disease_context_qualifier | logical entailment, automated_agent [*] | n/a | HPOA provides direct Gene-Phenotype associations between genes with variants causing or contributing to a disease, and each phenotype associated with the disease. For Mendelian diseases with a single causal gene, we report that a genetic variant form of the gene 'causes' each of the phenotypes associated with the disease. |
| Gene  | has_phenotype | Phenotypic Feature | subject_form_or_variant_qualifier: genetic_variant_form, qualified_predicate: causes, frequency_qualifier, disease_context_qualifier | logical entailment, automated_agent  | has_count, has_total, has_percentage, has_quotient, publications, has_evidence | to do | 

[*] Gene-to-Phenotype knowledge assertions are two hop knowledge inferences from the dataset.

#### Alternative proposal for rows 2 - 5 above:
|  Subject Category |  Predicate | Object Category | Qualifier Types |  AT / KL  | Edge Properties | UI Explanation |
|----------|----------|----------|----------|----------|---------|----------|
| Gene  | causes | Disease  |  subject_form_or_variant_qualifier: genetic_variant_form | manual agent, knowledge assertion  | n/a  | HPOA aggregates manually curated Gene-Disease associations from sources like Orphanet and MIM2Gene. For Mendelian diseases with a single causal gene, we report that a genetic variant form of the gene 'causes' the disease. |
| Gene  | contributes_to  | Disease |  subject_form_or_variant_qualifier: genetic_variant_form | manual agent, knowledge assertion  | n/a  | HPOA aggregates manually curated Gene-Disease associations from sources like Orphanet and MIM2Gene. For polygenic diseases with multiple contributing genes, we report that a genetic variant form of the gene 'contributes to' the disease.  |
| Gene  | associated_with | Disease  | n/a | manual agent, knowledge assertion  | n/a  | HPOA aggregates manually curated Gene-Disease associations from sources like Orphanet and MIM2Gene. When the genetic etiology of the diseases is not sufficiently specified, the relationship is reported using the 'associated_with' predicate. | 
| Gene  | causes | Phenotypic Feature | subject_form_or_variant_qualifier: genetic_variant_form, frequency_qualifier, disease_context_qualifier | logical entailment, automated_agent [*] | n/a | HPOA provides direct Gene-Phenotype associations between genes with variants causing or contributing to a disease, and each phenotype associated with the disease. For Mendelian diseases with a single causal gene, we report that a genetic variant form of the gene 'causes' each of the phenotypes associated with the disease. |

   
### Node Types

High-level Biolink categories of nodes produced from this ingest as assigned by ingestors are listed below - however downstream normalization of node identifiers may result in new/different categories ultimately being assigned.

| Biolink Category    | Source Identifier Type(s) | Node Properties |  Notes (o) |
|---------------------|---------------------------|-----------------|--------|
| Disease             | OMIM, ORPHANET, DECIPHER  |  Mode of Inheritance |  |
| PhenotypicFeature   | HPO                       |  none  |  | 
| Gene                | NCBI Gene                 |  none  |  |
| Mode of Inheritance | HPO                       |  none  |  |


### Future Modeling Considerations (o)
- Consider alternate patterns for representing G-causes-D and G-contributes_to-D associations where we place more semantics into predicates, per https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues/22
- Should we consider creating support paths in our data/graphs, for the G-D-P hops over which HPO infers G-P associations?
   -  e.g. `GENE1 -causes-> DISEASE1 -has_phenotype-> PHENO1	  ---->    GENE1 -causes-> PHENO1`

-----------------

## Provenance Information (o)

### Ingest Contributors (o)
- **Richard Bruskiewich**: code author
- **Kevin Schaper**: code author
- **Sierra Moxon**: data modeling, domain expertise, code support
- **Matthew Brush**: data modeling, domain expertise

### Artifacts (o)
- [Ingest Survey](https://docs.google.com/spreadsheets/d/1R9z-vywupNrD_3ywuOt_sntcTrNlGmhiUWDXUdkPVpM/edit?gid=0#gid=0)
- [Ingest Ticket](https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues/24)
- [Modeling Ticket](https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues/22)
  
### Additional Notes (o)
