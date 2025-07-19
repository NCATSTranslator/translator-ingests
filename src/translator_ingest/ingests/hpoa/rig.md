# Reference Ingest Guide for HPOA: Human Phenotype Ontology Annotations

---------------

## Source Information

### Infores
 - [infores:hpo-annotations](https://biolink.github.io/information-resource-registry/resources/hpo-annotations)

### Description
 
The [Human Phenotype Ontology (HPO)](https://hpo.jax.org/) provides standard vocabulary of phenotypic abnormalities encountered in human disease. Each term in the HPO describes a phenotypic abnormality, such as Atrial septal defect. The HPO is currently being developed using the medical literature, Orphanet, DECIPHER, and OMIM. HPO currently contains over 18,000 terms and over 156,000 annotations to hereditary diseases. The HPO project and others have developed software for phenotype-driven differential diagnostics, genomic diagnostics, and translational research. 

The Human Phenotype Ontology group curates and assembles over 115,000 HPO-related annotations ("HPOA") to hereditary diseases using the HPO ontology. Here we create Biolink associations between diseases and phenotypic features, together with their evidence, and age of onset and frequency (if known).  Disease annotations here are also cross-referenced to the [**MON**arch **D**isease **O**ntology (MONDO)](https://mondo.monarchinitiative.org/).

There are four HPOA ingests ('disease-to-phenotype', 'disease-to-mode-of-inheritance', 'gene-to-phenotype' and 'gene-to-disease') that parse out records from the [HPO Phenotype Annotation File](http://purl.obolibrary.org/obo/hp/hpoa/phenotype.hpoa).
   
### Source Category(ies)
- **[Primary Knowledge Provider](https://biolink.github.io/biolink-model/primary_knowledge_source/):** HPOA 
- **[Supporting Knowledge Providers](https://biolink.github.io/biolink-model/primary_knowledge_source/):** OMIM, Orphanet, Decipher
- **[Ontology/Terminology Provider](https://biolink.github.io/biolink-model/OntologyClass/):** HP, MONDO

### Citation

Sebastian Köhler, Michael Gargano, Nicolas Matentzoglu, Leigh C Carmody, David Lewis-Smith, Nicole A Vasilevsky, Daniel Danis, Ganna Balagura, Gareth Baynam, Amy M Brower, Tiffany J Callahan, Christopher G Chute, Johanna L Est, Peter D Galer, Shiva Ganesan, Matthias Griese, Matthias Haimel, Julia Pazmandi, Marc Hanauer, Nomi L Harris, Michael J Hartnett, Maximilian Hastreiter, Fabian Hauck, Yongqun He, Tim Jeske, Hugh Kearney, Gerhard Kindle, Christoph Klein, Katrin Knoflach, Roland Krause, David Lagorce, Julie A McMurry, Jillian A Miller, Monica C Munoz-Torres, Rebecca L Peters, Christina K Rapp, Ana M Rath, Shahmir A Rind, Avi Z Rosenberg, Michael M Segal, Markus G Seidel, Damian Smedley, Tomer Talmy, Yarlalu Thomas, Samuel A Wiafe, Julie Xian, Zafer Yüksel, Ingo Helbig, Christopher J Mungall, Melissa A Haendel, Peter N Robinson, The Human Phenotype Ontology in 2021, Nucleic Acids Research, Volume 49, Issue D1, 8 January 2021, Pages D1207–D1217, https://doi.org/10.1093/nar/gkaa1043

### Terms of Use

 - Bespoke 'terms of use' are described here: https://hpo.jax.org/license

### Data Access Locations

- **HPO Annotation**: https://hpo.jax.org/data/annotations
- **HPO**: http://purl.obolibrary.org/obo/hp.obo
- **MONDO**: https://mondo.monarchinitiative.org/pages/download/
   
### Provision Mechanisms and Formats
- **Mechanism(s):** File download.
- **Formats:** Text files, with formats described here: https://hpo.jax.org/data/annotation-format
   
### Releases and Versioning
 - GitHub managed releases: https://github.com/obophenotype/human-phenotype-ontology/releases
 - No consistent cadence for releases.
 - Versioning is based on the month and year of the release

---------------- 

## Ingest Information
    
### Utility

- The HPO and associated annotations are a flagship product of the [Monarch Initiative](https://monarchinitiative.org/), an NIH-supported international consortium dedicated to semantic integration of biomedical and model organism data with the ultimate goal of improving biomedical research. The human phenotype/disease/gene knowledge integration aligns well with the general mission of the Biomedical Data Translator.  As a consequence, several members of the Monarch Initiative are direct participants in the Biomedical Data Translator, with Monarch data forming one primary knowledge source contributing to Translator knowledge graphs. 

### Scope

- Covers curated Disease, Phenotype and Genes relationships annotated with Human Phenotype Ontology terms. 

  #### Relevant Files:
  
  | File / Endpoint / Table                                                                 | Description                                                                |
  |-----------------------------------------------------------------------------------------|----------------------------------------------------------------------------|
  | [phenotype.hpoa](http://purl.obolibrary.org/obo/hp/hpoa/phenotype.hpoa)                 | disease to HPO phenotype annotations,<br>including inheritance information |
  | [genes_to_disease.txt](http://purl.obolibrary.org/obo/hp/hpoa/genes_to_disease.txt)     | gene to HPO disease annotations                                            | 
  | [genes_to_phenotype.txt](http://purl.obolibrary.org/obo/hp/hpoa/genes_to_phenotype.txt) | gene to HPO phenotype annotations                                          | 
  | [hp.obo](http://purl.obolibrary.org/obo/hp.obo)                                         | Human Phenotype Ontology ("HPO")                                           | 
  | [mondo.sssom.tsv](https://data.monarchinitiative.org/mappings/latest/mondo.sssom.tsv)   | Monarch Disease Ontology ("MONDO")                                         | 

  #### Included Content:

  | File                   | Included Content                                                                                                                                                                  | Fields Used                                                                               |
  |------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
  | phenotype.hpoa         | Disease to Phenotype relationships<br>(i.e., rows with 'aspect' == 'P')                                                                                                           | database_id, qualifier, hpo_id,<br>reference, evidence, onset,<br>frequency, sex, aspect |
  | phenotype.hpoa         | Disease "Mode of Inheritance" relationships<br>(i.e., rows with 'aspect' == 'I')                                                                                                  | database_id, hpo_id, reference,<br>evidence, aspect                                       |
  | genes_to_disease.txt   | Mendelian Gene to Disease relationships<br>(i.e., rows with 'association_type' == 'MENDELIAN')                                                                                    | ncbi_gene_id, gene_symbol,<br>association_type, disease_id, source                        |
  | genes_to_disease.txt   | Polygenic Gene to Disease relationships<br>(i.e., rows with 'association_type' == 'POLYGENIC')                                                                                    | ncbi_gene_id, gene_symbol,<br>association_type, disease_id, source                        |
  | genes_to_disease.txt   | General Gene Contributions to Disease relationships<br>(i.e., rows with 'association_type' == 'UNKNOWN')                                                                          | ncbi_gene_id, gene_symbol,<br>association_type, disease_id, source                        |
  | genes_to_phenotype.txt | Gene to Phenotype relationships                                                                                                                                                   | ncbi_gene_id, gene_symbol,<br>hpo_id, hpo_name, frequency, disease_id                     |
  | mondo.sssom.tsv        | Mappings of HPOA (OMIM/ORPHANET/DECIPHER)<br>contextual disease identifiers onto MONDO terms<br>(may not be done in the ingest, but rather,<br>in the ingest normalization step?) | subject_id, predicate_id, object_id                                                       |
  | obo/hp.obo             | Human Phenotype Ontology<br>(definitions and term hierarchy)                                                                                                                     | Accessed for "disease mode of inheritance"<br>HPO terms mappings                          |


-----------------

##  Target Information

### Infores:
 - [infores:hpo-annotations](https://biolink.github.io/information-resource-registry/resources/hpo-annotations)
   
### Edge Types

| # | Association Type                                                    | Subject Category | Predicate                        | Object Category    | Qualifier Types                                                                                                                  | Other Edge Properties                                                                                     | AT / KL                                   | UI Explanation |
|---|---------------------------------------------------------------------|------------------|----------------------------------|--------------------|----------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|-------------------------------------------|----------------|
| 1 | Disease To Phenotypic Feature Association                           | Disease          | `has_phenotype`                  | PhenotypicFeature  | negated, onset_qualifier,<br>frequency_qualifier,<br>sex_qualifier (female -> PATO:0000383,<br>male -> PATO:0000384 or None)<br> | has_count, has_total,<br>has_percentage, has_quotient,<br>publications,<br>has_evidence (IEA,PCS,TAS,ICE) | manual agent<br>knowledge assertion       | n.s.           |
| 2 | CausalGeneToDiseaseAssociation                                      | Gene             | `causes`                         | Disease            |                                                                                                                                  | n.s.                                                                                                      | manual agent<br>knowledge assertion       | n.s.           |
| 3 | CorrelatedGeneToDiseaseAssociation                                  | Gene             | `contributes_to`                 | Disease            |                                                                                                                                  | n.s.                                                                                                      | manual agent<br>knowledge assertion       | n.s.           |
| 4 | CorrelatedGeneToDiseaseAssociation                                  | Gene             | `gene_associated_with_condition` | Disease            |                                                                                                                                  | n.s.                                                                                                      | manual agent<br>knowledge assertion       | n.s.           |
| 5 | Gene To Phenotypic Feature Association                              | Gene             | `has_phenotype`                  | Phenotypic Feature | frequency_qualifier,<br>disease_context_qualifier                                                                                | n.s.                                                                                                      | manual agent<br>knowledge assertion       | n.s.           |
| 6 | Gene To Phenotypic Feature Association                              | Gene             | `has_phenotype`                  | Phenotypic Feature | frequency_qualifier,<br> disease_context_qualifier                                                                               | has_count, has_total,<br>has_percentage, has_quotient,<br>publications,<br>has_evidence (IEA,PCS,TAS,ICE) | logical entailment<br>automated_agent [*] | n.s.           |
| 7 | Disease or Phenotypic Feature<br>to Genetic Inheritance Association | Disease          | `has_mode_of_inheritance`        | PhenotypicFeature  | n.s.                                                                                                                             | publications                                                                                              | manual agent<br>knowledge assertion       | n.s.           |

[*] Gene-to-disease knowledge assertions are two hop knowledge inferences from the dataset.
   
### Node Types

High-level Biolink categories of nodes produced from this ingest as assigned by ingestors are listed below - however downstream normalization of node identifiers may result in new/different categories ultimately being assigned.

| Biolink Category  | Source Identifier Type(s) | 
|-------------------|---------------------------|
| Disease           | OMIM, ORPHANET, DECIPHER  |
| PhenotypicFeature | HPO                       |
| Gene              | NCBI Gene                 |

------------------

## Example Transformations

### Disease to Phenotype

#### Example Source Data

```json
{
  "database_id": "OMIM:117650",
  "disease_name": "Cerebrocostomandibular syndrome",
  "qualifier": "",
  "hpo_id": "HP:0001249",
  "reference": "OMIM:117650",
  "evidence": "TAS",
  "onset": "",
  "frequency": "50%",
  "sex": "",
  "modifier": "",
  "aspect": "P"
}
```

#### Example Transformed Data

```json
{
  "id": "uuid:...",
  "subject": "OMIM:117650",
  "predicate": "biolink:has_phenotype",
  "object": "HP:0001249",
  "frequency_qualifier": 50.0,
  "has_evidence": ["ECO:0000033"],
  "primary_knowledge_source": "infores:hpo-annotations",
  "aggregator_knowledge_source": ["infores:monarchinitiative"]
}
```

### Disease to Mode of Inheritance

#### Example Source Data

```json
{
  "database_id": "OMIM:300425",
  "disease_name": "Autism susceptibility, X-linked 1",
  "qualifier": "",
  "hpo_id": "HP:0001417",
  "reference": "OMIM:300425",
  "evidence": "IEA",
  "aspect": "I"
}
```

#### Example Transformed Data

```json
{
  "id": "uuid:...",
  "subject": "OMIM:300425",
  "predicate": "biolink:has_mode_of_inheritance",
  "object": "HP:0001417",
  "has_evidence": ["ECO:0000501"],
  "primary_knowledge_source": "infores:hpo-annotations",
  "aggregator_knowledge_source": ["infores:monarchinitiative"]
}
```

### Gene to Disease

This ingest replaces the direct OMIM ingest so that we share gene-to-disease associations 1:1 with HPO. It processes the tab-delimited [genes_to_disease.txt](http://purl.obolibrary.org/obo/hp/hpoa/genes_to_disease.txt) file.

#### Example Source Data

```json
{
  "association_type": "MENDELIAN",
  "disease_id": "OMIM:212050",
  "gene_symbol": "CARD9",
  "ncbi_gene_id": "NCBIGene:64170",
  "source": "ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/mim2gene_medgen"
}
```

#### Example Transformed Data

```json
{
  "id": "uuid:...",
  "subject": "NCBIGene:64170",
  "predicate": "biolink:causes",
  "object": "OMIM:212050",
  "primary_knowledge_source": "infores:omim",
  "aggregator_knowledge_source": ["infores:monarchinitiative", "infores:medgen"]
}
```

### Gene to Phenotype

#### Example Source Data

```json
{
  "ncbi_gene_id": "8192",
  "gene_symbol": "CLPP",
  "hpo_id": "HP:0000252",
  "hpo_name": "Microcephaly",
  "publications": "PMID:1234567;OMIM:614129",
  "frequency": "3/10",
  "disease_id": "OMIM:614129"
}
```

#### Example Transformed Data

```json
{
  "id": "uuid:...",
  "subject": "NCBIGene:8192",
  "predicate": "biolink:has_phenotype",
  "object": "HP:0000252",
  "publications": ["PMID:1234567","OMIM:614129"],
  "frequency_qualifier": 30.0,
  "in_taxon": "NCBITaxon:9606",
  "primary_knowledge_source": "infores:hpo-annotations",
  "aggregator_knowledge_source": ["infores:monarchinitiative"]
}
```

------------------

## Ingest Contributors
- **Richard Bruskiewich**: code author
- **Kevin Schaper**: code author
- **Sierra Moxon**: code support
- **Matthew Brush**: data modeling, domain expertise
