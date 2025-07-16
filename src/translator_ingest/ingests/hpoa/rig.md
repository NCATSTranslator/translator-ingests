# Reference Ingest Guide for HPOA: Human Phenotype Ontology Annotations

---------------

## Source Information

### Infores
 - [infores:hpo-annotations](https://biolink.github.io/information-resource-registry/resources/hpo-annotations)

### Description
 
The [Human Phenotype Ontology (HPO)](https://hpo.jax.org/) provides standard vocabulary of phenotypic abnormalities encountered in human disease. Each term in the HPO describes a phenotypic abnormality, such as Atrial septal defect. The HPO is currently being developed using the medical literature, Orphanet, DECIPHER, and OMIM. HPO currently contains over 18,000 terms and over 156,000 annotations to hereditary diseases. The HPO project and others have developed software for phenotype-driven differential diagnostics, genomic diagnostics, and translational research. 

The Human Phenotype Ontology group curates and assembles over 115,000 HPO related annotations to hereditary diseases using the HPO ontology. Here we create Biolink associations between diseases and phenotypic features, together with their evidence, and age of onset and frequency (if known).  Disease annotations here are also cross-referenced to the [**MON**arch **D**isease **O**ntology (MONDO)](https://mondo.monarchinitiative.org/).

There are three <!-- four --> HPOA ingests - 'disease-to-phenotype', <!-- 'disease-to-mode-of-inheritance',--> 'gene-to-phenotype' and 'gene-to-disease'  - that parse out records from the [HPO Phenotype Annotation File](http://purl.obolibrary.org/obo/hp/hpoa/phenotype.hpoa).
   
### Source Category(ies)
- [Primary Knowledge Source](https://biolink.github.io/biolink-model/primary_knowledge_source/)   

### Citation

Sebastian Köhler, Michael Gargano, Nicolas Matentzoglu, Leigh C Carmody, David Lewis-Smith, Nicole A Vasilevsky, Daniel Danis, Ganna Balagura, Gareth Baynam, Amy M Brower, Tiffany J Callahan, Christopher G Chute, Johanna L Est, Peter D Galer, Shiva Ganesan, Matthias Griese, Matthias Haimel, Julia Pazmandi, Marc Hanauer, Nomi L Harris, Michael J Hartnett, Maximilian Hastreiter, Fabian Hauck, Yongqun He, Tim Jeske, Hugh Kearney, Gerhard Kindle, Christoph Klein, Katrin Knoflach, Roland Krause, David Lagorce, Julie A McMurry, Jillian A Miller, Monica C Munoz-Torres, Rebecca L Peters, Christina K Rapp, Ana M Rath, Shahmir A Rind, Avi Z Rosenberg, Michael M Segal, Markus G Seidel, Damian Smedley, Tomer Talmy, Yarlalu Thomas, Samuel A Wiafe, Julie Xian, Zafer Yüksel, Ingo Helbig, Christopher J Mungall, Melissa A Haendel, Peter N Robinson, The Human Phenotype Ontology in 2021, Nucleic Acids Research, Volume 49, Issue D1, 8 January 2021, Pages D1207–D1217, https://doi.org/10.1093/nar/gkaa1043

### Terms of Use

 - Bespoke 'terms of use' are described here: https://hpo.jax.org/license

### Data Access Locations

Bulk downloading from https://hpo.jax.org/data/annotations
   
### Provision Mechanisms and Formats
- Mechanism(s): File download.
- Text files, with formats described here: https://hpo.jax.org/data/annotation-format
   
### Releases and Versioning
 - No consistent cadence for releases(?).
 - Versioning is based on the month and year of the release

---------------- 

## Ingest Information
    
### Utility

- The HPO and associated annotations are a flagship product of the [Monarch Initiative](https://monarchinitiative.org/), an NIH-supported international consortium dedicated to semantic integration of biomedical and model organism data with the ultimate goal of improving biomedical research.

- Several members of the Monarch Initiative are also direct participants in the Biomedical Data Translator, with Monarch data forming one primary knowledge source contributing to Translator knowledge graphs. 

### Scope

- Covers curated Disease to Phenotype and Genes to Phenotype associations that report ... 
- Relevant Files:
    - [phenotype.hpoa](http://purl.obolibrary.org/obo/hp/hpoa/phenotype.hpoa)
    - [genes_to_disease.txt](http://purl.obolibrary.org/obo/hp/hpoa/genes_to_disease.txt)
    - [genes_to_phenotype.txt](http://purl.obolibrary.org/obo/hp/hpoa/genes_to_phenotype.txt)
    - [mondo.sssom.tsv](https://data.monarchinitiative.org/mappings/latest/mondo.sssom.tsv)
    - [monarch_ingest/constants.py](https://raw.githubusercontent.com/monarch-initiative/monarch-ingest/main/src/monarch_ingest/constants.py)
    - [obo/hp.obo](http://purl.obolibrary.org/obo/hp.obo)

  #### Relevant Files:

  | File                        | Location | Description |
  |-----------------------------|----------|-------------|
  | phenotype.hpoa              | ?        | ?           |
  | genes_to_disease.txt        | ?        | ?           |
  | genes_to_phenotype.txt      | ?        | ?           |
  | mondo.sssom.tsv             | ?        | ?           |
  | monarch_ingest/constants.py | ?        | ?           |
  | obo/hp.obo                  | ?        | ?           |
  
  #### Included Content:

  | File                        | Included Content | Fields Used |
  |-----------------------------|------------------|-------------|
  | phenotype.hpoa              | ?                | ?           |
  | genes_to_disease.txt        | ?                | ?           |
  | genes_to_phenotype.txt      | ?                | ?           |
  | mondo.sssom.tsv             | ?                | ?           |
  | monarch_ingest/constants.py | ?                | ?           |
  | obo/hp.obo                  | ?                | ?           |


  #### Filtered Records (o):

<!--
  | File | Filtered Content | Rationale |
  |----------|----------|----------|
  | CTD_chemicals_diseases.tsv.gz  | Inferred associations - i.e. rows lacking a value in the DirectEvidence column | Decided that the methodology used to create these inferences gave associations that were not strong/meaningful enough to be of use to Translator |
-->

  #### Future Considerations (o):

<!--
  | File | Content |  Rationale |
  |----------|----------|----------|
  | CTD_exposure_events.tsv.gz |  Additional chemical-disease edges reporting statistical correlations from environmental exposure studies | This is a unique/novel source for this kind of knowledge, but there is not a lot of data here, and utility is not clear, so it may not be worth it. |
-->

-----------------

##  Target Information

### Infores:
 - [infores:hpo-annotations](https://biolink.github.io/information-resource-registry/resources/hpo-annotations)
   
### Edge Types

| No. | Association Type                                                        | MetaEdge                                       | Qualifiers                                                      | AT / KL                           | Evidence Code |
|-----|-------------------------------------------------------------------------|------------------------------------------------|-----------------------------------------------------------------|-----------------------------------|---------------|
| 1   | Disease To Phenotypic Feature Association                               | Disease - `has_phenotype` - Phenotypic Feature | negated, sex_qualifier,<br>onset_qualifier, frequency_qualifier | manual agent, knowledge assertion | n.s.          |
| 2   | CausalGeneToDiseaseAssociation or<br>CorrelatedGeneToDiseaseAssociation | Gene - `causes` - Disease                      | n.s.                                                            | manual agent, knowledge assertion | n.s.          |
| 3   | Gene To Phenotypic Feature Association                                  | Gene - `has_phenotype` - Phenotypic Feature    | n.s.                                                            | manual agent, knowledge assertion | n.s.          |

<!-- TODO: Need to review AT/KL and Evidence Code -->

**Rationale (o)**:

<!--
1. The `treats_or_applied_or_studied_to_treat` predicate is used to avoid making too strong a claim, as CTDs definition of its "T" flag is broad ("a chemical that has a known or potential therapeutic role in a disease"), which covered cases where a chemical may formally treat a disease or only have been studied or applied to treat a disease. All edges are manual agent knowledge assertions, as the ingested data is based on manual literature curation.
2. The `marker_or_causal_for` predicate is used because the CTD 'M' flag does not distinguish between when a chemical is a correlated marker for a condition, or a contributing cause for the condition. All edges are manual agent knowledge assertions, as the ingested data is based on manual literature curation.
-->

- All edges are manual agent knowledge assertion, as the ingested data is based on manual literature curation.
   
### Node Types

High-level Biolink categories of nodes produced from this ingest as assigned by ingestors are listed below - however downstream normalization of node identifiers may result in new/different categories ultimately being assigned.

| Biolink Category  | Source Identifier Type(s) | Notes |
|-------------------|--------------------------|-------|
| Disease           | MONDO                    | ?     |
| PhenotypicFeature | HPO                      | ?     |
| Gene              | HGNC                     | ?     |

------------------

## Ingest Contributors
- ** Richard Bruskiewich **: code author
- **Kevin Schaper**: code author
- **Sierra Moxon**: code support
- **Matthew Brush**: data modeling, domain expertise

-------------------

## Additional Notes (o)

#### ============================= Old RIG =====================================================================

# Reference Ingest Guide for HPOA: Human Phenotype Ontology Annotations 

## Source Description
The [Human Phenotype Ontology (HPO)](https://hpo.jax.org/) provides standard vocabulary of phenotypic abnormalities encountered in human disease. Each term in the HPO describes a phenotypic abnormality, such as Atrial septal defect. The HPO is currently being developed using the medical literature, Orphanet, DECIPHER, and OMIM. HPO currently contains over 18,000 terms and over 156,000 annotations to hereditary diseases. The HPO project and others have developed software for phenotype-driven differential diagnostics, genomic diagnostics, and translational research. 

The Human Phenotype Ontology group curates and assembles over 115,000 HPO related annotations to hereditary diseases using the HPO ontology. Here we create Biolink associations between diseases and phenotypic features, together with their evidence, and age of onset and frequency (if known).  Disease annotations here are also cross-referenced to the [**MON**arch **D**isease **O**ntology (MONDO)](https://mondo.monarchinitiative.org/).

There are three <!-- four --> HPOA ingests - 'disease-to-phenotype', <!-- 'disease-to-mode-of-inheritance',--> 'gene-to-phenotype' and 'gene-to-disease'  - that parse out records from the [HPO Phenotype Annotation File](http://purl.obolibrary.org/obo/hp/hpoa/phenotype.hpoa).

## Source Utility to Translator

The HPO and associated annotations are a flagship product of the [Monarch Initiative](https://monarchinitiative.org/), an NIH-supported international consortium dedicated to semantic integration of biomedical and model organism data with the ultimate goal of improving biomedical research.

Several members of the Monarch Initiative are also direct participants in the Biomedical Data Translator, with Monarch data forming one primary knowledge source contributing to Translator knowledge graphs. 

## Data Access

- **HPOA data**: http://purl.obolibrary.org/obo/hp/hpoa
- **MONDO data**: https://mondo.monarchinitiative.org/pages/download/
- **Related Monarch Phenotype (HPOA) Metadata**: https://github.com/monarch-initiative/monarch-phenotype-profile-ingest
     
## Ingest Scope
- Covers curated Disease to Phenotype and Genes to Phenotype associations that report ... 
- Relevant Files:
    - [phenotype.hpoa](http://purl.obolibrary.org/obo/hp/hpoa/phenotype.hpoa)
    - [genes_to_disease.txt](http://purl.obolibrary.org/obo/hp/hpoa/genes_to_disease.txt)
    - [genes_to_phenotype.txt](http://purl.obolibrary.org/obo/hp/hpoa/genes_to_phenotype.txt)
    - [mondo.sssom.tsv](https://data.monarchinitiative.org/mappings/latest/mondo.sssom.tsv)
    - [monarch_ingest/constants.py](https://raw.githubusercontent.com/monarch-initiative/monarch-ingest/main/src/monarch_ingest/constants.py)
    - [obo/hp.obo](http://purl.obolibrary.org/obo/hp.obo)


  ### Included:


  | File                        | Rows | Columns |
  |-----------------------------|------|---------|
  | phenotype.hpoa              | ?    | ?       |
  | genes_to_disease.txt        | ?    | ?       |
  | genes_to_phenotype.txt      | ?    | ?       |
  | mondo.sssom.tsv             | ?    | ?       |
  | monarch_ingest/constants.py | ?    | ?       |
  | obo/hp.obo                  | ?    | ?       |

  ### Excluded:

<!--
  | File | Excluded Subset | Rationale  |
  |----------|----------|----------|
  | CTD_chemicals_diseases.tsv.gz  | Rows where the direct evidence label is of type "M" (marker/mechanism) | These will likely be included in a future ingest |
  | CTD_chemicals_diseases.tsv.gz  | Rows lacking evidence in the DirectEvidence column | Decided that the methodology used to create these inferences gave associations that were not strong/meaningful enough to be of use to Translator |
  | CTD_exposure_events.tsv.gz | All | These will likely be included in a future ingest |
-->

  | File                        | Excluded Subset | Rationale |
  |-----------------------------|-----------------|-----------|
  | phenotype.hpoa              | ?               | ?         |
  | genes_to_disease.txt        | ?               | ?         |
  | genes_to_phenotype.txt      | ?               | ?         |
  | mondo.sssom.tsv             | ?               | ?         |
  | monarch_ingest/constants.py | ?               | ?         |
  | obo/hp.obo                  | ?               | ?         |

  ### Future Ingest Considerations:

  Disease "Mode of Inheritance" inferences.

  | File           | Rationale                                      |
  |----------------|------------------------------------------------|
  | phenotype.hpoa | Captures some "Mode of Inheritance" inferences |


## Biolink Edge Types


| No. | Association Type                                                         | MetaEdge                                       | Qualifiers                                                      | AT / KL                           | Evidence Code |
|-----|--------------------------------------------------------------------------|------------------------------------------------|-----------------------------------------------------------------|-----------------------------------|---------------|
| 1   | Disease To Phenotypic Feature Association                                | Disease - `has_phenotype` - Phenotypic Feature | negated, sex_qualifier,<br>onset_qualifier, frequency_qualifier | manual agent, knowledge assertion | n.s.          |
| 2   | CausalGeneToDiseaseAssociation or<br>CorrelatedGeneToDiseaseAssociation | Gene - `causes` - Disease                      | n.s.                                                            | manual agent, knowledge assertion | n.s.          |
| 3   | Gene To Phenotypic Feature Association                                   | Gene - `has_phenotype` - Phenotypic Feature    | n.s.                                                            | manual agent, knowledge assertion | n.s.          |

<!-- TODO: Need to review AT/KL and Evidence Code -->

**Rationale**:

- All edges are manual agent knowledge assertion, as the ingested data is based on manual literature curation.

      
## Biolink Node Types

<!--
- ChemicalEntity (MeSH)
- DiseaseOrPhenotypicFeature (MeSH)

Note: The ChemicalEntity and Disease nodes here are placeholders only and lack a full representation node properties, and may not accurately reflect the correct biolink category.
-->
- Disease
- PhenotypicFeature
- Gene

## Source Quality/Confidence Assessment
- ...


## Misc Notes
- ...

### ### Original Monarch Ingest doc (from [monarch-phenotype-profile-ingest](https://github.com/monarch-initiative/monarch-phenotype-profile-ingest) Git repo)

<!--

# monarch-phenotype-profile-ingest Report

{{ get_nodes_report() }}

{{ get_edges_report() }}
-->

# Human Phenotype Ontology Annotations (HPOA)

~~The [Human Phenotype Ontology](http://human-phenotype-ontology.org) group
curates and assembles over 115,000 annotations to hereditary diseases
using the HPO ontology. Here we create Biolink associations
between diseases and phenotypic features, together with their evidence,
and age of onset and frequency (if known).

There are four HPOA ingests - 'disease-to-phenotype', 'disease-to-mode-of-inheritance', 'gene-to-disease' and 'gene-to-phenotype' - that parse out records from the [HPO Annotation File](http://purl.obolibrary.org/obo/hp/hpoa/phenotype.hpoa).~~

## Disease to Phenotype

This ingest processes the tab-delimited [phenotype.hpoa](https://hpo-annotation-qc.readthedocs.io/en/latest/annotationFormat.html#phenotype-hpoa-format) file, filtered for rows with **Aspect == 'P'** (phenotypic anomalies).

### Biolink Entities Captured

* biolink:DiseaseToPhenotypicFeatureAssociation
    * id (random uuid)
    * subject (disease.id)
    * predicate (has_phenotype)
    * negated (True if 'qualifier' == "NOT")
    * object (phenotypicFeature.id)
    * publications (List[publication.id])
    * has_evidence (List[evidence.id])
    * sex_qualifier (female -> PATO:0000383, male -> PATO:0000384 or None)
    * onset_qualifier (Onset.id)
    * frequency_qualifier (See Frequencies section in hpoa.md)
    * aggregating_knowledge_source (["infores:monarchinitiative"])
    * primary_knowledge_source ("infores:hpo-annotations")

### Example Source Data

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

### Example Transformed Data

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

## Disease to Mode of Inheritance

This ingest processes the tab-delimited [phenotype.hpoa](https://hpo-annotation-qc.readthedocs.io/en/latest/annotationFormat.html#phenotype-hpoa-format) file, filtered for rows with **Aspect == 'I'** (inheritance).

### Biolink Entities Captured

* biolink:DiseaseOrPhenotypicFeatureToGeneticInheritanceAssociation
    * id (random uuid)
    * subject (disease.id)
    * predicate (has_mode_of_inheritance)
    * object (geneticInheritance.id)
    * publications (List[publication.id])
    * has_evidence (List[evidence.id])
    * aggregating_knowledge_source (["infores:monarchinitiative"])
    * primary_knowledge_source ("infores:hpo-annotations")

### Example Source Data

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

### Example Transformed Data

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

## Gene to Disease

This ingest replaces the direct OMIM ingest so that we share gene-to-disease associations 1:1 with HPO. It processes the tab-delimited [genes_to_disease.txt](http://purl.obolibrary.org/obo/hp/hpoa/genes_to_disease.txt) file.

### Biolink Entities Captured

* biolink:CorrelatedGeneToDiseaseAssociation or biolink:CausalGeneToDiseaseAssociation (depending on predicate)
    * id (random uuid)
    * subject (ncbi_gene_id)
    * predicate (association_type)
      * MENDELIAN: `biolink:causes`
      * POLYGENIC: `biolink:contributes_to`
      * UNKNOWN: `biolink:gene_associated_with_condition`
    * object (disease_id)
    * primary_knowledge_source (source)
      * medgen: `infores:omim`
      * orphanet: `infores:orphanet`
    * aggregator_knowledge_source (["infores:monarchinitiative"])
      * also for medgen: `infores:medgen`

### Example Source Data

```json
{
  "association_type": "MENDELIAN",
  "disease_id": "OMIM:212050",
  "gene_symbol": "CARD9",
  "ncbi_gene_id": "NCBIGene:64170",
  "source": "ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/mim2gene_medgen"
}
```

### Example Transformed Data

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

## Gene to Phenotype

This ingest processes the tab-delimited [genes_to_phenotype_with_publications.tsv](http://purl.obolibrary.org/obo/hp/hpoa/genes_to_phenotype.txt) file, which is generated by joining genes_to_phenotype.txt with phenotype.hpoa to add publication information.

The publication data is pre-processed using the `scripts/gene_to_phenotype_publications.py` script, which performs a join between gene-to-phenotype associations and phenotype annotations in HPOA. The join is based on matching HPO terms, disease IDs, and frequency values. This enriches gene-to-phenotype associations with relevant publication references from the phenotype.hpoa file.

### Biolink Entities Captured

* biolink:GeneToPhenotypicFeatureAssociation
    * id (random uuid)
    * subject (gene.id)
    * predicate (has_phenotype)
    * object (phenotypicFeature.id)
    * publications (List[publication.id])
    * frequency_qualifier (calculated from frequency data if available)
    * in_taxon (NCBITaxon:9606)
    * aggregating_knowledge_source (["infores:monarchinitiative"])
    * primary_knowledge_source ("infores:hpo-annotations")

### Example Source Data

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

### Example Transformed Data

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

## Citation

Sebastian Köhler, Michael Gargano, Nicolas Matentzoglu, Leigh C Carmody, David Lewis-Smith, Nicole A Vasilevsky, Daniel Danis, Ganna Balagura, Gareth Baynam, Amy M Brower, Tiffany J Callahan, Christopher G Chute, Johanna L Est, Peter D Galer, Shiva Ganesan, Matthias Griese, Matthias Haimel, Julia Pazmandi, Marc Hanauer, Nomi L Harris, Michael J Hartnett, Maximilian Hastreiter, Fabian Hauck, Yongqun He, Tim Jeske, Hugh Kearney, Gerhard Kindle, Christoph Klein, Katrin Knoflach, Roland Krause, David Lagorce, Julie A McMurry, Jillian A Miller, Monica C Munoz-Torres, Rebecca L Peters, Christina K Rapp, Ana M Rath, Shahmir A Rind, Avi Z Rosenberg, Michael M Segal, Markus G Seidel, Damian Smedley, Tomer Talmy, Yarlalu Thomas, Samuel A Wiafe, Julie Xian, Zafer Yüksel, Ingo Helbig, Christopher J Mungall, Melissa A Haendel, Peter N Robinson, The Human Phenotype Ontology in 2021, Nucleic Acids Research, Volume 49, Issue D1, 8 January 2021, Pages D1207–D1217, https://doi.org/10.1093/nar/gkaa1043