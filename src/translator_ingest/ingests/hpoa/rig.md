# HPOA: Human Phenotype Ontology Annotation Reference Ingest Guide

## Source Description
The [Human Phenotype Ontology (HPO)](https://hpo.jax.org/) provides a standardized vocabulary of phenotypic abnormalities encountered in human disease. Each term in the HPO describes a phenotypic abnormality, such as Atrial septal defect. The HPO is currently being developed using the medical literature, Orphanet, DECIPHER, and OMIM. HPO currently contains over 18,000 terms and over 156,000 annotations to hereditary diseases. The HPO project and others have developed software for phenotype-driven differential diagnostics, genomic diagnostics, and translational research. 

## Source Utility to Translator
The HPO is a flagship product of the [Monarch Initiative](https://monarchinitiative.org/), an NIH-supported international consortium dedicated to semantic integration of biomedical and model organism data with the ultimate goal of improving biomedical research. Several members of the Monarch Initiative are also direct participants in the Biomedical Data Translator, with Monarch data forming one primary knowledge source contributing to Translator knowledge graphs. 

## Data Access
- **CTD Bulk Downloads**: http://ctdbase.org/downloads/
- **CTD Catalog**: https://ctdbase.org/reports/
     
## Ingest Scope
- Covers curated Disease to Phenotype and Genes to Phenotype associations that report .... 
- Relevant Files:
    - phenotype.hpoa
    - genes_to_disease.txt
    - genes_to_phenotype.txt

  ### Included:

  | File | Rows | Columns |
  |----------|----------|----------|
  | CTD_chemicals_diseases.tsv.gz  | Rows where a direct evidence label is applied, and is of type "T" (therapeutic)  | ChemicalName, ChemicalID, CasRN, DiseaseName, DiseaseID, DirectEvidence, InferenceGeneSymbol, InferenceScore, OmimIDs, PubMedIDs |

  ### Excluded:

  | File | Excluded Subset | Rationale  |
  |----------|----------|----------|
  | CTD_chemicals_diseases.tsv.gz  | Rows where the direct evidence label is of type "M" (marker/mechanism) | These will likely be included in a future ingest |
  | CTD_chemicals_diseases.tsv.gz  | Rows lacking evidence in the DirectEvidence column | Decided that the methodology used to create these inferences gave associations that were not strong/meaningful enough to be of use to Translator |
  | CTD_exposure_events.tsv.gz | All | These will likely be included in a future ingest |

  ### Future Ingest Considerations:

  | File | Rationale |
  |----------|----------|
  | CTD_exposure_events.tsv.gz | May provide additional chemical-disease edges reporting statistical correlations from environmental exposure studues |

## Biolink Edge Types

| No. | Association Type | MetaEdge | Qualifiers |  AT / KL  | Evidence Code  |
|----------|----------|----------|----------|----------|----------|
| 1 | Chemical To Disease Or Phenotypic Feature Association | Chemical Entity - `treats_or_applied_or_studied_to_treat` - Disease Or Phenotypic Feature  |  n/a  |  manual agent, knowledge assertion  | n.s. |

**Rationale**:
- The `treats_or_applied_or_studied_to_treat` predicate is used to avoid making too strong a claim, as CTDs definition of "T" was broad ("a chemical that has a known or potenitial therapeutic role in a disease"), which covered cases where a chemical may formally treat a disease or only have been studied or applied to treat a disease. 
- ALl edges are manual agent knowledge assertiosn, as theingested data is based on manual literature curation.
      
## Biolink Node Types
- ChemicalEntity (MeSH)
- DiseaseOrPhenotypicFeature (MeSH)

Note: The ChemicalEntity and Disease nodes here are placeholders only and lack a full representation node properties, and may not accurately reflect the correct biolink category.



## Source Quality/Confidence Assessment
- ...


## Misc Notes
- ...

### ### Original Monarch Ingest doc (from monarch-phenotype-profile-ingest Git repo)

<!--

# monarch-phenotype-profile-ingest Report

{{ get_nodes_report() }}

{{ get_edges_report() }}
-->

# Human Phenotype Ontology Annotations (HPOA)

The [Human Phenotype Ontology](http://human-phenotype-ontology.org) group
curates and assembles over 115,000 annotations to hereditary diseases
using the HPO ontology. Here we create Biolink associations
between diseases and phenotypic features, together with their evidence,
and age of onset and frequency (if known).

There are four HPOA ingests - 'disease-to-phenotype', 'disease-to-mode-of-inheritance', 'gene-to-disease' and 'gene-to-phenotype' - that parse out records from the [HPO Annotation File](http://purl.obolibrary.org/obo/hp/hpoa/phenotype.hpoa).

## [Disease to Phenotype](#disease_to_phenotype)

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

## [Disease to Mode of Inheritance](#disease_modes_of_inheritance)

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

## [Gene to Disease](#gene_to_disease)

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

## [Gene to Phenotype](#gene_to_phenotype)

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