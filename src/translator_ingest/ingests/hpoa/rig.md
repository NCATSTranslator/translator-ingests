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

### ############# Monarch Ingest doc (original) ##################

# Human Phenotype Ontology Annotations (HPOA)

The [Human Phenotype Ontology](http://human-phenotype-ontology.org) group
curates and assembles over 115,000 annotations to hereditary diseases
using the HPO ontology. Here we create Biolink associations
between diseases and phenotypic features, together with their evidence,
and age of onset and frequency (if known).

There are four HPOA ingests - 'disease-to-phenotype', 'disease-to-mode-of-inheritance', 'gene-to-disease' and 'disease-to-mode-of-inheritance' - that parse out records from the [HPO Annotation File](http://purl.obolibrary.org/obo/hp/hpoa/phenotype.hpoa).

The 'disease-to-phenotype', 'disease-to-mode-of-inheritance' and 'gene-to-disease' parsers currently only process the "abnormal" annotations.
Association to "remarkable normality" may be added in the near future.

The 'disease-to-mode-of-inheritance' ingest script parses 'inheritance' record information out from the annotation file.

## [Gene to Disease](#gene_to_disease)

This ingest replaces the direct OMIM ingest so that we share g2d associations 1:1 with HPO. The mapping between association_type and biolink predicates shown below is the one way in which this ingest is opinionated, but attempts to be a direct translation into the biolink model.

**genes_to_disease.txt** with the following fields:

  - 'ncbi_gene_id'
  - 'gene_symbol'
  - 'association_type'
  - 'disease_id'
  - 'source'

__**Biolink Captured**__

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

## [Disease to Phenotype](#disease_to_phenotype)

**phenotype.hpoa:** [A description of this file is found here](https://hpo-annotation-qc.readthedocs.io/en/latest/annotationFormat.html#phenotype-hpoa-format), has the following fields:

  - 'database_id'
  - 'disease_name'
  - 'qualifier'
  - 'hpo_id'
  - 'reference'
  - 'evidence'
  - 'onset'
  - 'frequency'
  - 'sex'
  - 'modifier'
  - 'aspect'
  - 'biocuration'


Note that we're calling this the disease to phenotype file because - using the YAML file filter configuration for the ingest - we are only parsing rows with **Aspect == 'P' (phenotypic anomalies)**, but ignoring all other Aspects.

__**Frequencies**__

The 'Frequency' field of the aforementioned **phenotypes.hpoa** file has the following definition, excerpted from its [Annotation Format](https://hpo-annotation-qc.readthedocs.io/en/latest/annotationFormat.html#phenotype-hpoa-format) page:

    8. Frequency: There are three allowed options for this field. (A) A term-id from the HPO-sub-ontology below the term “Frequency” (HP:0040279). (since December 2016 ; before was a mixture of values). The terms for frequency are in alignment with Orphanet. * (B) A count of patients affected within a cohort. For instance, 7/13 would indicate that 7 of the 13 patients with the specified disease were found to have the phenotypic abnormality referred to by the HPO term in question in the study referred to by the DB_Reference; (C) A percentage value such as 17%.

The Disease to Phenotype ingest attempts to remap these raw frequency values onto a suitable HPO term.  A simplistic (perhaps erroneous?) assumption is that all such frequencies are conceptually comparable; however, researchers may wish to review the original publications to confirm fitness of purpose of the specific data points to their interpretation - specific values could designate phenotypic frequency at the population level; phenotypic frequency at the cohort level; or simply, be a measure of penetrance of a specific allele within carriers, etc..

__**Biolink captured**__

* biolink:DiseaseToPhenotypicFeatureAssociation
    * id (random uuid)
    * subject (disease.id)
    * predicate (has_phenotype)
    * negated (True if 'qualifier' == "NOT")
    * object (phenotypicFeature.id)
    * publications (List[publication.id])
    * has_evidence (List[Note [1]]),
    * sex_qualifier (Note [2]) 
    * onset_qualifier (Onset.id)
    * frequency_qualifier (Note [3])
    * aggregating_knowledge_source (["infores:monarchinitiative"])
    * primary_knowledge_source ("infores:hpo-annotations")

Notes:
1. CURIE of [Evidence and Conclusion Ontology(https://bioportal.bioontology.org/ontologies/ECO)] term
2. female -> PATO:0000383, male -> PATO:0000384 or None
3. See the [Frequencies](#frequencies) section above.

## [Disease to Modes of Inheritance](#disease_modes_of_inheritance)

Same as above, we again parse the [phenotype.hpoa file](https://hpo-annotation-qc.readthedocs.io/en/latest/annotationFormat.html#phenotype-hpoa-format).

However, we're calling this the 'disease to modes of inheritance' file because - using the YAML file filter configuration for the ingest - we are only parsing rows with **Aspect == 'I' (inheritance)**, but ignoring all other Aspects.

__**Biolink captured**__

* biolink:DiseaseOrPhenotypicFeatureToGeneticInheritanceAssociation
    * id (random uuid)
    * subject (disease.id)
    * predicate (has_mode_of_inheritance)
    * object (geneticInheritance.id)
    * publications (List[publication.id])
    * has_evidence (List[Note [1]]),
    * aggregating_knowledge_source (["infores:monarchinitiative"])
    * primary_knowledge_source ("infores:hpo-annotations")

## [Gene to Phenotype](#gene_to_phenotype)

The gene-to-phenotype ingest processes the tab-delimited [HPOA gene_to_phenotype.txt](http://purl.obolibrary.org/obo/hp/hpoa/genes_to_phenotype.txt) file, which has the following fields:

  - 'ncbi_gene_id'
  - 'gene_symbol'
  - 'hpo_id'
  - 'hpo_name'

__**Biolink captured**__

* biolink:GeneToPhenotypicFeatureAssociation
    * id (random uuid)
    * subject (gene.id)
    * predicate (has_phenotype)
    * object (phenotypicFeature.id)
    * aggregating_knowledge_source (["infores:monarchinitiative"])
    * primary_knowledge_source (infores:hpo-annotations)
 
## Citation

Sebastian Köhler, Michael Gargano, Nicolas Matentzoglu, Leigh C Carmody, David Lewis-Smith, Nicole A Vasilevsky, Daniel Danis, Ganna Balagura, Gareth Baynam, Amy M Brower, Tiffany J Callahan, Christopher G Chute, Johanna L Est, Peter D Galer, Shiva Ganesan, Matthias Griese, Matthias Haimel, Julia Pazmandi, Marc Hanauer, Nomi L Harris, Michael J Hartnett, Maximilian Hastreiter, Fabian Hauck, Yongqun He, Tim Jeske, Hugh Kearney, Gerhard Kindle, Christoph Klein, Katrin Knoflach, Roland Krause, David Lagorce, Julie A McMurry, Jillian A Miller, Monica C Munoz-Torres, Rebecca L Peters, Christina K Rapp, Ana M Rath, Shahmir A Rind, Avi Z Rosenberg, Michael M Segal, Markus G Seidel, Damian Smedley, Tomer Talmy, Yarlalu Thomas, Samuel A Wiafe, Julie Xian, Zafer Yüksel, Ingo Helbig, Christopher J Mungall, Melissa A Haendel, Peter N Robinson, The Human Phenotype Ontology in 2021, Nucleic Acids Research, Volume 49, Issue D1, 8 January 2021, Pages D1207–D1217, https://doi.org/10.1093/nar/gkaa1043

