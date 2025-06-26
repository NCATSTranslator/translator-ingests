# CTD Chemical-Disease Reference Ingest Guide (RIG)

## Source Description and Licensing
 - CTD is a robust, publicly available database that aims to advance understanding about how environmental exposures affect human health.
 - It provides manually curated information about chemical–gene/protein interactions, chemical–disease and gene–disease relationships.
 - These data are integrated with functional and pathway data to aid in development of hypotheses about the mechanisms underlying environmentally influenced diseases.
 - Terms of use are described here: https://ctdbase.org/about/legal.jsp


## Source Utility to Translator
CTD is the premier source of curated chemical-disease associations, which are an improtant type of edge for Translator query and reasoning use cases, including treatment predictions, chemical-gene regulation predictions, and pathfinder queries. 


## Data Provision Mechanism(s)
- File download.  tsv, csv, obo, or xml .gz files.
- There are two file download webpages for this - not sure what the difference is. 


## Data Release and Versioning
 - Releases vary in cadence, roughly 1-2 reelases per month.
 - Versioning is based on month + year  of the release
 - Releases page / change log: https://ctdbase.org/about/changes/
 - Latest status page: https://ctdbase.org/about/dataStatus.go

   
## Data Access and Documentation
 - CTD Bulk Downloads: http://ctdbase.org/downloads/  (this page includes simple data dictioanries for each download)
 - CTD Catalog: https://ctdbase.org/reports/


## Ingest Scope
 - Translator's ingest covers curated Chemical to Disease associations that report therapeutic associations (treats or studied or applied to treat).
 - Scope to be expanded pendingingest team discussions


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

## Ingested Edge Types

| No. | Association Type | MetaEdge | Qualifiers |  AT / KL  | Evidence Code  |
|----------|----------|----------|----------|----------|----------|
| 1 | Chemical To Disease Or Phenotypic Feature Association | Chemical Entity - `treats_or_applied_or_studied_to_treat` - Disease Or Phenotypic Feature  |  n/a  |  manual agent, knowledge assertion  | n.s. |

**Rationale**:
1. The `treats_or_applied_or_studied_to_treat` predicate is used to avoid making too strong a claim, as CTDs definition of "T" was broad ("a chemical that has a known or potenitial therapeutic role in a disease"), which covered cases where a chemical may formally treat a disease or only have been studied or applied to treat a disease. All edges are manual agent knowledge assertions, as the ingested data is based on manual literature curation.
      
## Ingested Node Types
- ChemicalEntity (MeSH)
- DiseaseOrPhenotypicFeature (MeSH)

Note: The ChemicalEntity and Disease nodes here are placeholders only and lack a full representation node properties, and may not accurately reflect the correct biolink category.



## Source Quality/Confidence Assessment
- ...


## Misc Notes
- ...


