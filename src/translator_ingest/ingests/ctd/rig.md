# CTD Chemical-Disease Reference Ingest Guide (RIG)

---------------

## Source Information
*(Note that most/all of the 'Source' info below  may be collected in the infores catalog as a source of truth, and the infores id/web page just be referenced here.*)

### Infores 
 - infores:ctd

### Source Category(ies)
- Primary Knowledge Provider 
  
### Description
 - CTD manually curates knowledge from the literature, and produces structured summaries of study results.
 - It also generates novel inferences by further analyzing the knowledge they curate/create - based on statistically significant connections with intermediate concept (e.g. Chemical X associated with Disease Y based on shared associations with a common set of genes).

### Terms of Use
 - No formal license. Bespoke 'terms of use' are described here: https://ctdbase.org/about/legal.jsp

### Data Access Locations
There are two pages for downloading data files.
 - CTD Bulk Downloads: http://ctdbase.org/downloads/  (this page includes file sizes and simple data dictionaries for each download)
 - CTD Catalog: https://ctdbase.org/reports/   (a simple list of files, reports the number of rows in each file)
   
### Provision Mechanisms and Formats
- Mechanism(s): File download.
- Formats: tsv, csv, obo, or xml .gz files.
   
### Releases and Versioning
 - No consistent cadence for releases, but on average there are 1-2 releases each month.
 - Versioning is based on month + year  of the release
 - Releases page / change log: https://ctdbase.org/about/changes/
 - Latest status page: https://ctdbase.org/about/dataStatus.go

----------------

## Ingest Information
    
### Utility 
- CTD is the richest source of manually curated chemical-disease associations, which are an important type of edge for Translator query and reasoning use cases, including treatment predictions, chemical-gene regulation predictions, and pathfinder queries. 

### Scope
This ingest covers curated Chemical to Disease associations that report therapeutic and marker/mechanism relationships. 

  #### Relevant Files:

  | File | Location | Description |
  |----------|----------|----------|
  | CTD_chemicals_diseases.tsv.gz  | http://ctdbase.org/downloads/ |  Manually curated and computationally inferred associations between chemicals and diseases | 
  | CTD_exposure_events.tsv.gz  | http://ctdbase.org/downloads/ |  Descriptions of statistical studies of how exposure to chemicals affects a particular population, with some records providing outcomes| 
  
  #### Included Content:

  | File | Included Content | Columns Used |
  |----------|----------|----------|
  | CTD_chemicals_diseases.tsv.gz  | Curated therapeutic and marker/mechanism associations -i.e. rows where a "DirectEvidence" value is populated with type "T" or "M"  | ChemicalName, ChemicalID, CasRN, DiseaseName, DiseaseID, DirectEvidence, InferenceGeneSymbol, InferenceScore, OmimIDs, PubMedIDs |

  #### Excluded Content:

  | File | Excluded Content | Rationale |
  |----------|----------|----------|
  | CTD_chemicals_diseases.tsv.gz  | Inferred associations - i.e. rows lacking a value in the DirectEvidence column | Decided that the methodology used to create these inferences gave associations that were not strong/meaningful enough to be of use to Translator |
  | CTD_exposure_events.tsv.gz | All | These report chemical-disease association, but mapping to Biolink is not clear. These will likely be included in a future ingest |

  #### Future Considerations:

  | File | Content |  Rationale |
  |----------|----------|----------|
  | CTD_exposure_events.tsv.gz |  Additional chemical-disease edges reporting statistical correlations from environmental exposure studies | This is a unique/novel source for this kind of knowledge, but there is not a lot of data here, and utility is not clear, so it may not be worth it. |

-----------------

##  Target Information

### Infores:
 - infores:ctd-chemical-disease-kgx
   
### Edge Types

| # | Association Type | Subject Category |  Predicate | Object Category | Qualifier Types |  AT / KL  | UI Explanation |
|----------|----------|----------|----------|----------|----------|---------|----------|
| 1 | Chemical To Disease Or Phenotypic Feature Association | Chemical Entity | treats or applied or studied to treat` | Disease Or Phenotypic Feature  |  n/a  |  manual agent, knowledge assertion  | CTD Chemical-Disease records with a "T" (therapeutic) DirectEvidence code indicate the chemical to be a "potential" treatment in virtue of its clinical use or study - which maps best to the Biolink predicate `treats_or_applied_or_studied_to_treat`. |
| 2 | Chemical To Disease Or Phenotypic Feature Association | Chemical Entity | marker or causal for | Disease Or Phenotypic Feature  |  n/a  |  manual agent, knowledge assertion  | CTD Chemical-Disease records with an "M" (marker/mechanism) DirectEvidence code indicate the chemical to correlate with or play an etiological role in a condition - which maps best to the Biolink predicate `marker_or_causal_for`. |

**Rationale**:
1. The `treats_or_applied_or_studied_to_treat` predicate is used to avoid making too strong a claim, as CTDs definition of its "T" flag is broad ("a chemical that has a known or potential therapeutic role in a disease"), which covered cases where a chemical may formally treat a disease or only have been studied or applied to treat a disease. All edges are manual agent knowledge assertions, as the ingested data is based on manual literature curation.
2. The `marker_or_causal_for` predicate is used because the CTD 'M' flag does not distinguish between when a chemical is a correlated marker for a condition, or a contributing cause for the condition. All edges are manual agent knowledge assertions, as the ingested data is based on manual literature curation.
   
### Node Types
High-level Biolink categories of nodes produced from this ingest as assigned by ingestors are listed below - however downstream normalization of node identifiers may result in new/different categories ultimately being assigned.
| Biolink Category |  Source Identifier Type(s) | Notes |
|------------------|----------------------------|--------|
| ChemicalEntity |  MeSH  | Majority are biolink:SmallMolecule |
| DiseaseOrPhenotypicFeature| MeSH | |

------------------

## Ingest Contributors
- **Kevin Schaper**: code author
- **Evan Morris**: code support
- **Sierra Moxon**: code support
- **Vlado Dancik**: code support, domain expertise
- **Matthew Brush**: data modeling, domain expertise

-------------------

## Additional Notes
