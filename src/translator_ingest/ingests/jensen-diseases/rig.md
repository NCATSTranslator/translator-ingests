# Jensen Lab DISEASES Database

---------------

## Source Information

### Infores
 - [infores:diseases](https://w3id.org/information-resource-registry/diseases)

### Description
 
The Diseases database is a web resource that integrates knowledge about on disease-gene associations. It generates de novo associations through automated text mining, and aggregates associations from external sources of manually curated knowledge and GWAS-based study results. The associations are assigned a confidence score to facilitate comparisons across data types and sources.   

### Source Category(ies)
- Primary Knowledge Provider
- Aggregation Provider

### Citation
https://www.sciencedirect.com/science/article/pii/S1046202314003831

### Terms of Use
CC BY 4.0

### Data Access Locations
https://diseases.jensenlab.org/Downloads
   
### Provision Mechanisms and Formats
- Mechanism(s): File download.
- Formats: tsv
   
### Releases and Versioning
 - Updated weekly.  Website offers only download of latest version.
 - Versioned prior releases archived at https://figshare.com/authors/Lars_Juhl_Jensen/96428

----------------

## Ingest Information
    
### Utility
DISEASES text-mined co-occurrrence data is a unique and rich soruce of G2D associations.  Their NLP-based extraction of associations from free text in Medline Plus is also unique and valuable.  However, knoweldge aggregated from soruces like UniProt and TIGA may best be ingested directly from these sources. 

### Scope
This ingest covers text-mined co-occurrence associations, NLP-based extraction of associations from free text in Medline Plus, and manually curated content form AmyCo. Experiment-based associations from TIGA data are not ingested (we will find a diect soruce of GWAS-based associations)

  #### Relevant Files:

  | File | Location | Description |
  |----------|----------|----------|
  | human_disease_textmining_filtered.tsv  | https://diseases.jensenlab.org/Downloads |  Text mined associations, filtered to contain only the non-redundant associations that are shown within the web interface when querying for a gene | 
  | human_disease_knowledge_filtered.tsv  | https://diseases.jensenlab.org/Downloads | Curated assocaitions, filtered to contain only the non-redundant associations that are shown within the web interface when querying for a gene | 
  
  #### Included Content:

  | File | Included Content | Fields Used |
  |----------|----------|----------|
  | human_disease_textmining_filtered.tsv | Only association records from Medline Plus? |   gene_id,	gene_name, disease_id,	disease_name,	z_score?, confidence_score?,	url?  |
  | human_disease_knowledge_full.tsv | All association records? |   gene_id,	gene_name,	disease_id,	disease_name,	source_db,	evidence_type?,	confidence_score?  |

  #### Filtered Records (o):

  | File | Filtered Content | Rationale |
  |----------|----------|----------|
  | human_disease_knowledge_full.tsv | Curated association recrods from UniProt, AmyCo, ...? | Lower quality and/or utility |

  #### Future Considerations (o):

  | File | Content |  Rationale |
  |----------|----------|----------|

  None?

-----------------

##  Target Information

### Infores:
 - infores:jensen-diseases-translator-kgx
   
### Edge Types

| # | Association Type | Subject Category |  Predicate | Object Category | Qualifier Types |  AT / KL  | Edge Properties | UI Explanation |
|----------|----------|----------|----------|----------|----------|---------|----------|---------|
| 1 | GeneToDiseaseAssociation | Gene | cooccurs_in_literature_with | Disease  |  n/a  |  text-mining agent, statistical association | has_confidence, z-score? | TO DO |
| 2 | GeneToDiseaseAssociation | Gene | gene_associated_with_disease | Disease  |  n/a  |  manual_agent, knowledge_assertion  | has_confidence | TO DO |

**Rationale (o)**:


### Node Types

High-level Biolink categories of nodes produced from this ingest as assigned by ingestors are listed below - however downstream normalization of node identifiers may result in new/different categories ultimately being assigned.

| Biolink Category |  Source Identifier Type(s) | Node Properties | Notes |
|------------------|----------------------------|--------|---------|
| Gene | 	ENSEMBL  |  | Source uses the ENSP protein identifiers for Ensembl |
| Disease| DOID |  |  |

------------------

## Ingest Provenance

### Ingest Contributors
- **Colleen Xu**: code author, data modeling
- **Andrew Su**: code support, domain expertise
- **Matthew Brush**: data modeling, domain expertise

### Artifacts(o)
- Github Ticket: https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues/13

### Additional Notes (o)
