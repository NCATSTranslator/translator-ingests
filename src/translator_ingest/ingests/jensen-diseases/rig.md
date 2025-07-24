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
DISEASES contains gene-disease associations from unique sources, including their own text-mining pipeline and external human-curated resources that are hard to access or parse (MedlinePlus, AmyCo). These associations could be used in MVP1 ("may treat disease X") or Pathfinder queries.

### Scope
This ingest covers text-mined co-occurrence associations, and manually curated associations Medline Plus and AmyCo. Content aggregatd from UniProt is not ingested. Experiment-based associations from TIGA data are not ingested (we will find a direct source of GWAS-based associations).

  #### Relevant Files:

  | File | Location | Description |
  |----------|----------|----------|
  | human_disease_textmining_filtered.tsv  | https://diseases.jensenlab.org/Downloads |  Text mined associations, filtered to contain only the non-redundant associations that are shown within the web interface when querying for a gene | 
  | human_disease_knowledge_filtered.tsv  | https://diseases.jensenlab.org/Downloads | Curated assocaitions, filtered to contain only the non-redundant associations that are shown within the web interface when querying for a gene | 
  
  #### Included Content:

  | File | Included Content | Fields Used |
  |----------|----------|----------|
  | human_disease_knowledge_full.tsv | All association records from Medline Plus and AmyCo |   gene_id,	gene_name, disease_id,	disease_name,	z_score, confidence_score,	url?  |
  | human_disease_textmining_filtered.tsv | All association records |   gene_id,	gene_name,	disease_id,	disease_name,	source_db,	evidence_type?,	confidence_score |

  #### Filtered Records (o):

  | File | Filtered Content | Rationale |
  |----------|----------|----------|
  | human_disease_knowledge_full.tsv | Curated association records from UniProt | Questionable quality and completeness in DISEASES - best to get this content directly from UniProt.|

  #### Future Considerations (o):

  | File | Content |  Rationale |
  |----------|----------|----------|

- Revisit modeling of confidence score and z-score if/when we refactor these parts of the Biolink Model. 

-----------------

##  Target Information

### Infores:
 - infores:translator-jensen-diseases-kgx
   
### Edge Types

| # | Association Type | Subject Category |  Predicate | Object Category | Qualifier Types |  AT / KL  | Edge Properties | UI Explanation |
|----------|----------|----------|----------|----------|----------|---------|----------|---------|
| 1 | Association | Gene, Protein | occurs_together_in_literature_with | Disease  |  n/a  |  text-mining agent, statistical association | has_confidence_score, z-score, original_subject, original_object | DISEASES text-mining method is based on statistically significant co-occurrence of gene and disease concepts in the literature - which is captured by the Biolink occurs_together_in_literature_withpredicate. |
| 2 | Association | Gene, Protein | associated_with | Disease |  n/a  |  manual_agent, knowledge_assertion  | has_confidence |  DISEASES does to attempt to report more specific types of gene-disease relationships that it aggregates from curated soruces, so we can only report that they are associated_with each other in some way. |

**Notes/Rationale (o)**:


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
