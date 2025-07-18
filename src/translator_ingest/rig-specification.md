# Instructions for Populating Reference Ingest Guides (RIGs)
Complete the sections described below to document the scope and rationale of a source ingest. Optional sections are marked with an "(o)".
An example of a completed RIG for the Comparative Toxicogenomics Database (CTD) ingest can be found here. 

---------------

## Section I: Source Information

### Infores
The infores identifier of the source from which content is being ingested. Ideally, the value of this field will have a link pointing to the corresponding Infores Registry entry, e.g. [CTD](https://w3id.org/information-resource-registry/ctd) for the Comparative Toxicogenomics Database infores.
  
### Description
A short description of the source, focused on info relevant to its ingest into Translator.

### Source Category(ies)
One or more of the following categories, describing the kind of source being ingested:

  | Category | Description |
  |----------|-------------|
  |**Primary Knowledge Provider**| Provides knowledge that is curated/created/mined by the source (e.g. via literature curation/interpretation, analysis of datasets, inference over evidence) |
  | **Aggregation Provider** | Aggregates knowledge from external sources and provides as is - with minimal alteration/interpretation of what the primary source reported. (e.g. Monarch, Diseases, Pharos) |
  | **Aggregation Interpreter** | Aggregates knowledge and assesses/interprets it to draw and report its own conclusion (e.g. DGIdb, DisGenNet) |
  | **Supporting Data Provider** | Provides information (data or prior facts) that is subsequently analyzed / interpreted by a Translator tool to derive new knowledge (e.g. TCGA, GTex, Carolina Data Warehouse) |
  | **Translator Knowledge Creator** | A Translator tool that generates de novo knowledge from statistical analysis, interpretation, or reasoning with more foundational data/ or facts (e.g. ICEES, COHD, Multiomics Wellness, BigGIM) |
  | **Ontology/Terminology Provider** | An ontology or terminology artifact providing concept identifiers, definitions, mappings, hierarchical relationships that are ingested into Translator KGs |
  | **Node Properties Only Provider** | Provides only information that is used to annotate nodes of a particular kind in Translator graphs |
  | **Other/Unknown** | Used when the information provided by a source is not known, or does not fit into the defined categories above |

### Citation
(Optional) literature citation(s) officially/originally publishing or describing the source.

### Terms of Use
Information about the conditions for use of the ingested source - may be a written description, the name of a community license type (e.g. `CC-BY`), and/or a link to a "terms of use" or license information web page (e.g. `https://ctdbase.org/about/legal.jsp`)

### Data Access Locations
Any urls where the source data that is being ingested can be accessed - (and any documentation about the data and how to understand/use it (e.g. data dictionaries. data models).
   
### Provision Mechanisms and Formats
How the source distributes their data (file download, API endpoints, database), and a brief description of each (e.g. formats, scope of data provided, other considerations). As possible, provide links to  documentation about the data and how to understand/use it (e.g. data dictionaries, data models).
   
### Releases and Versioning
Description of how releases are managed by the source (e.g. general approach, frequency, any important considerations) - and/or links to web pages describing such information. 


----------------

## Section II. Ingest Information
    
### Utility 
Description of the general rationale for ingesting selected content from this source, and its utility for Translator use cases. 

### Scope
Describe and define the subset of data from the source that is included in this ingest.

  #### Relevant Files:
  A table listing all files (or API endpoints or database tables) that contain ingested or pertinent content to the defined scope of ingest, with (markdown clickable) url of each and a brief description.
  | File / Endpoint / Table | Description |
  |----------|----------|
  | **`["file/endpoint/table name"]("url")`** |  | 
  |  |  | 

  
  #### Included Content:
  A table describing the content that is ingested from each relevant file/endpoint/table. 
  | File | Included Content | Fields Used |
  |----------|----------|----------|
  |  |  |  | 
  |  |  |  | 

  #### Filtered Records (o):
  A table describing records that are filtered out during processing/ingest of the source. As possible, describe the rationale behind any filtering rules or exclusion criteria.
  Note that there is no need to list all files from which no content was ingested, or the fields from ingested files that were not used.  
  The goal here is to indicate if some subset of records *from ingested files* were filtered out, and why (e.g. lack of relevance, low quality data, deferred to future ingest, etc). 
  | File | Excluded Content | Rationale |
  |----------|----------|----------|
  |  |  |  |
  |  |  |  | 

  #### Future Considerations (o):
  A table describing data that was excluded but may be useful to include in future ingests.
  | File | Content |  Rationale |
  |----------|----------|----------|
  |  |  |  |
  |  |  |  | 
  
-----------------

##  Section III. Target Information

### Infores:
The infores identifier assigned to the Translator resource that will be created from the ingested content. 
   
### Edge Types
A table describing the types of edges created in the KG produced by this ingest. Provides the Biolink Association type, a meta-edge representation, KL/AT assigned to each edge type, and a brief explanation of why the modeling pattern/predicate was deemed appropriate to represent the source data - for the UI to consume and display to end users.

| # | Association Type | Subject Category | Predicate | Object Category | Qualifier Types | Other Edge Properties | AT / KL | UI Explanation |
|---|------------------|------------------|-----------|-----------------|-----------------|-----------------------|---------|----------------|
|   |                  |                  |           |                 |                 |                       |         |                |

**Rationale (o)**:
Optional additional information about the modeling/mapping rationale for a specific edge type in the table above (can use the # for a given entry in the table to reference/further describe it here).
   
### Node Types
A table describing the high-level Biolink categories of nodes produced from this ingest as assigned by ingestors. Note however that downstream normalization of node identifiers may result in new/different categories ultimately being assigned.
| Biolink Category |  Source Identifier Type(s) | Notes (o) |
|------------------|----------------------------|--------|
|  |  |  |
|  |  |  |

------------------

## Section IV: Ingest Contributors
A list of people who contributed to this ingest and the role they played. Use the following terms to describe roles. 

| Role | Description |
|------|-------------|
| **code author** |  writing of code to perfom this ingest |
| **code support** | provision of advice or example code that informed the writing of the final ingest code |
| **domain expertise** | provision of expertise / advice in the subject area relevant to the source |
| **data modeling** | drafting of models and mapping/transform definitions that were implemented by ingest code |

-------------------

## Section V: Additional Notes (o)
Optional, additional considerations or notes about why/how data was ingested, transformed, annotated during the ingest process.
