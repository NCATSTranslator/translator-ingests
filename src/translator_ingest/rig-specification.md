# Instructions for Populating Reference Ingest Guides (RIGs)
- RIGs document the scope, rationale, and modeling for a single source ingest. 
- Below we define and provide guidance for populating each section of a RIG. 
- Optional sections are marked with an "(o)".

An informative example of a completed RIG for the Comparative Toxicogenomics Database (CTD) ingest can be found [here](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/ctd/rig.md).  

New RIGs can be created by copying and overwriting the markdown content of this CTD RIG in a new document. 

---------------

## Section I: Source Information

### Infores
The infores identifier of the source from which content is being ingested. Ideally, the value of this field will have a link pointing to the corresponding Infores Registry entry, e.g. [CTD](https://w3id.org/information-resource-registry/ctd) for the Comparative Toxicogenomics Database infores.
  
### Description (o)
(Optional) A short description of the source, focused on info relevant to its ingest into Translator.

### Source Category(ies)
One or more of the following categories, describing the kind of source being ingested:

  | Category | Description |
  |----------|-------------|
  | **Primary Knowledge Provider**| Provides knowledge that is curated/created/mined by the source (e.g. via literature curation/interpretation, analysis of datasets, inference over evidence) |
  | **Aggregation Provider** | Aggregates knowledge from external sources and provides as is - with minimal alteration/interpretation of what the primary source reported. (e.g. Monarch, Diseases, Pharos) |
  | **Aggregation Interpreter** | Aggregates knowledge and assesses/interprets it as evidence to draw and report its own conclusion (e.g. DGIdb, DisGenNet) |
  | **Supporting Data Provider** | Provides information (data or prior facts) that is subsequently analyzed / interpreted by a Translator tool to derive new knowledge (e.g. TCGA, GTex, Carolina Data Warehouse) |
  | **Translator Knowledge Creator** | A Translator tool that generates de novo knowledge from statistical analysis, interpretation, or reasoning with more foundational data/ or facts (e.g. ICEES, COHD, Multiomics Wellness, BigGIM) |
  | **Ontology/Terminology Provider** | An ontology or terminology artifact providing concept identifiers, definitions, mappings, hierarchical relationships that are ingested into Translator KGs |
  | **Node Property Only Provider** | Provides only information that is used to annotate nodes of a particular kind in Translator graphs |
  | **Other/Unknown** | Used when the information provided by a source is not known, or does not fit into the defined categories above |

### Citation (o)
(Optional) Literature citation(s) officially/originally publishing or describing the source.

### Terms of Use
Information about the conditions for use of the ingested source - minimally the name of a community license type (e.g. `CC-BY 4.0`), and/or a link to a "terms of use" or license information web page (e.g. `https://ctdbase.org/about/legal.jsp`).

### Data Access Locations
URL(s) where the source data that is being ingested can be accessed - (and optionally documentation about the data and how to understand/use it (e.g. data dictionaries. data models).
   
### Provision Mechanisms and Formats
How the source distributes their data (file download, API endpoints, database), and a brief description of each (e.g. formats, scope of data provided, other considerations). As possible, provide links to  documentation about the data and how to understand/use it (e.g. data dictionaries, data models).
   
### Releases and Versioning
Description of how releases are managed by the source (e.g. general approach, frequency, any important considerations) - and/or links to web pages describing such information. 


----------------

## Section II. Ingest Information
    
### Utility 
Brief description of why we ingest this source, and the utility of the data it provides for Translator use cases. 

### Scope
A high-level summary of the types of knowledge that is and is not included in this ingest.


### Relevant Files
Source files (or API endpoints or database tables) that contain content we aim to ingest.

  | File / Endpoint / Table | Location | Description |
  |----------|----------|----------|
  |  |  |  |
  |  |  |  |  

  
### Included Content / Records
Records from relevant files/endpoints/tables above that are included in this ingest, and optionally a list of fields in the data that are part of or inform the ingest. 

  | File | Included Records | Fields Used (o) | 
  |----------|----------|----------|
  |  |  |  |
  |  |  |  |  


### Filtered Content / Records
Records from relevant files that are not included in the ingest, with the rationale for any filtering rules or exclusion criteria.

If no content/records are filtered, a simple statement to this effect is sufficient (no need for a table).

Note that there is no need to list source files from which no content was ingested, or the fields from ingested files that were not used.  

  | File | Filtered Records | Rationale |
  |----------|----------|----------|
  |  |  |  |
  |  |  |  |

### Future Content Considerations (o) 
(Optional) Notes about content additions or changes to consider in future iterations of this ingest. Specifically, consdier Edge content, Node Property content, and Edge Property / EPC content.  

- **Edges**
   - ...

- **Node Properties**
  - ...
    
- **Edge Properties/EPC Metadata**
  - ...
    
-----------------

##  Section III. Target Information

### Infores:
The infores identifier assigned to the Translator resource that will be created from the ingested content. 
   
### Edge Types
A table describing the types of edges created in the KG produced by this ingest. Provides the Biolink Association type, a meta-edge representation, KL/AT assigned to each edge type, edge properties included, and a brief explanation of why the modeling pattern/predicate was deemed appropriate to represent the source data - for the UI to consume and display to end users.
If there are not any qualifiers and/or edge properties to report, simply indicate 'n/a' or 'none'.

| Subject Category | Predicate | Object Category | Qualifier Types (o) | AT / KL | Edge Properties | UI Explanation |
|------------------|-----------|-----------------|---------------------|---------|-----------------|----------------|
|                  |           |                 |                     |         |                 |                |
|                  |           |                 |                     |         |                 |                |

**Additional Notes/Rationale (o)**:
(Optional)  Additional information about the modeling/mapping rationale for a specific edge type in the table above.
   
### Node Types
A table describing the high-level Biolink categories of nodes produced from this ingest as assigned by ingestors. Note however that downstream normalization of node identifiers may result in new/different categories ultimately being assigned.
| Biolink Category |  Source Identifier Type(s) | Node Properties  |  Notes (o) |
|------------------|----------------------------|------------------|--------|
|  |  |  |  |
|  |  |  |  |

### Future Modeling Considerations (o) 
Notes about mapping/modeling changes to consider in future iterations of this ingest. 
  
------------------

## Section IV: Provenance Information (o)

### Ingest Contributors (o)
(Optional) A list of people who contributed to this ingest and the role they played. 
Use the following terms to describe roles. 

| Role Term | Description |
|------|-------------|
| **code author** |  writing of code to perfom this ingest |
| **code support** | provision of advice or example code that informed the writing of the final ingest code |
| **domain expertise** | provision of expertise / advice in the subject area relevant to the source |
| **data modeling** | drafting of models and mapping/transform definitions that were implemented by ingest code |

### Artifacts (o)
(Optional) Links to external artifacts withmore information about / discussion of this ingest. 

### Additional Notes (o)
(Optional)  Additional considerations or notes about why/how data was ingested, transformed, annotated during the ingest process.
