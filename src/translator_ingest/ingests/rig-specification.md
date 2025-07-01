# Guidance for Populating Reference Ingest Guides
Populate each section defiend below as relevant/possible. Include in each section any links to source documentation relevant to the topic it describes.

# Section Definitions 

**Source Infores**: The infores of the source. Note that this may be a "sub-resource" infores (e.g. `infores:ctd-chem-disease-subset`)

**Source Description and Licensing**: A short description of the source focused on info relevant to this ingest, including any notes or links about licensing or terms of use.

**Source Utility to Translator:** Description of the general rationale for ingesting selected content from this source, and its utility for Translator use cases

**Source Data Access Locations:** Any urls where source data that was ingested can be accessed - (and any documentation about the data and how to understand/use it (e.g. data dictionaries. data models).

**Source Provision Mechanism(s) and Formats:** How the source distributes their data  (file download, API endpoints, database), and a brief description of each (e.g. formats, scope of data provided, other considerations) 

**Source Releases and Versioning:** Description of how releases are managed by the source (e.g. general approach, frequency, any important considerations) 

**Ingest Scope:** Define the subset of data from the source that is included in this ingest.
  - **Relevant Files:** files provided by the source that include data relevant to the defined ingest scope. 
  - **Included:** details of what data was ingested from these files (or used to inform ingest/transformation) - in terms of files and columns for tabular data.
  - **Excluded:** what data was not ingested, or filtered out during processing. As possible, describe the rationale behind any filtering rules or exclusion criteria.
  - **Future Considerations:** notes about data that was excluded but may be useful to take in future ingests.

**Ingested Edge Info:** List the types of edges created in our KG (Biolink Association type and meta-edge representation. As possible, include a brief rationale for why the modeling pattern/predicate was deemed appropriate to represent the source data.

**Ingested Node Into:** List the types of nodes that were created in the target KG (in terms of final/mapped Biolink category).  As possible, indicate the identifier type(s) used by the source for each category. 

**Source Quality/Confidence Assessment:** Report any Translator-defined quality or confidence metrics / considerations that apply to the data generally, or specific types of edges. e.g. “we chose ‘causes’, but are only 90% sure this is true for any edge given how source data was generated”

**Misc Notes:** Other considerations or notes about why/how data was ingested, transformed, annotated during the ingest process (e.g. rationale for KL/AT assignments, evidence type assignments, etc.)

----------

# Guiding Examples
- [CTD RIG](https://github.com/NCATSTranslator/translator-ingests/edit/mbrush-patch-1/src/translator_ingest/ingests/ctd/rig.md)


---------
  
# Template


# [Source Name] Reference Ingest Guide (RIG)


## Source Infores: 


## Source Description and Licensing


## Source Utility to Translator


## Source Data Access Locations


## Source Provision Mechanism(s) and Formats
- Mechanism: 
- Formats: 
   
## Source Releases and Versioning

## Ingest Scope

  ### Relevant Files:

  | File | Description |
  |----------|----------|
  |          |          |
  
  ### Included:

  | File | Included Content | Columns |
  |----------|----------|----------|
  |          |          |          |

  ### Excluded:

  | File | Excluded Content | Rationale  |
  |----------|----------|----------|
  |          |          |          |
 

  ### Future Considerations:

  | File | Rationale |
  |----------|----------|
  |          |          |
  
## Ingested Edge Info

| No. | Association Type | MetaEdge | Qualifiers |  AT / KL  | Evidence Code  |
|----------|----------|----------|----------|----------|----------|
|          |          |          |          |          |          |

**Rationale**:
1. ... (match number to row number in table above)
2. ...

   
## Ingested Node Info
- Biolink Category (source id type)

## Source Quality/Confidence Assessment


## Misc Notes

