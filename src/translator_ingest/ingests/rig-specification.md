# Guidance for Populating Reference Ingest Guides
Populate each section defiend below as relevant/possible. Include in each section any links to source documentation relevant to the topic it describes.

# Section Definitions 

**Source Description**: The infores of the source, and a short description providing info relevant to this ingest. Note that this may be a "sub-resource" infores (e.g. `infores:ctd-chem-disease-subset`)

**Source Utility to Translator:** description of the general rationale for ingesting selected content from this source, and its utility for Translator use cases

**Data Access Locations:** urls where data ingested can be accessed - (and any documentation about the data and how to understand/use it (e.g. data dictionaries. data models).

**Provision Mechanism(s) and Formats:** how the source distributes their data  (file download, API endpoints, database), and a brief description of each (e.g. formats, scope of data provided, other considerations) 

**Releases and Versioning:** description of how releases are managed (general approach, frequency, any important considerations) 

**Ingest Scope:** Define the subset of data form the source that was included in this ingest.
  - **Included:** Details of what data was ingested (or used to inform ingest/transformation) - in terms of files and columns for tabular data
  - **Excluded:** what data was not ingested or filtered out during processing. As possible, describe the rationalebehindany filtering rules or exclusion criteria.
  - **Future Considerations:** note data that was excluded but may be useful to take in future ingests.

**Ingested Edge Types:** List the types of edges created in our KG (Biolink Association type and meta-edge representation. As possible, include a brief rationale for why the modeling pattern/predicate was deemed appropriate to represent the source data.

**Ingested Node Types:** List the types of nodes were created in our KG, in terms of Biolink category, as well as identifier types used by the source.

**Source Quality/Confidence Assessment:** Report any Translator-defined quality or confidence metrics / considerations that apply to the data generally, or specific types of edges. e.g. “we chose ‘causes’, but are only 90% sure this is true for any edge given how source data was generated”

**Misc Notes:** Other considerations or notes about why/how data was ingested, transformed, annotated during the ingest process (e.g. rationale for KL/AT assignments, evidence type assignments, etc.)

----------

# Guiding Examples
- [CTD RIG](https://github.com/NCATSTranslator/translator-ingests/edit/mbrush-patch-1/src/translator_ingest/ingests/ctd/rig.md)


---------
  
# Template


# [Source Name] Reference Ingest Guide (RIG)

## Source Description and Licensing
 - ...

## Source Utility to Translator
- ...

## Data Access Locations
There are two pages for downloading data files.
 - ...
   
## Provision Mechanism(s) and Formats
- ...
   
## Releases and Versioning
 - ...

## Ingest Scope
 - ...

  ### Included:

  | File | Rows | Columns |
  |----------|----------|----------|
  |          |          |          |

  ### Excluded:

  | File | Excluded Subset | Rationale  |
  |----------|----------|----------|
  |          |          |          |
 

  ### Future Ingest Considerations:

  | File | Rationale |
  |----------|----------|
  |          |          |

## Ingested Edge Types

| No. | Association Type | MetaEdge | Qualifiers |  AT / KL  | Evidence Code  |
|----------|----------|----------|----------|----------|----------|
|          |          |          |          |          |          |

**Rationale**:
1. ... (match number to row number in table above)
2. ...

   
## Ingested Node Types
-  ...
-  ...

## Source Quality/Confidence Assessment
- ...

## Misc Notes
- ...

