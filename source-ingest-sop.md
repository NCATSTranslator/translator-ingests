# Standard Operating Procedure for a Translator Source Ingest
- Below we define general tasks that should be performed and artifacts to be used and created for each source ingest. 
- The present SOP is focused on **re-ingest** of sources included in the prior phase of work - as this is our focus through 2025. But most processes and artifacts apply generally to new ingests as well.
- As written in assumes source data is provisioned via file download, but analogous tasks can be envisioned for data provided through API endpoints, database access, etc.

## Relevant Repositories
A list of Github repositories with content related to ingest tasks.
1. [Data Ingest Coordination Working Group Repo](https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group): No code/model artifacts here. Used only for Issues/Discussions about content and modeling issues related to specific DINGO ingests 
   - **Ingest Tickets**: For discussing specific sources, content, and modeling questions or requests ([issue list](https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues))
   - **Ingest Discussions**: For higher level planning and prioritization discussions ([discussion list](https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/discussions))
2. [Translator Ingests Repo](https://github.com/NCATSTranslator/translator-ingests): Holds code base for source ingests, Resource Ingest Guides (RIGs) for each ingest, and associated developer/user documentation.
   - **Ingest Code and Artifacts**: Code, RIGs, and other artifacts for specifying and executing ingests ([ingests](https://github.com/NCATSTranslator/translator-ingests/tree/add_rigs_back/src/translator_ingest/ingests))
   - **RIG Catalog**: RIGs that describe content, rationale, and modeling for each source ingest ([docs site](https://ncatstranslator.github.io/translator-ingests/src/docs/rig_index/))
   - **Unit Tests for Ingests**: Unit tests validating logic of each source ingest ([tests](https://github.com/NCATSTranslator/translator-ingests/tree/main/tests/unit/ingests))
3. [Resource Ingest Guide Schema Repo](https://github.com/biolink/resource-ingest-guide-schema): Holds a linkML-based schema for authoring RIGs that describe the content, modeling, and rationale for a given ingest.
   - **RIG Schema**: linkML schema for authoring/validating RIG documents ([yaml schema](https://github.com/biolink/resource-ingest-guide-schema/blob/main/src/resource_ingest_guide_schema/schema/resource_ingest_guide_schema.yaml)) ([docs site](https://biolink.github.io/resource-ingest-guide-schema/))
4. [Ingest Metadata Schema Repo](https://github.com/biolink/ingest-metadata): Holds a schema and documentation for KGX ingest-metadata files which accompany 'nodes' and 'edges' files for each ingest execution.
   - **Ingest Metadata Schema**: LinkML-based schema for authoring/validating KGX ingest-metadata files ([yaml schema](https://github.com/biolink/ingest-metadata/blob/main/src/ingest_metadata/schema/ingest_metadata.yaml)) ([docs site](https://biolink.github.io/ingest-metadata/))
5. [Information Resource Registry Repo](https://github.com/biolink/information-resource-registry): holds an information resource ('infores') catalog with identifiers and metadata about resources from which Translator ingests data, and a schema for authoring/validating these records.
   - **Information Resource Registry Schema**: LinkML-based schema for populating resource records ([yaml schema](https://github.com/biolink/information-resource-registry/blob/main/src/information_resource_registry/schema/information_resource_registry.yaml))
   - **Information Resource (Infores) Catalog**: YAML file with entries for each information resource ([yaml catalog](https://github.com/biolink/information-resource-registry/blob/main/infores_catalog.yaml))

## Process and Artifact Overview
Follow the steps and use / generate the artifacts described below, to perform a source ingest according to standard operating procedure.
1. **Ingest Assignment and Tracking** **_(required)_**: Record owner/contributor assignments and track status for each ingest. ([Ingest List](https://docs.google.com/spreadsheets/d/1nbhTsEb-FicBz1w69pnwCyyebq_2L8RNTLnIkGYp1co/edit?gid=506291936#gid=506291936)) 
2. **Ingest Surveys** **_(as needed)_**: Describe past ingests of a source to facilitate comparison and alignment (useful when there are multiple prior ingests). ([Directory](https://drive.google.com/drive/folders/1temEMKNvfMXKkC-6G4ssXG06JXYXY4gT)) ([CTD Example)](https://docs.google.com/spreadsheets/d/1R9z-vywupNrD_3ywuOt_sntcTrNlGmhiUWDXUdkPVpM/edit?gid=0#gid=0)
3. **Resource Ingest Guides (RIGs)** **_(required)_**: Document scope, content, and modeling decisions for an ingest, in a computable yaml format. ([yaml schema](https://github.com/biolink/resource-ingest-guide-schema/blob/main/src/resource_ingest_guide_schema/schema/resource_ingest_guide_schema.yaml)) ([yaml template](https://github.com/NCATSTranslator/translator-ingests/blob/add_rigs_back/src/docs/rig_template.yaml)) ([yaml example](https://github.com/NCATSTranslator/translator-ingests/blob/add_rigs_back/src/translator_ingest/ingests/ctd/ctd_rig.yaml)) ([derived markdown example](https://ncatstranslator.github.io/translator-ingests/rigs/ctd_rig/)) ([full rig catalog](https://ncatstranslator.github.io/translator-ingests/src/docs/rig_index/))
4. **Source Ingest Tickets** **_(as needed)_**: If content or modeling questions arise, create a `source-ingest` ticket in the DINGO repo ([ingest issues](https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues?q=label%3A%22source%20ingest%22))
5. **Ingest Code** **_(required)_**: Author ingest code / artifacts following RIG spec, using shared python code base. ([source code](https://github.com/NCATSTranslator/translator-ingests/tree/main/src/)) ([example](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/ctd/ctd.py))
6. **KGX Files** **_(required)_**: Execute ingest code and normalization services to generate normalized knowledge graphs and ingest metadata artifacts. ([ctd example]() - TO DO)
7. **KGX Summary Reports** **_(under development)_**: Automated scripts generate reports that summarize content of KGX ingest files, to facilitate manual QA/debugging, and provide documentation of KG content and modeling. ([ctd example]() - TO DO)
   
## Detailed Guidance
Specific guidance and considerations for executing a source ingest. 

### 1. Establish / Organize Owners and Contributors
- Determine who will perform and contribute to each ingest. We are currently using the following tables:
   - [Ingest List and Assignments](https://docs.google.com/spreadsheets/d/1nbhTsEb-FicBz1w69pnwCyyebq_2L8RNTLnIkGYp1co/edit?gid=506291936#gid=506291936): Lists sources for ingest, info about each to help prioritize, and columns to assign contributors and track status.
   - [Prior Ingestors](https://docs.google.com/spreadsheets/d/1nbhTsEb-FicBz1w69pnwCyyebq_2L8RNTLnIkGYp1co/edit?gid=1144506947#gid=1144506947): Table with one row per prior ingest of a source - reporting which / how many KPs ingested each source. 

- As appropriate, schedule a Planning Call between all contributors, to collaboratively address/document the questions/tasks below.

### 2. Assess Terms of Use 
- Find and evaluate license / terms of use information for the source.
- Shilpa Sundar will perform this task for initial ingests, and document outcomes in this [License Assessment Spreadsheet](https://docs.google.com/spreadsheets/d/1fsUMFTrLQCKV5-iYflu0rqOsQ6BzV8yE6miomqd7UF0/edit?gid=328401411#gid=328401411)
- Source owners should review outcomes and coordinate with Shilpa as needed.

### 3. Assess Utility
- Once it is determined that we are able to use a source, determine if it is worth ingesting.
- Assess the overall quality/utility of the data, and if there is a better alternative for this type of data/knowledge.
- For prior ingests, consider also findings from the [Translator Results Analysis](https://drive.google.com/drive/folders/1Ugr7rbOogsDz-tsVfIxQ8sSy5X2xGLLk), which quantifies how often edges from a given sources are surfaced in actual query results.
   - Key summary metrics are also included in the [Ingest List](https://docs.google.com/spreadsheets/d/1nbhTsEb-FicBz1w69pnwCyyebq_2L8RNTLnIkGYp1co/edit?gid=506291936#gid=506291936) spreadsheet, although many of these columns are hidden by default.

### 4. Understand and Compare Prior Ingests 
Assuming a source passes **terms of use** and **utility** assessments, proceed with the ingest by reviewing/comparing all prior ingests of a source:

#### Prepare
- Reps for each KP that ingested a source should prepare by reviewing what their KP ingested, from where, how it was transformed to Biolink Associations, and the logic/rationale behind any processing or filtering that was performed.
- We strongly recommend each KP populates an **Ingest Survey Spreadsheet** like the one [here](https://docs.google.com/spreadsheets/d/1R9z-vywupNrD_3ywuOt_sntcTrNlGmhiUWDXUdkPVpM/edit?gid=0#gid=0) for CTD - to help identify and resolve any differences in what and how source data is ingested, filtered, and modeled across KPs. This format will facilitate the 'Compare' task described below.
    - The 'owner' of a given source will create a survey spreadsheet, fill it out for their KP, then reach out to other relevant KPs to review.
    - These KPs may decide that the owner's ingest covers everything they did, in which case nothing else is needed.
    - Or they may determine that the owner's ingest is different in important ways (scope of content, filtering, modeling) - and populate rows describing their ingest that will help surface and resolve these differences.
    - This will likely require a meeting to discuss in real time. 
    
#### Compare
- Compare source files / data versions were ingested by each KP?
- Compare what content from these files was included?
   - Which records were included vs filtered, based on what logic.
   - what columns were used/ingested to create edges, and to provide node or edge metadata.
   - What KL/AT was assigned any why.
- Assess quality / utility of selected content - is there low quality/utility content that could be dropped?  or missed high value content that can be added?     
- How was source data represented using the target Biolink model?
   - Identify any differences between KPs, or divergence from current Translator modeling patterns and principles. 
   - Assess, and resolve differences in representation of same /related content, to align with current best models and practices

### 5. Source Data Exploration
- Programmatic exploration of a source is often performed by developers as part of the ingest process, to understand its content and inform how to best ingest and transform the data.
- This may be performed as needed by lead developers for an ingest, at any stage in the process that they find helpful.
  - Note that key outcomes/conclusions from this work can be documented in a RIG where useful (see below), to explain content or modeling decisions.
  - And key artifacts generated through this process (e.g. Jupyter Notebooks, summary spreadsheets) should be described in and linked to from the RIG.
  
### 6. Document Ingest Decisions in a RIG
RIGs like the one [here](https://github.com/NCATSTranslator/translator-ingests/blob/add_rigs_back/src/translator_ingest/ingests/ctd/ctd_rig.yaml) document these scope, content, and modeling decisions for a source ingest.
A template for authoring new yaml RIGs can be found [here](https://github.com/NCATSTranslator/translator-ingests/blob/add_rigs_back/src/docs/rig_template.yaml)), which follows [this schema](https://github.com/biolink/resource-ingest-guide-schema/blob/main/src/resource_ingest_guide_schema/schema/resource_ingest_guide_schema.yaml).

RIGs are used to:

#### Describe Content Decisions
- Document final decisions on what content will be included in the initial re-ingest
    - What knowledge (edges), concepts (nodes) are of high enough quality and utility to include in the ingest?
    - Remember to consider ingest of nodes not participating in ingested edges - if there are valuable node properties provided for them.
- Consider what node/edge metadata are of high enough quality and utility to include in the ingest?
    - Asses EPC use cases in particular, and remember to carefully assess and include KL/AT annotations.
   -  UI and O&O are also keenly interested in including Evidence Type annotations on all edges possible. These may be provided by a source (e.g. GOA), or as added by ingestors based on our understanding of the source
- Note what records are filtered out and why?

#### Describe Modeling Decisions
- Describe types of Biolink edges that will be created (define SPOQ patterns to be used) - and capture rationale for this representation*
- Describe node/edge properties and patterns that will be used to capture metadata?
- Consider any re-modeling of prior patterns needed, and note any required Biolink changes/additions. 

#### Describe Content/Modeling to Reconsider in Future Iterations
- Be sure to document any content left behind that should be revisited/ingested in future iterations*.
- Document areas where we might consider refactored modeling in the future - so we can return to and update the ingest. 

### 7. Create Ingest Tickets as Necessary
- If content or modeling issues arise that require public discussion or documentation, create a ticket in the [DINGO repository](https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues/).
- Be sure to add a `source-ingest` label to the issue.

### 8. Write Ingest Code
- Follow specification documented in the RIG . . . 
- Implementation details of the shared pipeline are still t.b.d. 

### 9. Execute Ingests to Produce KGX Files
- Create nodes, edges, ingest-metadata KGX files
- t.b.d. where these will be stored
