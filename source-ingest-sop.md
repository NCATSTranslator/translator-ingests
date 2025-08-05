# Standard Operating Procedure for a Source Ingest
- Below we define general tasks that should be performed and artifacts to be created for each source ingest. 
- The present SOP is focused on **re-ingest** of soruces included in Phase 2 work - as this is our foucs through 2025.
- As written in assumes source data is provisioned via file download, but analogous tasks can be envisioned for data provided through API endpoints, database access, etc.

## Key Artifacts
1. **Ingest Assignment Table**: Records owner and contributor assignments for each ingest ([link](https://docs.google.com/spreadsheets/d/1nbhTsEb-FicBz1w69pnwCyyebq_2L8RNTLnIkGYp1co/edit?gid=1969427496#gid=1969427496))
2. **Source Ingest Tickets**: Tracks contributor questions and discussions about the ingest ([CTD Example](https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues?q=state%3Aopen%20label%3A%22source%20ingest%22))
3. **Ingest Surveys**: Describe any current ingests of the source from Phase 2 KPs to facilaitate comparison and alignment. ([Examples)](https://docs.google.com/spreadsheets/d/1R9z-vywupNrD_3ywuOt_sntcTrNlGmhiUWDXUdkPVpM/edit?gid=0#gid=0)
4. **Reference Ingest Guides (RIGs)**: Document scope, content, and modeling decisions for an ingest. ([CTD Example](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/ctd/rig.md)) ([Instructions](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/rig-instructions.md)) ([Template](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/_ingest_template/rig-template.md))
5. **Ingst Code**: Python code used to execute an ingest as described in a RIG. ([CTD Example](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/ctd/ctd.py))
6. **KGX Files**: The final knowledge graphs and ingest metadata that is produced by ingest code. ([CTD Example]() - TO DO)

## Task Details

### 1. Establish / Organize Owners and Contributors
- Determine who will perform and contribute to each ingest. We are currently using the **Ingest Assignment Table** [here](https://docs.google.com/spreadsheets/d/1nbhTsEb-FicBz1w69pnwCyyebq_2L8RNTLnIkGYp1co/edit?gid=1969427496#gid=1969427496).
- As appropriate, schedule a Planning Call for all contributors to collaboratively address/document the questions/tasks below.

### 2. Create a Source Ingest Ticket in DINGO Repository
- This ticket will capture questions and discussion related to ingest of a specific source - including assessment of terms of use, content and modeling issues, and notes for future iterations of the ingest. 
- To create a new ticket, copy content from an [existing source ingest ticket](https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues?q=state%3Aopen%20label%3A%22source%20ingest%22) and overwrite with info for the new source. Be sure to add a "source ingest" label.

### 3. Assess Terms of Use 
- Find and evaluate license / terms of use information for the source, and document this info and our assessment in the Source Ingest Ticket as described above. 
- See [existing tickets](https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues?q=state%3Aopen%20label%3A%22source%20ingest%22) for examples.

### 4. Assess Utility
- Is the source as a whole worth ingesting?... perhaps not if overall quality/utility is low, or there is a better alternative for this type of data?
- Consider findings from the 'Translator Results Analysis' that quantifies how often different types of edges from different sources are surfaced in actual query results.

### 5. Understand and Compare Phase 2 Ingests 
Assuming a source passes terms of use and utility assessments, proceed with the ingest. 

#### Prepare
- Reps for each KP that ingested a source in Phase 2 should prepare by reviewing what their KP ingested, from where, how it was transformed to Biolink Associations, and the logic/rationale behind any processing or filtering that was performed.
- In particular for sources ingested by multiple KPs, we strongly recommend each KP populates an **Ingest Survey Spreadsheet** like the one [here](https://docs.google.com/spreadsheets/d/1R9z-vywupNrD_3ywuOt_sntcTrNlGmhiUWDXUdkPVpM/edit?gid=0#gid=0) like the one for CTD linked above - to help identify and resolve any differneces in what and how source data is ingested, filtered, and modeled across KPs. This format will facilitate the 'Compare' task described below.
    
#### Compare
- What KPs ingested the source in Phase 2?
- Compare source files / data versions were ingested by each KP?
- Compare what content from these files was included?
   - Which records were included vs filtered, based on what logic.
   - what columns were used/ingested to create edges, and to provide node or edge metadata.
   - What KL/AT was assigned any why.
- Assess quality / utility of selected content - is there low quality/utility content that could be dropped?  or missed high value content that can be added?     
- How was source data represented using the target Biolink model
   - Assess modeling generally in light of current state of Biolink model and approaches
   - If >1 KP, identify, assess, and resolve differences in representation of same /related content

### 6. Source Data Exploration
- Programmatic exploration of a source is often perfomred by developers as part of the ingest process, to understand its content and inform how to best ingest and transform the data.
- This may be performed as needed by lead develoeprs for an ingest.
  - Note that key outcomes/conclusions from this work can be documented in a RIG where useful (see below), to explain content or modeling decisions.
  - And key artifacts genrated through this process (e.g. Jupyter Notebooks, summary sprreadshets can be described in and linked to from the RIG.
  
### 7. Document Ingest Decisions in a RIG
RIGs like the one [here](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/ctd/rig.md) document these scope, content, and modeling decisions for a source ingest.

#### Describe Content Decisions
- Document final decisions on what content will be included in the initial re-ingest
    - What knowledge (edges), concepts (nodes) are of high enough quality and utility to include in the ingest?
    - Remember to consider ingest of nodes not participating in ingested edges - if there are valueable node properties provided for them.
- Consider what node/edge metadata are of high enough quality and utility to include in the ingest?
    - Asses EPC use cases in particular, and remember to carefully assess and include KL/AT annotations.
   -  UI and O&O are also keenly interested in including Evidence Type annotations on all edges possible. These may be provided by a source (e.g. GOA), or as added by ingestors based on our understanding of the source
- Note what records are filtered out and why?

#### Describe Modeling Decisions
- Describe types of Biolink edges will be created (define SPOQ patterns to be used) - and capture rationale for this representation*
- Describe node/edge properties and patterns that will be used to capture metadata?
- Consider any re-modeling of Phase 2 patterns needed, and note any Biolink changes/additions needed. 

#### Describe Content/Modeling to Reconsdier in Future Iterations
- Be sure to document any content left behind that should be revisited/ingested in future iterations*.
- Document areas wehre we might consider refactored modeling in the future - so we can return to and update the ingest. 


### 8. Write Ingest Code
- Follow specification documented in the RIG . . . 
- Implementation details of the shared pipeline are still t.b.d. 

### 9. Execute Ingests to Produce KGX Files
- Create nodes, edges, ingest-metadata KGX files
- t.b.d. where these will be stored
