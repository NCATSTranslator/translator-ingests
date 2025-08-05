# Standard Operating Procedure for a Source Ingest
Below we define general tasks that should be performed and artifacts to be created for each source ingest. It assumes the source data is provisioned via file download, but analogous tasks can be envisioned for data provided through API endpoints, database access, etc.

## Key Artifacts
1. **Ingest Assignment Table**: Records owner and contributor assignments for each ingest ([link](https://docs.google.com/spreadsheets/d/1nbhTsEb-FicBz1w69pnwCyyebq_2L8RNTLnIkGYp1co/edit?gid=1969427496#gid=1969427496))
2. **Source Ingest Tickets**: Tracks contributor questions and discussions about the ingest ([CTD Example](https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues?q=state%3Aopen%20label%3A%22source%20ingest%22))
3. **Ingest Surveys**: Describe any current ingests of the source from Phase 2 KPs to facilaitate comparison and alignment. ([Examples)](https://docs.google.com/spreadsheets/d/1R9z-vywupNrD_3ywuOt_sntcTrNlGmhiUWDXUdkPVpM/edit?gid=0#gid=0)
4. **Reference Ingest Guides (RIGs)**: Document scope, content, and modeling decisions for an ingest. ([CTD Example](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/ctd/rig.md)) ([Instructions](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/rig-instructions.md)) ([Template](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/_ingest_template/rig-template.md))
5. **Ingst Code**: Python code used to execute an ingest as described in a RIG. ([CTD Example](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/ctd/ctd.py))
6. **KGX Files**: The final knowledge graphs and ingest metadata that is produced by ingest code. ([CTD Example]() - TO DO)

## Task Details

### 1. Establish / Organize Owners and Contributors
Determine who will perform and contribute to each ingest. We are currently using the **Ingest Assignment Table** [here](https://docs.google.com/spreadsheets/d/1nbhTsEb-FicBz1w69pnwCyyebq_2L8RNTLnIkGYp1co/edit?gid=1969427496#gid=1969427496).

As appropriate, schedule a Planning Call for all contributors to collaboratively address/document the questions/tasks below.

### 2. Create a Source Ingest Ticket in DINGO Repository
This ticket will capture questions and discussion related to ingest of a specific source - including assessment of terms of use, content and modeling issues, and notes for future iterations of the ingest. 

Simply copy content from an [existing source ingest ticket](https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues?q=state%3Aopen%20label%3A%22source%20ingest%22) and overwrite with info for the new source. Be sure to add a "source ingest" label.

### 3. Assess Terms of Use 
Find and evaluate license / terms of use information for the source, and document this info and our assessment in the Source Ingest Ticket as described above. 

See existing tickets for examples.

### 3. Understand and Compare Phase 2 Ingests 
Create an **Ingest Survey Spreadsheet** like the one [here](https://docs.google.com/spreadsheets/d/1R9z-vywupNrD_3ywuOt_sntcTrNlGmhiUWDXUdkPVpM/edit?gid=0#gid=0).

#### Prepare
- Reps for each KP that ingested a source should prepare by reviewing what their KP ingested, from where, how it was transformed to Biolink Associations, and the logic/rationale behind any processing or filtering that was performed.
- As appropriate (in particular for sources ingested by multiple KPs), we strongly recommend each KP populates an Ingest Survey Spreadsheet like the one for CTD linked above - to help identify and resolve any differneces in what and how source data is ingested, filtered, and modeled across KPs. This format will facilitate the 'Compare' task described below. 
    
#### Compare
- What teams/KPs ingested the source?
- What source files / versions were ingested by each KP (alt, which database tables, API endpoints, etc)?
- What content from these files was included  (e.g.  specific columns)
   - Separately consider which columns/fields were used to create edges, which were used to create node/edge metadata, and what additional annotations may have been added by the KP (e.g. KL/AT)
   - Assess quality / utility of selected content - is there low quality/utility content that could be dropped?  or missed high value content that can be added?     
- What edges/nodes were filtered out during each ingest/transform, and why?
   - If >1 KP, assess differences in logic applied to same content across existing KP ingests
- How was source data represented using the target Biolink model
   - Assess modeling generally in light of current state of Biolink model and approaches
   - If >1 KP, identify, assess, and resolve  differences in representation of same /related content

### 5. Scope and Specify Re-Ingest Details
Create a **Reference Ingest Guide (RIG)** like the one [here](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/ctd/rig.md).

#### Assess Source Utility
- Is the source as a whole worth ingesting?
- Perhaps not if overall quality/utility is low, or there is a better alternative for this type of data?
- Consider findings from the 'Translator Results Analysis' that quantifies how often different types of edges from different sources are surfaced in query results (as answer edges, or support path edges)

#### Select Content to Ingest
- Make final decisions on what content will be included in the initial re-ingest
    - What knowledge (edges), concepts (nodes) are of high enough quality and utility to include in the ingest?*
    - Remember to consider ingest of nodes not participating in ingested edges
- What node/edge metadata are of high enough quality and utility to include in the ingest?*
    - Consider EPC use cases in particular
    - Remember to carefully assess and include KL/AT annotations.
   -  UI and O&O are also keenly interested in including evidence Type annotations on all edges possible - as provided by some sources (e.g. GOA), or as added by ingestors based on our knowledge of the source
- Note what content is excluded, and why?*
   - Be sure to document any content left behind that should be revisited/ingested in future iterations*.

#### Mapping and Modeling
- What types of Biolink edges will be created (define SPOQ patterns to be used) - and capture rationale for this representation*
- What node/edge properties and patterns should be used to capture metadata?
- Is re-modeling of Phase 2 patterns needed?
- How resolve inconsistencies/conflicts?
- Are new Biolink classes/properties needed?
- Map to / define association types in Biolink as appropriate?

#### Ingest/Infores Partitioning
 - Decide if it make sense to split ingest of source into separate ingests, with separate inforeses
 - Mint and document an infores for each ingest in the infores catalog
     - Assumes each KGX output will warrant a new infores once DAWG stands up a Tier 1 service that provisions this content . . . we can mint and define an infores stub (status: draft) in anticipation of this.

### 6. Write Ingest Code
- Follow specification documented in the RIG . . . 
- Implementation details of the shared pipeline are still t.b.d. 

### 7. Execute Ingests to Produce KGX Files
- Create nodes, edges, ingest-metadata KGX files
- t.b.d. where these will be stored
