# Standard Operating Procedure for a Source Re-Ingest
Below we define general tasks that should be done for each re-ingest. It assumes the source data is provisioned via file download, but analogous tasks can be envisioned for data provided through API endpoints, database access, etc.

## 1. Establish / Organize Owners and Contributors
- Determine who will perform and contribute to each re-ingest. We are currently using the s/s [here](https://docs.google.com/spreadsheets/d/1nbhTsEb-FicBz1w69pnwCyyebq_2L8RNTLnIkGYp1co/edit?gid=1969427496#gid=1969427496).
- As appropriate, schedule a Planning Call for all contributors to collaboratively address/document the questions/tasks below.

## 2. Understand and Compare Phase 2 Ingest Review

### Prepare
- Reps for each KP that ingested a source should prepare by reviewing what their KP ingested, from where, how it was transformed to Biolink Associations, and the logic/rationale behind any processing or filtering that was performed.
- As appropriate (in particular for sources ingested by multiple KPs), we strongly recommend each KP populates an **Ingest Survey Spreadsheet** like the one [here](https://docs.google.com/spreadsheets/d/1R9z-vywupNrD_3ywuOt_sntcTrNlGmhiUWDXUdkPVpM/edit?gid=0#gid=0) for CTD - to help surface and resolve any differneces in what and how source data is ingested, filtered, and modeled across KP graphs. This will help the 'Compare' task described below. 
    
### Compare
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

## 3. Scope and Specify Re-Ingest Details (document in a RIG)

### Assess Source Utility
- Is the source as a whole worth ingesting?
- Perhaps not if overall quality/utility is low, or there is a better alternative for this type of data?
- Consider findings from the 'Translator Results Analysis' that quantifies how often different types of edges from different sources are surfaced in query results (as answer edges, or support path edges)

### Select Content to Ingest
- Make final decisions on what content will be included in the initial re-ingest
    - What knowledge (edges), concepts (nodes) are of high enough quality and utility to include in the ingest?*
    - Remember to consider ingest of nodes not participating in ingested edges
- What node/edge metadata are of high enough quality and utility to include in the ingest?*
    - Consider EPC use cases in particular
    - Remember to carefully assess and include KL/AT annotations.
   -  UI and O&O are also keenly interested in including evidence Type annotations on all edges possible - as provided by some sources (e.g. GOA), or as added by ingestors based on our knowledge of the source
- Note what content is excluded, and why?*
   - Be sure to document any content left behind that should be revisited/ingested in future iterations*.

### Modeling Work
- What types of Biolink edges will be created (define SPOQ patterns to be used) - and capture rationale for this representation*
- What node/edge properties and patterns should be used to capture metadata?
- Is re-modeling of Phase 2 patterns needed?
- How resolve inconsistencies/conflicts?
- Are new Biolink classes/properties needed?
- Map to / define association types in Biolink as appropriate?

### Ingest/Infores Partitioning
 - Decide if it make sense to split ingest of source into separate ingests, with separate inforeses
 - Mint and document an infores for each ingest in the infores catalog
     - Assumes each KGX output will warrant a new infores once DAWG stands up a Tier 1 service that provisions this content . . . we can mint and define an infores stub (status: draft) in anticipation of this.

## 4. Write Ingest Code
- Follow specification documented in the RIG . . . 
- Implementation details of shared pipeline t,b,d. 

## 5. Execute Ingests to Produce KGX Files
- Create nodes, edges, ingest-metadata KGX files
- t.b.d. where these will be stored
