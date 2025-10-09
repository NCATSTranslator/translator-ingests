# EBI Gene2Phenotype Reference Ingest Guide (RIG)

---------------

## Source Information

### Infores
 - [infores:ebi-gene2phenotype](https://w3id.org/information-resource-registry/ebi-gene2phenotype)

### Description
EBI's Gene2Phenotype dataset contains high-quality gene-disease associations curated by UK disease domain experts and consultant clinical geneticists. 
It integrates data on genes, their variants, and related disorders. 
It is constructed by experts reviewing published literature, and it is primarily an inclusion list to allow targeted filtering of genome-wide data for diagnostic purposes. 
Each entry associates a gene with a disease, including a confidence level, allelic requirement and molecular mechanism.

### Source Category(ies)
Use terms from the enumerated list [here](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/rig-instructions.md#source-categoryies).

- Primary Knowledge Provider

### Citation (o)
- https://doi.org/10.1186/s13073-024-01398-1
- https://www.nature.com/articles/s41467-019-10016-3

### Terms of Use
- Unsure, but likely uses [EMBL-EBI terms of use](https://www.ebi.ac.uk/about/terms-of-use/#general) (linked in the [website](https://www.ebi.ac.uk/gene2phenotype/) footer). Don't see a formal license.
- Various webpages ([project](https://www.ebi.ac.uk/gene2phenotype/about/project), [downloads](https://www.ebi.ac.uk/gene2phenotype/download)) say that all data is "freely available"
- Also, various webpages ([downloads](https://www.ebi.ac.uk/gene2phenotype/download), [publications](https://www.ebi.ac.uk/gene2phenotype/publications)) say to please cite the date accessed/data version and [Thorman et al 2019](https://www.nature.com/articles/s41467-019-10016-3)

### Data Access Locations
- Latest data is provided [here](https://www.ebi.ac.uk/gene2phenotype/download) (downloads created on-the-fly).
- Archived static releases provided on the FTP site [here](https://ftp.ebi.ac.uk/pub/databases/gene2phenotype/G2P_data_downloads/). 
   
### Provision Mechanisms and Formats
- Mechanism(s): File Download
- Formats: csv (on-the-fly downloads) or csv.gz (FTP static releases)
   
### Releases and Versioning
- Releases cut and archived roughly every 1-2 months
- on-the-fly downloads: creation/download date are the same and can be used for versioning. Note that the date in the filename may differ from the date in your timezone.
- static releases: creation date (shown on FTP site and in folder/file names) can be used for versioning
  
----------------

## Ingest Information
    
### Utility
EBI G2P associations are useful as edges in support paths for MVP1 ("what may treat disease X"), and in Pathfinder queries. 

### Relevant Files
Source files with content we aim to ingest.

  | File | Location | Description |
  |----------|----------|----------|
  | G2P_all_[date].csv | https://www.ebi.ac.uk/gene2phenotype/api/panel/all/download/ | Associations from all panels (disease categories) | 


### Included Content / Records
Records from the relevant files that are included, and optionally a list of fields in the data that are part of or inform the ingest. 

  | File | Included Records | Fields Used (o) | 
  |----------|----------|----------|
  | G2P_all_[date].csv| Records where 'confidence' value is 'definitive', 'strong', or 'moderate' | g2p id, hgnc id, disease mim, disease MONDO, allelic requirement, confidence, molecular mechanism, publications, date of last review |
 
       
### Filtered Content
Records from relevant files that are not included in the ingest.

  | File | Filtered  Records | Rationale |
  |----------|----------|----------|
  | G2P_all_[date].csv |  Records where 'confidence' value is 'limited', 'disputed', or 'refuted' | Evidence level not sufficient for inclusion. |
  | G2P_all_[date].csv |  Records with no values in both 'disease mim' and 'disease MONDO' columns | No IDs to use for disease nodes |
  | G2P_all_[date].csv |  Records with NodeNorm mapping failures for the node IDs | Failed normalization means that the node would not be connected to other data/nodes in Translator graphs. |

     
### Future Content Considerations (o)
Content additions/changes to consider for future iterations (consider edge content node property content, and edge property/EPC content)

- **Edges**
  - Revisit exclusion of 'disputed' and/or 'refuted' records once Translator can model/handle negation better
    
- **Node Properties**
  - n/a
    
- **Edge Properties / EPC Metadata**
  - Lots of additional edge-level information that we could include in future iterations (see example record [here](https://www.ebi.ac.uk/gene2phenotype/lgd/G2P03700)):
     - 'confidence' level values when we improve/refactor modeling of confidence in Biolink
     - variant information ('variant consequence', 'variant types' columns). The values map to SO terms.
     - Matt's note: rich evidence and provenance metadata provided by the source (e.g. type of experiments/methods used to determine the molecular mechanism, and supporting publications).
    
-----------------

##  Target Information

### Infores
infores:translator-ebi-gene2phenotype-kgx
   
### Edge Types

| Subject Category |  Predicate | Object Category | Qualifier Types |  AT / KL  | Edge Properties | UI Explanation |
|----------|----------|----------|----------|---------|----------|---------|
| Gene | associated_with | Disease | qualified_predicate: causes, subject_form_or_variant_qualifier: ChemicalOrGeneOrGeneProductFormOrVariantEnum | manual_agent, knowledge_assertion | original_subject, original_object, update_date | EBI G2P curators manually determined through the evaluation of different types of evidence that variants of this gene of the indicated form (e.g. loss of function, gain of function, dominant negative) play a causal role in this disease. |


**Additional Notes/Rationale (o)**:
- n/a

### Node Types

High-level Biolink categories of nodes produced from this ingest as assigned by ingestors are listed below - however downstream normalization of node identifiers may result in new/different categories ultimately being assigned.

| Biolink Category |  Source Identifier Type(s) | Node Properties | Notes |
|------------------|----------------------------|--------|---------|
| Gene | 	HGNC  | none |  |
| Disease | OMIM, orphanet, MONDO | none | 'disease mim' column is source of OMIM and orphanet IDs. MONDO IDs from 'disease MONDO' column are only used if row doesn't have a value in 'disease mim' column |


### Future Modeling Considerations (o)
- May want to revisit how we handle the 'molecular mechanism' and 'variant types' columns VS the biolink-model qualifier options 
- Revisit modeling of allelic_requirement (uses a regex pattern to match HP id syntax now, rather than an enumerated list of permissible values). See PR linked below for more details.


------------------

## Ingest Provenance (o)

### Ingest Contributors (o)
- **Colleen Xu**: code author, data modeling
- **Andrew Su**: domain expertise
- **Sierra Moxon**: domain expertise
- **Matthew Brush**: data modeling, domain expertise

### Artifacts(o)
- Github Ticket on confidence 'limited' value: https://github.com/biolink/biolink-model/issues/1581
- PR on biolink allelic_requirement: https://github.com/biolink/biolink-model/pull/1576


### Additional Notes (o)
- **Source Confidence values**:
      - `limited`: the last sentence of the [definition](https://www.ebi.ac.uk/gene2phenotype/about/terminology#g2p-confidence-section) is "The majority are probably false associations. (previously labelled as possible)." We've decided that these may not be "real" associations, so we do not want to ingest them
      - `disputed` and `refuted`: these [values](https://www.ebi.ac.uk/gene2phenotype/about/terminology#g2p-confidence-section) mean there's strong evidence that there ISN'T an association (negation). **This decision to exclude could be revisited once Translator can model/handle negation better.**

- **Source Columns**:
     - `confidence`: currently not including as an edge property, because [more data-modeling in biolink-model](https://github.com/biolink/biolink-model/issues/1583) is needed. **Could revisit once this issue is addressed.**
     - **Seem harder to get into Translator, potentially useful**: 
       - `cross cutting modifier`: additional info on inheritance. Limited set of terms BUT "; "- delimited. Some terms may map to "HPO inheritance qualifier terms" (didn't try). Lots of missing data (NA)
          - would be a new edge/node property or qualifier. But complicated because EBI gene2pheno has custom terms, not just from HPO inheritance qualifiers. 
       - `variant consequence`: row can have multiple values ("; "- delimited). Limited set of terms already mapped to SO.
          - seems like aspect qualifier, but this can be a list for a gene-disease edge - and I'm not sure how to handle this (not that comfortable splitting into multiple edges)
       - `variant types`: row can have multiple values ("; "- delimited). Medium set of terms already mapped to SO. Lots of missing data (NA). **>100 UNIQUE VALUES (SINGLE, MIX OF TERMS). I don't think using this as a qualifier is a good idea - it's confusing**
          - would be a new edge/node property or qualifier (somewhat modeled as predicates, for variant-gene relationships).
       - `molecular mechanism support` (**new Aug 2025**): "qualifies" the molecular_mechanism (seems to say how molecular mechanism was decided: "inferred" or "evidence"). 
          - **Doesn't seem to show up on single-record webpage** (compared rows with value "evidence" / no "molecular_mechanism_categorisation" value to their webpages; the webpages don't have the string "evidence").
          - tricky since it's like "how knowledge was obtained" for a specific part of edge (I'm using molecular_mechanism to adjust the subject qualifier) 
       - `molecular mechanism categorisation` (**new Aug 2025**): more detailed mechanism info, structured (displayed in single-record webpage as a table). 
          - VERY SPARSE (LOTS OF NA), multivalued, more complex structure
       - `molecular mechanism evidence`: "types of evidence available to support reported mechanism", according to Data download format txt. complicated structure, is displayed in single-record webpages as a table. Lots of NA. 
          - VERY SPARSE (LOTS OF NA), multivalued, very complex structure with custom terms that aren't defined/explained anywhere yet
       - `comments`: treat as free text
       - `review` (**new Aug 2025**): indicates when record is under review (according to Data_download_format txt file). Don't know what the possible values are (boolean?) because I've only seen the column be all NA so far.
          - interesting concept idea, but not sure if we want it in Translator or not (also may change in source quickly and we'll be outdated
    - Not useful to get into Translator:
       - `gene mim`: using `hgnc id` column as the source of gene node IDs instead
       - `gene symbol`, `previous gene symbols`, `disease name`: nodes will ultimately use human-readable labels from NodeNorm
       - `phenotypes`: "reported by the publication". Unclear how they fit in gene-disease association or a diff edge (gene-phenotype, phenotype-disease)
       - `panel`: pretty specific, original resource's way of organizing data

- **Revisit Modeling**:  We may want to reconsider how we handle the `molecular mechanism` and `variant types` columns VS the biolink-model qualifier options:
  - There could be a separate qualifier for "genetic mechanisms" that would have the different effects of genetics on function like "loss of function", "dominant negative". And `molecular mechanism` terms could map to this.
  - `form_or_variant_qualifier` could have a lot of structural variant terms. Then `variant types` terms could map to this. **One problem is variant_types is multivalued (when a biolink model qualifier is not).** Other notes: 
    - this column has a lot of missing values
    - resource doesn't provide a file with all possible terms and their mappings to SO terms, making the parsing/maintenance of any mapping trickier
    - **>100 UNIQUE VALUES (SINGLE, MIX OF TERMS). I don't think using this as a qualifier is a good idea - it's confusing**
