# EBI Gene2Phenotype Reference Ingest Guide (RIG)

---------------

## Source Information

### Infores
 - [infores:ebi-gene2phenotype](https://w3id.org/information-resource-registry/ebi-gene2phenotype)

### Description
EBI's Gene2Phenotype dataset contains high-quality gene-disease associations curated by UK disease domain experts and consultant clinical geneticists. 
It integrates data on genes, variants and phenotypes for example relating to developmental disorders. 
It is constructed from published literature, and is primarily an inclusion list to allow targeted filtering of genome-wide data for diagnostic purposes. 
Each entry associates a gene with a disease phenotype via an evidence level, inheritance mechanism and mutation consequence. 

### Source Category(ies)
- Primary Knowledge Provider

### Citation (o)
- https://www.nature.com/articles/s41467-019-10016-3

### Terms of Use
- Minimal terms of use described on Downloads page [here](https://www.ebi.ac.uk/gene2phenotype/download). No formal license. 
- Key language:
  - "The data from G2P is freely available and interested parties can get in touch via g2p-help@ebi.ac.uk for further information."
  - "If you use these data in your work please cite the data version and Thorman et al 2019 (https://www.nature.com/articles/s41467-019-10016-3)  "

### Data Access Locations
- Latest data is provided [here](https://www.ebi.ac.uk/gene2phenotype/download).
- Archived static releases provided on the FTP site [here](https://ftp.ebi.ac.uk/pub/databases/gene2phenotype/G2P_data_downloads/). 
   
### Provision Mechanisms and Formats
- Mechanism(s): File Download
- Formats: csv
   
### Releases and Versioning
- Releases cut and archived roughly every 1-2 months
- Dates are used for versioning
  
----------------

## Ingest Information
    
### Utility
EBI G2P associations are useful as edges in support paths for MVP1 ("what may treat disease X"), and in Pathfinder queries. 

### Relevant Files
Source files with content we aim to ingest.

  | File | Location | Description |
  |----------|----------|----------|
  | G2P_all_[date].csv | https://www.ebi.ac.uk/gene2phenotype/download | Associations for all disease categories. Didn't use the checksum files.  | 


### Included Content
Records from relevant files that are included in this ingest.

  | File | Included Records | Fields Used | 
  |----------|----------|----------|
  | G2P_all_[date].csv| Records where 'confidence' value is 'definitive', 'strong', or 'moderate'  |  gene mim, hgnc id, disease mim, disease MONDO, confidence, allelic requirement, molecular mechanism |
 
       
### Filtered Content
Records from relevant files that are not included in the ingest.

  | File | Filtered  Records | Rationale |
  |----------|----------|----------|
  | G2P_all_[date].csv |  Records where 'confidence' value is 'limited', 'disputed', or 'refuted' | Evidence level not sufficient for inclusion, and/or not enough records to bother including. |
  | G2P_all_[date].csv |  Records with no value in 'disease mim' column | We currently use only this column as the source of disease IDs |
  | G2P_all_[date].csv |  Records with NodeNorm mapping failures for the node IDs | Failed normalization means that the node would not be connected to other data/nodes in Translator graphs. |

     
### Future Content Considerations (o)

- **Edges**
  - Revisit exclusion of 'disputed' and/or 'refuted' records once Translator can model/handle negation better
    
- **Node Properties**
  - n/a
    
- **Edge Properties / EPC Metadata**
  - Lots of additional qualifiers and edge properties we could include in future iterations (see example record [here](https://www.ebi.ac.uk/gene2phenotype/lgd/G2P03700)):
     - confidence level values when we improve/refactor modeling of confidence in Biolink
     - more granular 'variant type' information (SO terms)
     - rich evidence and provenance metadata provided by the source (e.g. type of experiments/methods used to determine the molecular mechanism, and supporting publications.
    
-----------------

##  Target Information

### Infores
infores:translator-ebi-gene2phenotype-kgx
   
### Edge Types

| Subject Category |  Predicate | Object Category | Qualifier Types |  AT / KL  | Edge Properties | UI Explanation |
|----------|----------|----------|----------|---------|----------|---------|
| Gene | associated_with | Disease  |  qualified_predicate: causes, subject_form_or_variant_qualifier: ChemicalOrGeneOrGeneProductFormOrVariantEnum, allelic_requirement_qualifier: regex (constrains to HP id syntax) |  manual_agent, knowledge_assertion | none | EBI G2P curators follow rigorous evidence interpretation guidelines to determine what specific types of mutations in a given gene are causal for a specific disease or phenotype, which Biolink models using the 'causes' predicate to connect a variant form of a Gene to the resulting condition. |

**Additional Notes/Rationale (o)**:
- n/a

### Node Types

High-level Biolink categories of nodes produced from this ingest as assigned by ingestors are listed below - however downstream normalization of node identifiers may result in new/different categories ultimately being assigned.

| Biolink Category |  Source Identifier Type(s) | Node Properties | Notes |
|------------------|----------------------------|--------|---------|
| Gene | 	HGNC, MIM  |  |  |
| Disease | MONDO, MIM|  |  |


### Future Modeling Considerations (o)
- Revisit modeling of constraints on allelic_requirement_qualifier values (uses a regex pattern to match HP id syntax now, rather than an enumerated list of permissible values). See PR linked below for more details.


------------------

## Ingest Provenance

### Ingest Contributors (o)
- **Colleen Xu**: code author, data modeling
- **Andrew Su**: domain expertise
- **Sierra Moxon**: code support, domain expertise
- **Matthew Brush**: data modeling, domain expertise

### Artifacts(o)
- Github Ticket: https://github.com/biolink/biolink-model/issues/1581
- PR: https://github.com/biolink/biolink-model/pull/1576


### Additional Notes (o)
- **Source Confidence values**:
      - `limited`: the last sentence of the [definition](https://www.ebi.ac.uk/gene2phenotype/about/terminology#g2p-confidence-section) is "The majority are probably false associations. (previously labelled as possible)." We've decided that these may not be "real" associations, so we do not want to ingest them
      - `disputed` and `refuted`: these [values](https://www.ebi.ac.uk/gene2phenotype/about/terminology#g2p-confidence-section) mean there's strong evidence that there ISN'T an association (negation).

- **Source Columns**:
     - `disease MONDO`: may revisit during this ingest process because resource has been working to improve this data
     - `confidence`: currently not including as an edge property, because [more data-modeling in biolink-model](https://github.com/biolink/biolink-model/issues/1583) is needed. Could revisit once this issue is addressed.
     - **Seem harder to get into Translator, potentially useful**: 
       - `molecular mechanism categorisation`: "qualifies" the `molecular mechanism` column's value (seems to say how molecular mechanism was decided: "inferred" or "evidence") 
           - tricky since it's like "how knowledge was obtained" for a specific part of edge (I'm using `molecular mechanism` to adjust the subject qualifier) 
       - `cross cutting modifier`: additional info on inheritance. Limited set of terms BUT "; "- delimited. Some terms may map to "HPO inheritance qualifier terms" (didn't try). Lots of missing data (NA)
          - would be a new edge/node property or qualifier. But complicated because EBI gene2pheno has custom terms, not just from HPO inheritance qualifiers. 
       - `variant consequence`: row can have multiple values ("; "- delimited). Limited set of terms already mapped to SO.
          - seems like aspect qualifier, but this can be a list for a gene-disease edge - and I'm not sure how to handle this (not that comfortable splitting into multiple edges)
       - `variant types`: row can have multiple values ("; "- delimited). 30+ set of terms, already mapped to SO. Lots of missing data (NA)
       - `molecular mechanism evidence`: treat as free text? very complicated string 
       - `comments`: treat as free text
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
