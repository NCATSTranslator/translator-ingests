# EBI Gene2Phenotype Reference Ingest Guide (RIG)

## Source Utility to Translator

[EBI's Gene2Phenotype](https://www.ebi.ac.uk/gene2phenotype/) contains high-quality gene-disease associations 
curated by UK disease domain experts and consultant clinical geneticists. 
These associations could be used in MVP1 ("may treat disease X") or Pathfinder queries. 

## Source Releases and Versioning

This resource provides downloads of the latest data, created on-the-fly, [here](https://www.ebi.ac.uk/gene2phenotype/download).
They also provide static releases on their [FTP site](https://ftp.ebi.ac.uk/pub/databases/gene2phenotype/G2P_data_downloads/) ~ every month. 

Dates can be used for versioning: this would be the creation/download date for on-the-fly downloads or the subfolder name (aka creation date) for static releases.

## Excluded Content 

Rows:
- complete duplicates (some rows were found in multiple files)
- no value in `disease mim` column: we currently use only this column as the source of disease IDs
- have the values `limited`, `disputed`, or `refuted` in the `confidence` column:
  - `limited`: the last sentence of the [definition](https://www.ebi.ac.uk/gene2phenotype/about/terminology#g2p-confidence-section) is "The majority are probably false associations. (previously labelled as possible)." We've decided that these may not be "real" associations, so we do not want to ingest them
  - `disputed` and `refuted`: these [values](https://www.ebi.ac.uk/gene2phenotype/about/terminology#g2p-confidence-section) mean there's strong evidence that there ISN'T an association (negation). **This decision to exclude could be revisited once Translator can model/handle negation better**.   
- **had NodeNorm mapping failures for the node IDs (only diseases in this case)**

Columns:
- `disease MONDO`: **may revisit during this ingest process** because resource has been working to improve this data
- `confidence`: currently not including as an edge property, because [more data-modeling in biolink-model](https://github.com/biolink/biolink-model/issues/1583) is needed. **Could revisit once this issue is addressed**
- **Seem harder to get into Translator, potentially useful**: 
  - `cross cutting modifier`: additional info on inheritance. Limited set of terms BUT "; "- delimited. Some terms may map to "HPO inheritance qualifier terms" (didn't try). Lots of missing data (NA). 
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

## Rationale for Edge Types

- `Gene` - `associated_with` - `Disease`: the gene doesn't cause the disease, so the stronger statement is moved to the qualifier set and a general predicate is used here
- every edge has the `qualified_predicate` `biolink:causes`: the association has the [`confidence` value](https://www.ebi.ac.uk/gene2phenotype/about/terminology#g2p-confidence-section) **"moderate", "strong", or "definitive"**, which mean there's moderate-to-definitive evidence that the gene DOES HAVE a causal role in this disease.  
- every edge has a qualifier on Gene (`subject_form_or_variant_qualifier`) because every association has an `allelic requirement` value and `molecular mechanism` value - these [terms](https://www.ebi.ac.uk/gene2phenotype/about/terminology) imply the gene's mutations are being considered here. The `molecular mechanism` values are mapped to these qualifier values (enum):
  - "loss of function" => `loss_of_function_variant_form`
  - "undetermined" => `genetic_variant_form`
  - "gain of function" => `gain_of_function_variant_form`
  - "dominant negative" => `dominant_negative_variant_form`
  - "undetermined non-loss-of-function" => `non_loss_of_function_variant_form`

## Rationale for knowledge level (KL) / agent type (AT)

- `knowledge_assertion` / `manual_agent`: because the associations are curated from literature by UK disease domain experts and consultant clinical geneticists. 

## Misc

We **may want to REVISIT** how we handle the `molecular mechanism` and `variant types` columns VS the **biolink-model qualifier options**:
- There could be a separate qualifier for "genetic mechanisms" that would have the different effects of genetics on function like "loss of function", "dominant negative". And `molecular mechanism` terms could map to this.
- `form_or_variant_qualifier` could have a lot of structural variant terms. Then `variant types` terms could map to this. **One problem is variant_types is multivalued (when a biolink model qualifier is not).** Other notes: 
  - this column has a lot of missing values
  - resource doesn't provide a file with all possible terms and their mappings to SO terms, making the parsing/maintenance of any mapping trickier
  - **>100 UNIQUE VALUES (SINGLE, MIX OF TERMS). I don't think using this as a qualifier is a good idea - it's confusing**