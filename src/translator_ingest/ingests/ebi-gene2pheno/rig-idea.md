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

Files:
- ".csv.gz.md5": didn't use the checksum files

Rows:
- complete duplicates (some rows were found in multiple files)
- no value in `disease_mim` column: we chose this column to be the source of disease node IDs
- had the value `disputed` or `refuted` in the `confidence` column: these [values](https://www.ebi.ac.uk/gene2phenotype/about/terminology#g2p-confidence-section) mean there's strong evidence that there ISN'T an association (negation)
- **had NodeNorm mapping failures for the node IDs (only diseases in this case)**

Columns:
- **Seem harder to get into Translator, potentially useful**: 
  - `molecular_mechanism_categorisation`: "qualifies" the `molecular_mechanism` column's value (seems to say how molecular mechanism was decided: "inferred" or "evidence") 
  - `cross_cutting_modifier`: additional info on inheritance. Limited set of terms BUT "; "- delimited. Some terms may map to "HPO inheritance qualifier terms" (didn't try). Lots of missing data (NA)
  - `variant_consequence`: row can have multiple values ("; "- delimited). Limited set of terms already mapped to SO.
  - `variant_types`: row can have multiple values ("; "- delimited). Medium set of terms already mapped to SO. Lots of missing data (NA)
  - `molecular_mechanism_evidence`: treat as free text? very complicated string 
  - `comments`: treat as free text
- Not useful to get into Translator:
  - gene_mim: using hgnc_id column as the source of gene node IDs instead
  - disease_MONDO: previous analysis of these IDs (running them thru NodeNorm) revealed issues with accuracy, specificity, and matching the `disease_mim` column's IDs. Decided to use the disease_mim` column as the only source of disease node IDs.
  - gene_symbol, previous_gene_symbols, disease_name: nodes will ultimately use human-readable labels from NodeNorm
  - phenotypes: "reported by the publication". Unclear how they fit in gene-disease association or a diff edge (gene-phenotype, phenotype-disease)
  - panel: pretty specific, original resource's way of organizing data

## Rationale for Edge Types

- `Gene` (`genetic_variant_form`) - `causes` - `Disease`: used for associations with [`confidence` values](https://www.ebi.ac.uk/gene2phenotype/about/terminology#g2p-confidence-section) **"moderate", "strong", or "definitive"**. These values mean there's moderate-to-definitive evidence that the gene DOES HAVE a causal role in this disease. We use the qualifier `genetic_variant_form` because every association had an allelic_requirement value, and those [terms](https://www.ebi.ac.uk/gene2phenotype/about/terminology) describe the gene's mutations that possibly cause the disease.  
- `Gene` (`genetic_variant_form`) - `related_to` - `Disease`: used for associations with the [`confidence` value](https://www.ebi.ac.uk/gene2phenotype/about/terminology#g2p-confidence-section) **"limited"**. We interpret the definition of "limited" as saying there is AN association - it's just not causal (as far as we know) and it's unclear how "real"/important it is. So we use a very general predicate. We use the qualifier `genetic_variant_form` because every association had an allelic_requirement value, and those [terms](https://www.ebi.ac.uk/gene2phenotype/about/terminology) describe the gene's mutations that possibly cause the disease.  

## Rationale for knowledge level (KL) / agent type (AT)

- `knowledge_assertion` / `manual_agent`: because the associations are curated from literature by UK disease domain experts and consultant clinical geneticists. 