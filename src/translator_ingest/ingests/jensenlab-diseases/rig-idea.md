# JensenLab DISEASES Reference Ingest Guide (RIG)

## Source Utility to Translator

[JensenLab DISEASES](https://diseases.jensenlab.org/About) contains gene-disease associations from unique sources, 
including their own text-mining pipeline and external human-curated resources that are hard to access or parse 
(MedlinePlus, AmyCo). 
These associations could be used in MVP1 ("may treat disease X") or Pathfinder queries. 

## Source Releases and Versioning

They claim to have weekly updates (ref: [resource's "About" page](https://diseases.jensenlab.org/About), 
[paper](https://doi.org/10.1093/database/baac019)), perhaps every weekend (paper: "The corpus in DISEASES is updated every weekend"). 
The paper reports the resource version as "2.0", but the resource's website does not provide versioning or 
dates for the data downloads (ref: [resource's "Downloads" page](https://diseases.jensenlab.org/Downloads)).

I have noticed an increase in the number of rows in the text-mining file and the MedlinePlus and UniProtKB subsets of the knowledge file over a 
~ 2 month span. I have not monitored/checked to see if a change happens weekly and when.

## Excluded Content 

Files:
- "full" versions: we used the "filtered" versions of these files instead, because we want non-redundant association data 
- "experiments channel" file: this data is originally from TIGA, and we decided that if we want this data, it is best retrieved directly from TIGA (or GWAS-Catalog). 

Rows:
- complete duplicates
- don't have an `ENSP` ID in the gene ID column or a `DOID` in the disease ID column: these rows instead had non-ID strings (based on string-searches, a little manual review) or IDs that wouldn't be resolved by NodeNorm (AmyCo)
- had the value `UniProtKB-KW` in the source database column (only relevant to the "knowledge channel" file): we didn't find DISEASES's ingest of this resource high-quality, so we decided that if we want this data, it is best retrieved directly from UniProtKB
- **had NodeNorm mapping failures for gene or disease IDs**

Columns (note I named these columns, original files didn't have any headers):
- `gene_name` and `disease_name`: nodes will ultimately use human-readable labels from NodeNorm
- `evidence_type` (only relevant to the "knowledge channel" file): same for all rows, not needed

## Rationale for Edge Types

- `Gene` - `occurs together in literature with` - `Disease`: used for associations from "text mining channel" file. These are based on co-mentions in biomedical literature, and we cannot put a more specific predicate on the relationship. 
- `Gene` - `genetically associated with` - `Disease`: used for associations from "knowledge channel" file. The [paper](https://doi.org/10.1093/database/baac019) does not explain the relationship types of these associations, so we used a "general" gene-disease predicate. 

## Rationale for knowledge level (KL) / agent type (AT)

- `statistical_association` / `text_mining_agent`: used for associations from "text mining channel" file. The knowledge level choice was suggested by Matt Brush, because the associations are based on "co-mentions in biomedical literature". 
- `knowledge_assertion` / `manual_agent`: used for associations from "knowledge channel" file because the underlying sources are manually curated. 