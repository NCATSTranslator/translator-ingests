# Gene Ontology Causal Activity Models (GO-CAM) Reference Ingest Guide (RIG)

---------------

## Source Information

### Infores
 - [infores:gocam](https://w3id.org/information-resource-registry/ctd)

### Description
   
### Source Category(ies)
- [Primary Knowledge Source](https://biolink.github.io/biolink-model/primary_knowledge_source/)   

### Citation (o)

### Terms of Use

### Data Access Locations
There are two pages for downloading data files.
 - GO-CAMs are downloaded model by model, via kghub-downloader that takes an index file that shows all possible
gocams by identifier, and then iterates one by one through the identifers, downloading each gocam. 
 - Downloads: 
   - index: https://s3.amazonaws.com/provider-to-model.json
   - URL pattern: https://live-go-cam.geneontology.io/product/yaml/go-cam/[id].json
 
### Provision Mechanisms and Formats
- Mechanism(s): Individual file downloads.
- Formats: json
   
### Releases and Versioning
 - New GO-CAMs are added to the index weekl.
 - Releases page / change log: https://geneontology.org/docs/download-go-cams/
 - Latest status page: https://ctdbase.org/about/dataStatus.go

----------------

## Ingest Information
    
### Utility

### Scope

### Relevant Files

  | File                        | Location | Description          |
  |-----------------------------|----------|----------------------|
  | provider-to-model.json      | https://s3.amazonaws.com/provider-to-model.json | index file of models |
  | 5a7e68a100001817.json, etc. | https://live-go-cam.geneontology.io/product/yaml/go-cam/[id].json | each model individually                     | 
  
### Included Content
Records from relevant files that are included in this ingest.

  | File | Included Records   | Fields Used                      | 
  |----------|--------------------|----------------------------------|
  | 5a7e68a100001817.json, etc.   | Gene to Gene edges | source, target, causal_predicate |

### Filtered Content
Records from relevant files that are not included in the ingest.

### Future Content Considerations (o)

- **Edges**
  - Currently, we are excluding GOTerms from the edges.  This is just a first pass at the GO-CAMs to get the
    Gene to Gene edges in place.  Future iterations will include the GOTerms, and potentially other edge types. 

- **Nodes**
  - Just Gene nodes.

- **Node Properties**
  - Includes only the gene identifier and category of "Gene". (Note, there are likely nodes that represent Genes or Gene
Products, but we are not distinguishing between these at this time because we will NodeNormalize the category and id.)

  - n/a
    
- **Edge Properties/EPC Metadata**
  - TODO: plenty of work to do here to make edges like this, Biolink compliant past the source, target, and causal_predicate
which are mapped in this ingest to "biolink:subject", "biolink:object", and the appropriate "biolink:predicate" respectively.
  - The edge properties are not currently being mapped to Biolink Model edge properties, but this will be done in future iterations.
  - The edge properties that are currently being mapped are:
    - source_gene_molecular_function
    - source_gene_biological_process
    - source_gene_occurs_in
    - target_gene_molecular_function
    - target_gene_biological_process
    - target_gene_occurs_in
```text
    "edges": [
      {
        "source": "WB:WBGene00006575",
        "target": "WB:WBGene00003822",
        "model_id": "gomodel:568b0f9600000284",
        "causal_predicate": "RO:0002629",
        "causal_predicate_label": "directly positively regulates",
        "source_gene_molecular_function": "GO:0035591",
        "source_gene_molecular_function_label": "signaling adaptor activity",
        "source_gene_biological_process": "GO:0140367",
        "source_gene_biological_process_label": "antibacterial innate immune response",
        "source_gene_occurs_in": "GO:0005737",
        "source_gene_occurs_in_label": "cytoplasm",
        "source_gene_product": "WB:WBGene00006575",
        "source_gene_product_label": "tir-1 Cele",
        "target_gene_molecular_function": "GO:0004709",
        "target_gene_molecular_function_label": "MAP kinase kinase kinase activity",
        "target_gene_biological_process": "GO:0140367",
        "target_gene_biological_process_label": "antibacterial innate immune response",
        "target_gene_occurs_in": "GO:0005737",
        "target_gene_occurs_in_label": "cytoplasm",
        "target_gene_product": "WB:WBGene00003822",
        "target_gene_product_label": "nsy-1 Cele"
      },
```


## Provenance Information

### Ingest Contributors
- **Sierra Moxon**: code, model development
- **Matthew Brush**: data modeling, domain expertise

### Artifacts (o)
- [Ingest Survey](https://docs.google.com/spreadsheets/d/1R9z-vywupNrD_3ywuOt_sntcTrNlGmhiUWDXUdkPVpM/edit?gid=0#gid=0)

### Additional Notes (o)
