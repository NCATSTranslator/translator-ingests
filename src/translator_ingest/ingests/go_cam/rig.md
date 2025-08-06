# Gene Ontology Causal Activity Models (GO-CAM) Reference Ingest Guide (RIG)

---------------

## Source Information

### Infores
 - [infores:gocam](https://w3id.org/information-resource-registry/gocam)

### Description
GO-CAM (Gene Ontology Causal Activity Models) is a framework that extends standard GO annotations by connecting molecular functions, biological processes, and cellular components into causally linked pathways. GO-CAMs provide explicit causal connections between gene products and their activities within specific biological contexts, enabling more detailed representation of biological mechanisms than traditional GO annotations.
### Source Category(ies)
- [Primary Knowledge Source](https://biolink.github.io/biolink-model/primary_knowledge_source/)   

### Citation (o)
Thomas PD, Hill DP, Mi H, Osumi-Sutherland D, Van Auken K, Carbon S, Balhoff JP, Albou LP, Good B, Gaudet P, Lewis SE, Mungall CJ. Gene Ontology Causal Activity Modeling (GO-CAM) moves beyond GO annotations to structured descriptions of biological functions and systems. Nat Genet. 2019 Oct;51(10):1429-1433. doi: 10.1038/s41588-019-0500-1
### Terms of Use
CC BY 4.0 - Creative Commons Attribution 4.0 International License
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
 - Latest status page: https://geneontology.org/docs/go-cam-overview/

----------------

## Ingest Information
    
### Utility
GO-CAMs provide structured causal relationships between gene products that are essential for pathway analysis, mechanistic understanding, and systems biology approaches in Translator. Unlike traditional GO annotations, GO-CAMs explicitly model how gene products causally regulate each other, making them valuable for reasoning about biological mechanisms and predicting downstream effects of perturbations.
### Scope
This initial ingest focuses on gene-to-gene causal regulatory relationships extracted from GO-CAM models. The scope includes direct regulatory relationships (positive and negative regulation) between gene products, with associated molecular function, biological process, and cellular component annotations for context.
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

  | File | Filtered Records | Rationale |
  |----------|----------|----------|
  | GO-CAM models | GO Term nodes and non-gene entities | Initial focus on gene-gene relationships; GO Terms and other entity types will be included in future iterations |
  | GO-CAM models | Edges without clear causal predicates | Only including edges with explicit causal relationship predicates to ensure high-quality causal assertions |
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

  
-----------------

##  Target Information

### Infores:
 - infores:translator-gocam-kgx
   
### Edge Types

|  Subject Category |  Predicate | Object Category | Qualifier Types |  AT / KL  | Edge Properties | UI Explanation |
|----------|----------|----------|----------|----------|---------|----------|
| Gene | directly_positively_regulates | Gene  |  n/a  |  manual_agent, knowledge_assertion  | model_id, source_gene_molecular_function, source_gene_biological_process, source_gene_occurs_in, target_gene_molecular_function, target_gene_biological_process, target_gene_occurs_in |  GO-CAM models provide explicit causal relationships where one gene product directly positively regulates another gene product's activity. |
| Gene | directly_negatively_regulates | Gene  |  n/a  |  manual_agent, knowledge_assertion  | model_id, source_gene_molecular_function, source_gene_biological_process, source_gene_occurs_in, target_gene_molecular_function, target_gene_biological_process, target_gene_occurs_in |  GO-CAM models provide explicit causal relationships where one gene product directly negatively regulates another gene product's activity. |
| Gene | positively_regulates | Gene  |  n/a  |  manual_agent, knowledge_assertion  | model_id, source_gene_molecular_function, source_gene_biological_process, source_gene_occurs_in, target_gene_molecular_function, target_gene_biological_process, target_gene_occurs_in |  GO-CAM models provide causal relationships where one gene product positively regulates another gene product's activity, potentially through indirect mechanisms. |
| Gene | negatively_regulates | Gene  |  n/a  |  manual_agent, knowledge_assertion  | model_id, source_gene_molecular_function, source_gene_biological_process, source_gene_occurs_in, target_gene_molecular_function, target_gene_biological_process, target_gene_occurs_in |  GO-CAM models provide causal relationships where one gene product negatively regulates another gene product's activity, potentially through indirect mechanisms. |

**Additional Notes/Rationale (o)**:
- GO-CAM predicates are mapped to Biolink regulation predicates based on the causal relationship types defined in the Relation Ontology (RO)
- All edges are manual agent knowledge assertions as GO-CAMs are manually curated models of biological mechanisms
- Edge properties preserve the functional context (molecular function, biological process, cellular component) for both source and target genes
   
### Node Types

High-level Biolink categories of nodes produced from this ingest as assigned by ingestors are listed below - however downstream normalization of node identifiers may result in new/different categories ultimately being assigned.

| Biolink Category |  Source Identifier Type(s) | Notes |
|------------------|----------------------------|--------|
| Gene |  UniProtKB, WormBase, FlyBase, SGD, MGI, etc.  | Gene identifiers from various model organism databases |

### Future Modeling Considerations (o)
- Consider including GO Term nodes and their relationships to genes in future iterations
- Evaluate modeling of complex regulatory cascades and multi-step pathways
- Assess integration with other pathway databases and resources


-----------------

## Provenance Information

### Ingest Contributors
- **Sierra Moxon**: code, model development
- **Matthew Brush**: data modeling, domain expertise

### Artifacts (o)
- [Ingest Survey](https://docs.google.com/spreadsheets/d/1R9z-vywupNrD_3ywuOt_sntcTrNlGmhiUWDXUdkPVpM/edit?gid=0#gid=0)

### Additional Notes (o)
