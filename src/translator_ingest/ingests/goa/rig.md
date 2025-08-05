# GO Annotations (GOA) Reference Ingest Guide (RIG)

---------------

## Source Information

### Infores
 - [infores:goa](https://w3id.org/information-resource-registry/goa)

### Description
GO Annotations connect genes to a Gene Ontology term that describes a molecular function it enables, a biological process in which it participates, or a cellular component in which it is located. 
Most are produced through rigorous manual curation of the literature, although some are based on automated pipelines that assign GO terms based on things like orthology or sequence similarity. 

### Source Category(ies)
- Primary Knowledge Provider

### Citation (o)
- Data Archive: https://zenodo.org/records/10536401
- Publication: https://doi.org/10.1093/nar/gky1055

### Terms of Use
- https://geneontology.org/docs/go-citation-policy/
- Uses the [CC BY 4.0 license](https://creativecommons.org/licenses/by/4.0/legalcode#s3a1),

### Data Access Locations
- All downloads: https://geneontology.org/docs/download-go-annotations/
- Commonly studied organisms: https://current.geneontology.org/products/pages/downloads.html
   
### Provision Mechanisms and Formats
- Mechanism(s): File download.
- Formats: tsv in GAF format (17 columns)

   
### Releases and Versioning
* **Release cadence:** Approximately every four weeks, synchronized with UniProtKB.
* **Versioning:** By date - each GAF header includes a `!Generated: YYYY-MM-DD` line.
* **Release notes:** [https://geneontology.org/docs/download-go-annotations/](https://geneontology.org/docs/download-go-annotations/) and [https://geneontology.org/docs/go-annotation-file-gaf-format-2.2/](https://geneontology.org/docs/go-annotation-file-gaf-format-2.2/)
* **Release archive**: https://release.geneontology.org/

----------------

## Ingest Information
    
### Utility
GOA is a rich source of manually curated knowledge about gene function with broad relevance to all Translator queries and use cases. 

### Scope
This initial ingest of GOA covers molecular function, biological process, and cellular component annotations about human and mouse genes only, including manually curated and electronically inferred content, from GAF files (GPAD and GPI formats not ingested). 
Other species may be added in future updates to the ingest. 


### Relevant Files
Source files with content we aim to ingest.

  | File | Location | Description |
  |----------|----------|----------|
  | goa_human.gaf | https://current.geneontology.org/products/pages/downloads.html|  Human gene-product to GO term associations (GAF 2.2)  | 
  | mgi.gaf | https://current.geneontology.org/products/pages/downloads.html | Mouse gene-product to GO term associations (GAF 2.2)  | 
  
### Included Content
Records from the relevant files that are included, and a list of fields in the data that are part of or inform the ingest. 

  | File | Included Records | Fields Used | 
  |----------|----------|----------|
  | goa_human.gaf | All records included | DB, DB Object ID, DB Object Symbol, Relation, GO ID, DB:Reference(s), Evidence Code, With (or) From, Aspect, DB Object Type, Taxon |
  | mgi.gaf | All records included | DB, DB Object ID, DB Object Symbol, Relation, GO ID, DB:Reference(s), Evidence Code, With (or) From, Aspect, DB Object Type, Taxon |
  
### Filtered Content
Records from relevant files that are not included in the ingest.

n/a - no recrods are filtered from the source data sets listed above. 


### Future Content Considerations (o)

- **Edges**
  - Consider ingesting Gene/Product to GO Term annotations from other taxon
  - Consider inclusion of qualifying information (as may be found in the Annotation Extensions, or With or From columns) to existing and new Gene/Product to GO Term annotations 
  - Consider ingesting associations between two GO Terms, per the specification [here](https://wiki.geneontology.org/index.php/Annotation_Relations#Standard_Annotation:_Annotation_Extension_Relations)

- **Node Properties**
  - t.b.d. if we will bring in taxon info about gene/gene product nodes from GOA, or rely on other gene property authorities for this information (e.g. ncbigene)
    
- **Edge Properties/EPC Metadata**
  - n/a

  
-----------------

##  Target Information

### Infores:
 - infores:translator-goa-kgx
   
### Edge Types

|  Subject Category |  Predicate | Object Category | Qualifier Types |  AT / KL  | Edge Properties | UI Explanation |
|----------|----------|----------|----------|----------|---------|----------|
|  Gene, Protein, MacromolecularComplex, RNAProduct  | enables |  MolecularFunction | n/a | varies, depending on mappings from GO evidence codes   | 'negated', 'has_evidence', 'publications' | A GO Annotation uses 'enables' predicate when a gene product is solely capable of executing the reported function. |
|  MacromolecularComplex | contributes_to |  MolecularFunction | n/a | varies, depending on mappings from GO evidence codes   | 'negated', 'has_evidence', 'publications' | A GO Annotation uses 'contributes_to' predicate when a gene product is required as part of a macromelecular complex for executing the reported function. |
|  Gene, Protein, MacromolecularComplex, RNAProduct | involved_in |  BiologicalProcess | n/a | varies, depending on mappings from GO evidence codes   | 'negated', 'has_evidence', 'publications' | A GO Annotation uses 'involved_in' predicate when a gene product's molecular function plays an integral role in the reported biological process. |
|  Gene, Protein, MacromolecularComplex, RNAProduct | acts_upstream_of_or_within |  BiologicalProcess | n/a | varies, depending on mappings from GO evidence codes   | 'negated', 'has_evidence', 'publications' | A GO Annotation uses 'acts_upstream_of_or_within' predicate when the mechanism / timing of the gene product's activity relative to the reported biological process is not known, as is the directionality of its effect on the process. |
|  Gene, Protein, MacromolecularComplex, RNAProduct | acts_upstream_of_or_within_positive_effect |  BiologicalProcess | n/a | varies, depending on mappings from GO evidence codes   | 'negated', 'has_evidence', 'publications' | A GO Annotation uses 'acts_upstream_of_or_within_positive_effect' predicate when the mechanism / timing of the gene product's activity relative to the reported biological process is not known, but the activity of the gene product has a positive effect on the process. |
|  Gene, Protein, MacromolecularComplex, RNAProduct | acts_upstream_of_or_within_negative_effect |  BiologicalProcess | n/a | varies, depending on mappings from GO evidence codes   | 'negated', 'has_evidence', 'publications' |A GO Annotation uses 'acts_upstream_of_or_within_negative_effect' predicate when the mechanism / timing of the gene product's activity relative to the reported biological process is not known, but the activity of the gene product has a negative effect on the process. |
|  Gene, Protein, MacromolecularComplex, RNAProduct | acts_upstream_of |  BiologicalProcess | n/a | varies, depending on mappings from GO evidence codes   | 'negated', 'has_evidence', 'publications' |A GO Annotation uses 'acts_upstream_of' predicate when the a gene product acts through a known mechanism upstream of the reported biological process, does not regualte the process, and the directionality of its effect on the process is not known.  |
|  Gene, Protein, MacromolecularComplex, RNAProduct | acts_upstream_of_positive_effect |  BiologicalProcess | n/a | varies, depending on mappings from GO evidence codes   | 'negated', 'has_evidence', 'publications' | A GO Annotation uses 'acts_upstream_of_positive_effect' predicate when a gene product acts through a known mechanism upstream of the reported biological process, does not regulate the process, and the activity of the gene product is requred for the process but does not regulate it.|
|  Gene, Protein, MacromolecularComplex, RNAProduct | acts_upstream_of_negative_effect |  BiologicalProcess | n/a | varies, depending on mappings from GO evidence codes   | 'negated', 'has_evidence', 'publications'  | A GO Annotation uses 'acts_upstream_of_negative_effect' predicate when a gene product acts through a known mechanism upstream of the reported biological process, does not regulate the process, and the activity of the gene product prevents or reduces the process but does not regulate it. |
|  Gene, Protein, MacromolecularComplex, RNAProduct  | is_active_in |  CellularComponent | n/a | varies, depending on mappings from GO evidence codes   | 'negated', 'has_evidence', 'publications' |  A GO Annotation uses 'is_active_in' predicate when a gene product is present in and performs its molecular function in the reported cellular component. |
|  Gene, Protein, MacromolecularComplex, RNAProduct  | located_in |  CellularComponent | n/a | varies, depending on mappings from GO evidence codes   | 'negated', 'has_evidence', 'publications' |  A GO Annotation uses 'located_in' predicate when a gene product enables is detected in the reported cellular component.|
|  Gene, Protein, RNAProduct | part_of |  MacromolecularComplex | n/a | varies, depending on mappings from GO evidence codes   | 'negated', 'has_evidence', 'publications' |  A GO Annotation uses 'part_of' predicate when a gene product is a component of the reported macromolecular complex. |
|  Gene, Protein, MacromolecularComplex, RNAProduct  | colocalizes_with |  CellularComponent | n/a | varies, depending on mappings from GO evidence codes   | 'negated', 'has_evidence', 'publications' |  A GO Annotation uses 'cplocalizes_with' predicate when a gene product has a transient or dynamic association with the reported cellular component.|


**Additional Notes/Rationale (o)**:
- NOTE that where we can identify annotations that were imported/re-structured from GO-CAM, we can keep infores:goa as the primary source for these edges, but indicate infores:go-cam as a 'supporting_data_provider'
   
### Node Types
High-level Biolink categories of nodes produced from this ingest as assigned by ingestors are listed below - however downstream normalization of node identifiers may result in new/different categories ultimately being assigned.


| Biolink Category |  Source Identifier Type(s) | Node Properties | Notes |
|------------------|----------------------------|----------------|--------|
| Gene          | MGI  |  none  |   |
| Protein       | UniProtKB accession  | none  |   |
| MacromolecularComplex | ComplexPortal IDs  |  none  |   |
| RNAProduct    | RNAcentral IDs  | none  |   |
| BiologicalProcess | Gene Ontology IDs (Aspect P)  | none |   |
| MolecularActivity | Gene Ontology IDs (Aspect F)  | none |   |
| CellularComponent | Gene Ontology IDs (Aspect C) | none |   |

  
### Future Modeling Considerations (o)
- Introduce qualifier-based representation if/when we decide to ingest any qualifying context on GO annotations
- If we end up ingesting taxon info for gene nodes, we may have to update the Biolink Model to support this (currently in_taxon is represented as a predicate, and species_context_qualifier as an edge property - but there is no taxon node property) 

-----------------

## Provenance Information

### Ingest Contributors
- **Adilbek Bazarkulov**: code author
- **Evan Morris**: code support
- **Adilbek Bazarkulov**: code support, domain expertise
- **Sierra Moxon**: data modeling, domain expertise
- **Matthew Brush**: data modeling, domain expertise

### Artifacts (o)
- [Ingest Survey](https://docs.google.com/spreadsheets/d/18wGm2a0W1oIXm7cn8TZ99xn_aAMJ91SgAsuPDcV-lII/edit?gid=325339947#gid=325339947)
- [Ingest Ticket](https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues/8)

### Additional Notes (o)

### Framework Integration
- **Koza Framework**: Uses `@koza.transform_record()` decorator for record-by-record processing
- **Biolink Pydantic Model**: Leverages biolink pydantic models for validation and structure
- **Dynamic Class Selection**: Uses database source to determine appropriate biolink class for entities
- **Dynamic GO Term Categorization**: Maps GO aspects to specific biolink classes for semantic precision

### Database Source Mapping
- **Dynamic Selection**: Database source determines biolink class:
  - `UniProtKB` -> `Protein`
  - `MGI`, `SGD`, `RGD`, `ZFIN`, `FB`, `WB`, `TAIR` -> `Gene`
  - `ComplexPortal` -> `MacromolecularComplex`
  - `RNAcentral` -> `RNAProduct`
- **Extensibility**: Easy to add new database sources and their corresponding biolink classes

### GO Aspect Mapping
- **Dynamic Categorization**: GO aspects map to specific biolink classes:
  - `P` (Process) -> `BiologicalProcess`
  - `F` (Function) -> `MolecularActivity`
  - `C` (Component) -> `CellularComponent`
- **Semantic Precision**: Uses most appropriate biolink class for each GO aspect

### Evidence Code Mapping
- **Hardcoded Mapping**: Uses hardcoded evidence code to knowledge level/agent type mapping for simplicity and performance
- **Biolink Enums**: Leverages `KnowledgeLevelEnum` and `AgentTypeEnum` for type safety and validation
- **Fallback Values**: Unknown evidence codes default to `not_provided` for both knowledge level and agent type
- **Evidence Formatting**: Evidence codes are formatted as ECO CURIEs (e.g., `ECO:IEA`)

### Association Selection
- **Dynamic Association**: Uses `GeneToGoTermAssociation` for Gene entities, generic `Association` for others
- **Biolink Compliance**: Uses specific association when available, falls back to generic Association
- **Extensibility**: Can easily add specific associations for Protein, MacromolecularComplex, etc. when they become available

### Taxon Modeling
- **Node-Level Only**: `in_taxon` is only set on entity nodes, not on associations
- **Framework Constraint**: GeneToGoTermAssociation doesn't include the 'thing with taxon' mixin in the biolink model
- **Inference**: Taxon information can be inferred from the subject node's `in_taxon` property

