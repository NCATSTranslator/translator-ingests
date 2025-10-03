# Panther Gene Orthology and Annotation RIG

## Source Information

**InfoRes ID:** infores:panther

**Description:** The PANTHER (Protein ANalysis THrough Evolutionary Relationships) Classification System  was designed to classify proteins (and their genes) in order to facilitate high-throughput analysis. The core of PANTHER is a comprehensive, annotated library of gene family phylogenetic trees. All nodes in the tree have persistent identifiers that are maintained between versions of PANTHER, providing a stable substrate for annotations of protein properties like subfamily and function.

**Citations:**
- https://doi.org/10.1002/pro.4218

**Data Access Locations:**
- http://data.pantherdb.org/ftp/

**Data Provision Mechanisms:** file_download

**Data Formats:** csv

**Data Versioning and Releases:** Versioning by number. See https://www.pantherdb.org/data/

**Additional Notes:** None

## Ingest Information

**Ingest Categories:** primary_knowledge_provider

**Utility:** Homology relationships and association of GO and related annotation by orthology inference can be made in between human genes and  non-human species like mouse, rat and many model species, based on the phenotypic characteristics of genes transitively inferred from experimental observations in the model species which cannot generally be easily or ethically replicated upon human beings.

**Scope:** Gene to gene genomic orthology relationships and associated annotations

### Relevant Files

| File Name | Location | Description |
| --- | --- | --- |
| RefGenomeOrthologs.tar.gz | http://data.pantherdb.org/ftp/ortholog/current_release/ | Gene to Gene Orthology Relationships in reference genomes |
| PTHR<Panther-version>_<taxon_name> | http://data.pantherdb.org/ftp/sequence_classifications/current_release/PANTHER_Sequence_Classification_files/ | Gene sequence annotation from specific genomes |

### Included Content

| File Name | Included Records | Fields Used |
| --- | --- | --- |
| RefGenomeOrthologs.tar.gz | All records, with taxonomic filtering noted below. | Gene, Ortholog, Type of ortholog, Panther Ortholog ID |
| PTHR<Panther-version>_<taxon_name> | All records for the specified taxon. | Gene, Ortholog, Type of ortholog, Panther Ortholog ID |

### Filtered Content

| File Name | Filtered Records | Rationale |
| --- | --- | --- |
| RefGenomeOrthologs.tar.gz, PTHR<Panther-version>_<taxon_name> | All records with Gene and Ortholog pairwise annotated with taxon name as 'HUMAN', 'MOUSE' or 'RAT' specific. | Panther contains a huge number of records covering orthologs across 144 diverse species (as of September 2025), but our core interest in Translator focuses on genes in taxa close in evolutionary terms to human, thus having significant genetic, molecular and physiological  annotation closer to human biology in character. It is also expected that the genes of these evolutionarily close species also already have a significant assignment of functional roles  partially inferred from other model organisms (e.g. developmental gene functions mapped  from fruit fly or nematode onto mouse genes, but also experimentally tested in mouse). The character of genetic and physiological systems are much more similar between humans and  mice or rats, in particular, studied responses in pharmacology, metabolism, and the immune system. |

### Future Content Considerations

**other:** Additional model species may be included in the future.

## Target Information

**Target InfoRes ID:** infores:translator-panther-kgx

### Edge Types

| Subject Categories | Predicate | Object Categories | Knowledge Level | Agent Type | UI Explanation |
| --- | --- | --- | --- | --- | --- |
| biolink:Gene | biolink:orthologous_to | biolink:Gene | knowledge_assertion | manual_validation_of_automated_agent | Panther orthology family |
| biolink:GeneFamily | biolink:has_part | biolink:GeneFamily | knowledge_assertion | manual_validation_of_automated_agent | Panther gene family to gene family relationship |
| biolink:GeneFamily | biolink:has_part | biolink:Gene | knowledge_assertion | manual_validation_of_automated_agent | Panther Gene family membership |
| biolink:GeneFamily | biolink:located_in | biolink:CellularComponent | knowledge_assertion | manual_validation_of_automated_agent | Panther Gene family cellular location of expression |
| biolink:GeneFamily | biolink:actively_involved_in | biolink:BiologicalProcess | knowledge_assertion | manual_validation_of_automated_agent | Panther Gene Family involvement in a biological process |
| biolink:GeneFamily | biolink:catalyzes | biolink:MolecularActivity | knowledge_assertion | manual_validation_of_automated_agent | Panther Gene Family catalysis of a molecular activity |
| biolink:GeneFamily | biolink:actively_involved_in | biolink:Pathway | knowledge_assertion | manual_validation_of_automated_agent | Panther active involvement of a Gene Family in a pathway |
| biolink:Pathway | biolink:has_participant | biolink:GeneFamily | knowledge_assertion | manual_validation_of_automated_agent | Panther participation of a Gene Family in a pathway |
| biolink:Pathway | biolink:subclass_of | biolink:BiologicalProcess | knowledge_assertion | manual_validation_of_automated_agent | Panther pathway as a subclass of a biological process |
| biolink:Pathway | biolink:subclass_of | biolink:Pathway | knowledge_assertion | manual_validation_of_automated_agent | Panther pathway as a subclass of another pathway |
| biolink:MolecularActivity | biolink:subclass_of | biolink:MolecularActivity | knowledge_assertion | manual_validation_of_automated_agent | Panther molecular activity as a subclass of another molecular activity |
| biolink:BiologicalProcess | biolink:subclass_of | biolink:BiologicalProcess | knowledge_assertion | manual_validation_of_automated_agent | Panther biological process as a subclass of another biological process |
| biolink:CellularComponent | biolink:subclass_of | biolink:CellularComponent | knowledge_assertion | manual_validation_of_automated_agent | Panther cellular component as a subclass of another cellular component |
| biolink:CellularComponent | biolink:subclass_of | biolink:AnatomicalEntity | knowledge_assertion | manual_validation_of_automated_agent | Panther cellular component as a subclass of anatomical entity |
| biolink:CellularComponent | biolink:subclass_of | biolink:GrossAnatomicalStructure | knowledge_assertion | manual_validation_of_automated_agent | Panther cellular component as a subclass of gross anatomical structure |
| biolink:AnatomicalEntity | biolink:subclass_of | biolink:CellularComponent | knowledge_assertion | manual_validation_of_automated_agent | Panther anatomical entity as a subclass of cellular component |
| biolink:GrossAnatomicalStructure | biolink:subclass_of | biolink:CellularComponent | knowledge_assertion | manual_validation_of_automated_agent | Panther gross anatomical structure as a subclass of cellular component |

### Node Types

| Node Category | Source Identifier Types | Additional Notes |
| --- | --- | --- |
| biolink:Gene | HGNC, MGI, RGD, ENSEMBL, NCBIGene |  |
| biolink:GeneFamily | PANTHER.FAMILY |  |
| biolink:Pathway | PANTHER.PATHWAY |  |
| biolink:MolecularActivity | GO |  |
| biolink:BiologicalProcess | GO |  |
| biolink:CellularComponent | GO |  |
| biolink:AnatomicalEntity | UBERON, UMLS |  |
| biolink:GrossAnatomicalStructure | UBERON | biolink:Attribute is used to capture the information content |

### Future Modeling Considerations

**other:** The Monarch Initiative ingest of Panther data (https://github.com/monarch-initiative/pantherdb-orthologs-ingest) sometimes attempts to map eccentric identifiers to the NCBI gene identifier space. The initial iteration of  the Panther ingest in Translator does not attempt to do this at this time, but rather simply uses the given UniProt identifier.

## Provenance Information

**Contributors:**
- Richard Bruskiewich - data modelling, domain expertise, code author
- Kevin Schaper - Phase 2 legacy code expert
- Evan Morris - Phase 2 legacy code expert
- Chunlei Wu - Phase 2 legacy code expert
- Matt Brush - data modelling

**Artifacts:**
- Ingest Survey (https://docs.google.com/spreadsheets/d/1YlpI5bjGNGR5JC9VWxZJ7dd87hS_b4BMZv5geYe2NCk/edit?gid=0#gid=0)
- Ingest Ticket (https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues/44)

