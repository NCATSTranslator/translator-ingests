# Panther Orthology RIG

## Source Information

**InfoRes ID:** infores:panther

**Description:** The PANTHER (Protein ANalysis THrough Evolutionary Relationships) Classification System  was designed to classify proteins (and their genes) in order to facilitate high-throughput analysis. The core of PANTHER is a comprehensive, annotated library of gene family phylogenetic trees. All nodes in the tree have persistent identifiers that are maintained between versions of PANTHER, providing a stable substrate for annotations of protein properties like subfamily and function.

**Citations:**
- https://onlinelibrary.wiley.com/doi/10.1002/pro.4218

**Data Access Locations:**
- http://data.pantherdb.org/ftp/ortholog/current_release/RefGenomeOrthologs.tar.gz

**Data Provision Mechanisms:** file_download

**Data Formats:** csv

**Data Versioning and Releases:** Versioning by number and associated date. See https://www.pantherdb.org/data/

**Additional Notes:** None

## Ingest Information

**Ingest Categories:** primary_knowledge_provider

**Utility:** Homology relationships between human genes and other model research species like mouse, rat and many other species can be suggestive of the biological function of genes, since gene function can often be partially inferred from experimental observations - in the model species - which cannot be easily or ethically replicated in human beings.

**Scope:** Gene to gene genomic orthology relationships

### Relevant Files

| File Name | Location | Description |
| --- | --- | --- |
| RefGenomeOrthologs.tar.gz | http://data.pantherdb.org/ftp/ortholog/current_release/ | Gene to Gene Orthology Relationships |

### Included Content

| File Name | Included Records | Fields Used |
| --- | --- | --- |
| RefGenomeOrthologs.tar.gz | All records, with taxonomic filtering noted below. | Gene, Ortholog, Type of ortholog, Panther Ortholog ID |

### Filtered Content

| File Name | Filtered Records | Rationale |
| --- | --- | --- |
| RefGenomeOrthologs.tar.gz | All records with Gene and Ortholog pairwise annotated as 'HUMAN', 'MOUSE' or 'RAT' specific. | Panther contains a huge number of records covering orthologs across 144 diverse species (as of September 2025), but our core interest in Translator focuses on genes in taxa close in evolutionary terms to human, thus having significant genetic, molecular and physiological  annotation closer to human biology in character. It is also expected that the genes of these evolutionarily close species also already have a significant assignment of functional roles  partially inferred from other model organisms (e.g. developmental gene functions mapped  from fruit fly or nematode onto mouse genes, but also experimentally tested in mouse). The character of genetic and physiological systems are much more similar between humans and  mice or rats, in particular, studied responses in pharmacology, metabolism, and the immune system. |

## Target Information

**Target InfoRes ID:** infores:translator-panther-kgx

### Edge Types

| Subject Categories | Predicate | Object Categories | Knowledge Level | Agent Type | UI Explanation |
| --- | --- | --- | --- | --- | --- |
| biolink:Gene | biolink:orthologous_to | biolink:Gene | knowledge_assertion | not_provided | Panther orthology family |

### Node Types

| Node Category | Source Identifier Types | Additional Notes |
| --- | --- | --- |
| biolink:Gene | HGNC, MGI, RGD, ENSEMBL |  |

### Future Modeling Considerations

**other:** The Monarch Initiative ingest of Panther data (https://github.com/monarch-initiative/pantherdb-orthologs-ingest) sometimes attempts to map eccentric identifiers to the NCBI gene identifier space. The initial iteration of  the Panther ingest in Translator does not attempt to do this at this time, but rather simply uses the given UniProt identifier.

## Provenance Information

**Contributors:**
- Richard Bruskiewich - data modeling, domain expertise, code author
- Kevin Schaper - code author

**Artifacts:**
- Ingest Survey (https://docs.google.com/spreadsheets/d/1YlpI5bjGNGR5JC9VWxZJ7dd87hS_b4BMZv5geYe2NCk/edit?gid=0#gid=0)
- Ingest Ticket (https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues/44)
