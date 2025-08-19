# Alliance Disease Association Ingest Pipeline

## Overview

The **Alliance Disease Association Ingest Pipeline** transforms genotype-to-disease associations from the [Alliance of Genome Resources](https://www.alliancegenome.org/) and integrates them with the [Monarch Knowledge Graph](https://monarchinitiative.org/), following the [Biolink Model](https://biolink.github.io/biolink-model/). This pipeline processes data from multiple Model Organism Databases (MODs), maps predicates according to their types, and excludes associations with experimental conditions or modifiers.

### Source Mapping

The data sources are mapped to specific information resources using the following mapping:

| **Source**                           | **Description**                                               |
|--------------------------------------|---------------------------------------------------------------|
| **FB (FlyBase)**                     | Represents *Drosophila melanogaster* (fruit fly).              |
| **MGI (Mouse Genome Informatics)**   | Represents *Mus musculus* (mouse).                             |
| **RGD (Rat Genome Database)**        | Represents *Rattus norvegicus* (rat).                          |
| **HGNC (HUGO Gene Nomenclature Committee)** | Represents human genes.                              |
| **SGD (Saccharomyces Genome Database)** | Represents *Saccharomyces cerevisiae* (yeast).           |
| **WB (WormBase)**                    | Represents *Caenorhabditis elegans* (nematode worm).           |
| **Xenbase**                          | Represents *Xenopus* species (African clawed frog).            |
| **ZFIN (Zebrafish Information Network)** | Represents *Danio rerio* (zebrafish).                    |


### Pipeline Details

The ingest pipeline processes different types of genotype-disease associations, such as gene-to-disease, variant-to-disease, and genotype-to-disease associations, while handling diverse predicate types (e.g., `biolink:related_to`, `biolink:biomarker_for`, `biolink:model_of`, etc.).

The main steps include:

1. **Subject Category Identification**: Identifies the subject category (gene, allele, or affected genomic model) to determine the correct association type (`GeneToDiseaseAssociation`, `VariantToDiseaseAssociation`, or `GenotypeToDiseaseAssociation`).

2. **Predicate Mapping**: Maps the predicate for each association based on the `AssociationType` in the data.

3. **Filtering**: Excludes rows with unsupported association types, experimental conditions (other than standard conditions), or modifiers.

4. **Output Generation**: Outputs the processed associations in a format compatible with the Monarch Knowledge Graph.

### Resulting Biolink Class

The resulting Biolink class includes the following fields:

- **Category**: `biolink:GenotypeToDiseaseAssociation`
- **Subject**: `MGI:123456`
- **Predicate**: `biolink:model_of`
- **Object**: `MONDO:0001234`
- **Publications**: `PMID:123456`
- **Primary Knowledge Source**: `infores:mgi`
- **Aggregator Knowledge Source**: `infores:monarchinitiative, infores:agrkb`
- **Knowledge Level**: `knowledge_assertion`
- **Agent Type**: `manual_agent`

{{ get_edges_report() }}

The edges report provides a summary of the number of genotype-to-disease associations from different sources, categorized by subject prefix and predicate:
This table captures the total number of associations processed from each source, categorized by their specific predicates and object prefixes. The "MOD" (Model Organism Databases) sources such as MGI, RGD, WB, and ZFIN provide the foundational data for these associations.

{{ get_nodes_report() }}
