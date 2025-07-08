# Reactome Reference Ingest Guide (RIG)
---------------

## Source Information

### Infores
 - `infores:reactome`

### Description
 - 

### Terms of Use
 - Reactome's data is available under a Creative Commons Public Domain Dedication (CC0) license. This is described here: https://reactome.org/license

### Data Access Locations
Reactome can be downloaded from this link, replacing XX with the desired version. Note, only versions 60, 65, and 70 onwards (excluding the latest version) are available through this link. (As of June 2025, Reactome is on version 93).
 - https://download.reactome.org/XX/databases/gk_current.sql.gz

For the latest version, use:
- https://reactome.org/download/current/databases/gk_current.sql.gz

Alternative data formats can be found here (but do not appear to be downloadable by version):
 - https://reactome.org/download-data

### Provision Mechanisms and Formats
- Mechanism(s): File download and API
- Formats:
  - Downloadable Files: Neo4j graphdb; MySQL database; collection of TSV mapping files, pathway information, and protein-protein interactions; BioPAX, SBML; SBGN
  - API Query Instructions: https://reactome.org/AnalysisService/
   
### Releases and Versioning
 - Fairly consistent cadence for releases (March, June, September, and December of each year)
 - Version is a single integer that is increased by 1 for each release
 - Release calendar page: https://reactome.org/about/release-calendar
 - High-level change log: https://reactome.org/about/news


----------------
## Ingest Information

### Utility
- 

### Scope

  #### Relevant Files:

  | File | Description |
  |----------|----------|
  |          |          |
  
  #### Included Content:

  | File | Included Content | Columns |
  |----------|----------|----------|
  |          |          |          |

  #### Excluded Content:

  | File | Excluded Content | Rationale  |
  |----------|----------|----------|
  |          |          |          |
 

  #### Future Considerations:

  | File | Rationale |
  |----------|----------|
  |          |          |

----------------

## Target Information

### Infores
 - `infores:reactome`

### Edge Types

| # | Association Type | Biolink MetaEdge | Qualifier Types |  AT / KL  | UI Explanation |
|----------|----------|----------|----------|----------|----------|
|          |          |          |          |          |          |

**Rationale**:
1. ... (match number to row number in table above)
2. ...

   
### Node Types

| Biolink Category |  Source Identifier Type | Source Identifier Description (source: https://download.reactome.org/documentation/DataModelGlossary_V90.pdf) | Notes |
|------------------|----------------------------|--------|--|
| `biolink:BiologicalProcess` | `Reaction` |  | |
| `biolink:BiologicalEntity` | `OtherEntity` | | |
| `biolink:BiologicalEntity` | `SimpleEntity` | | |
| `biolink:BiologicalEntity` | `GenomeEncodedEntity` | | |
| `biolink:BiologicalProcess` | `BlackBoxEvent` | | |
| `biolink:BiologicalEntity` | `DefinedSet` | | |
| `biolink:SmallMolecule` | `ChemicalDrug` | | |
| `biolink:BiologicalEntity` | `Complex` | | |
| `biolink:PathologicalProcess` | `FailedReaction` | | |
| `biolink:Pathway` | `Pathway` | | |
| `biolink:BiologicalProcess` | `Depolymerisation` | | |
| `biolink:BiologicalProcess` | `PositiveRegulation` | | |
| `biolink:BiologicalProcess` | `NegativeGeneExpressionRegulation` | `NegativeGeneExpressionRegulation` is a subclass of `NegativeRegulation` that describes a direct inhibitory effect of a Regulator on a `BlackBoxEvent` that represents gene expression. The Regulator is a complex of regulatory component(s) such as transcription factors with the target gene or mRNA molecule. The regulatory component(s) are defined as activeUnit(s) of the `NegativeGeneExpressionRegulation` instance. | |
| `biolink:BiologicalProcess` | `NegativeRegulation` |  | |
| `biolink:BiologicalEntity` | `CandidateSet` |  | |
| `biolink:BiologicalProcess` | `Requirement` |  | |
| `biolink:ChemicalEntity` | `ProteinDrug` |  | |
| `biolink:BiologicalEntity` | `Polymer` |  | |
| `biolink:BiologicalEntity` | `EntityWithAccessionedSequence` |  | |
| `biolink:BiologicalProcess` | `Polymerisation` |  | |
| `biolink:BiologicalProcess` | `PositiveGeneExpressionRegulation` |  | |

------------------

## Ingest Contributors
 - Erica Wood