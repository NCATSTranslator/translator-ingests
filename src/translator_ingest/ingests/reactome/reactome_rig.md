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
| `biolink:BiologicalProcess` | `Reaction` | The transformation of input physical entities into output ones in a single step. Transformations include chemical changes, as in metabolism, the transport of an entity between locations, the association of entities to form a complex, and the dissociation of complexes. These transformations can be catalyzed and regulated. | |
| `biolink:BiologicalEntity` | `OtherEntity` | PhysicalEntities that we are unable or unwilling to describe in chemical detail and which, therefore, cannot be put in any other class. OtherEntity can be used to represent complex structures in the cell that take part in a reaction but which we can't or don't want to define molecularly. | |
| `biolink:BiologicalEntity` | `SimpleEntity` | A defined chemical species not encoded directly or indirectly in the genome, typically a small molecule such as ATP or ethanol. The detailed structure of a `SimpleEntity` is specified by linking it to the information provided for the molecule in the ChEBI external database via the _referenceEntity_ slot. Separate `SimpleEntity` instances are needed for each subcellular location (_compartment_) in which a molecule is found, e.g., ATP \[cytosol\] and ATP \[nucleoplasm\]. SimpleEntities such as ATP that occur in many species are not assigned a species. | |
| `biolink:BiologicalEntity` | `GenomeEncodedEntity` | Any informational macromolecule (DNA, RNA, protein) or entity derived from one by post-synthetic modifications, for example, covalently modified forms of proteins. `GenomeEncodedEntity` is a subclass of `PhysicalEntity` and has one subclass: `EntityWithAccessionedSequence`. Unlike an EntityWithAccessionedSequence, a GenomeEncodedEntity is not required to have a reference entity, though a reference to an entity in a database can be specified in the _crossReference_ attribute. | |
| `biolink:BiologicalProcess` | `BlackBoxEvent` | This class can be used to describe an incompletely specified `ReactionLikeEvent` or series of RLEs. A BlackBoxEvent for a reaction is for one that is known to happen but whose molecular details are incompletely known, e.g., a partially purified protein catalyzes a reaction so no EWAS can be identified, or degradation of a protein yields unspecified polypeptide products. BBEs may also be used to represent a multistep process (such as the transcription and translation of a gene into a protein) when the details don't need to be specified in full. A fully annotated sequence of events for a BBE may be depicted in full elsewhere in Reactome. | |
| `biolink:BiologicalEntity` | `DefinedSet` | A collection of well-characterized physical entities that are functionally indistinguishable for the purpose of Reactome annotation, e.g., a collection of isoforms of a protein that all mediate the identical metabolic reaction. A set is formally a list of instances linked by logical ORs. Sets may be ordered or unordered as specified in the _isOrdered_ attribute. A specific member of an ordered set has a correspondence with a specific member of another ordered set, as specified by their positions within the sets. For example, consider an ordered set containing substrate1 and substrate2 that reacts to yield an ordered set containing product1 and product2. In this case, substrate1 will yield only product1 and substrate2 will yield only product2. In the case of unordered sets, any member in an input set can correspond to any member in an output set. Sets in Reactome are considered ordered by default. | |
| `biolink:SmallMolecule` | `ChemicalDrug` | A subclass of the `Drug` class. A ChemicalDrug is a synthetic substance or non-human natural substance that is not a protein, RNA, or DNA molecule and is administered to the body to modify a metabolic reaction or reactions, usually to treat a disease or abnormal state. Mandatory attributes of ChemicalDrug include one or more _diseases_ treated by a drug, and _referenceEntity_, which is a reference to a corresponding entity in a database such as Guide to Pharmacology. | |
| `biolink:BiologicalEntity` | `Complex` | | |
| `biolink:PathologicalProcess` | `FailedReaction` | A `FailedReaction` instance is a step in a disease process that is directly affected by a loss-of-function (LoF) mutation (germline or somatic). This type of disease event has its normal `ReactionLikeEvent` counterpart (the reaction mediated by the un-mutated gene product) specified at its normalReaction attribute and is represented as having inputs (an abnormal physicalEntity plus any wild-type entities that are inputs in the normal reaction), but no outputs. FailedReaction instances are labeled with disease term(s) that are used to populate the disease attribute of the associated abnormal `PhysicalEntity` (`GenomeEncodedEntitiy`, `Complex` or `EntitySet`). | |
| `biolink:Pathway` | `Pathway` | | |
| `biolink:BiologicalProcess` | `Depolymerisation` | | |
| `biolink:BiologicalProcess` | `PositiveRegulation` | | |
| `biolink:BiologicalProcess` | `NegativeGeneExpressionRegulation` | `NegativeGeneExpressionRegulation` is a subclass of `NegativeRegulation` that describes a direct inhibitory effect of a Regulator on a `BlackBoxEvent` that represents gene expression. The Regulator is a complex of regulatory component(s) such as transcription factors with the target gene or mRNA molecule. The regulatory component(s) are defined as activeUnit(s) of the `NegativeGeneExpressionRegulation` instance. | |
| `biolink:BiologicalProcess` | `NegativeRegulation` |  | |
| `biolink:BiologicalEntity` | `CandidateSet` |  | |
| `biolink:BiologicalProcess` | `Requirement` |  | |
| `biolink:ChemicalEntity` | `ProteinDrug` |  | |
| `biolink:BiologicalEntity` | `Polymer` | Molecules that consist of indeterminate numbers of repeated units, and complexes that contain repeated units whose stoichiometry is variable or unknown. The repeated unit(s) (identified in the _repeatedUnit_ attribute) can be any `PhysicalEntity`. The presence of more than one _repeatedUnit_ value implies that the relative numbers of units in the polymer are unknown. If the units are present in known proportions, a `Complex` of the appropriate numbers of units is used as the _repeatedUnit_. The size range of a `Polymer` can be specified with _minUnitCount_ and _maxUnitCount_ values. | |
| `biolink:BiologicalEntity` | `EntityWithAccessionedSequence` |  | |
| `biolink:BiologicalProcess` | `Polymerisation` | A subclass of `ReactionLikeEvent` in which two or more identical molecules or complexes are assembled into a polymer. | |
| `biolink:BiologicalProcess` | `PositiveGeneExpressionRegulation` |  | |



------------------

## Ingest Contributors
 - Erica Wood