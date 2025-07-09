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

Table | Rationale
-- | --
\_deleted |  
\_deleted_2_deletedinstance |  
\_deleted_2_deletedinstancedb_id |  
\_deleted_2_replacementinstancedb_ids |  
\_deleted_2_replacementinstances |  
\_deletedinstance |  
\_deletedinstance_2_species |  
\_updatetracker |  
\_updatetracker_2_action |  
abstractmodifiedresidue |  
affiliation |  
affiliation_2_name |  
anatomy |  
blackboxevent |  
book |  
book_2_chapterauthors |  
candidateset |  
candidateset_2_hascandidate |  
catalystactivity |  
catalystactivity_2_activeunit |  
catalystactivityreference |  
cell |  
cell_2_markerreference |  
cell_2_proteinmarker |  
cell_2_rnamarker |  
cell_2_species |  
celldevelopmentstep |  
celllineagepath |  
celltype |  
chemicaldrug | This table is just a list of database identifiers for nodes with the Reactome category "ChemicalDrug". This information is already contained in the databaseobject table (`select DB_ID from databaseobject where _class="ChemicalDrug"`).
compartment | This table is just a list of database identifiers for nodes with the Reactome category "Compartment". This information is already contained in the databaseobject table (`select DB_ID from databaseobject where _class="Compartment"`).
complex |  
complex_2_compartment |  
complex_2_entityonothercell |  
complex_2_includedlocation |  
complex_2_relatedspecies |  
controlledvocabulary |  
controlledvocabulary_2_name |  
controlreference |  
controlreference_2_literaturereference |  
crosslinkedresidue |  
crosslinkedresidue_2_secondcoordinate |  
databaseidentifier_2_crossreference |  
databaseobject_2_modified |  
datamodel |  
definedset |  
deletedcontrolledvocabulary |  
depolymerisation |  
disease | This table is just a list of database identifiers for nodes with the Reactome category "Disease". This information is already contained in the databaseobject table (`select DB_ID from databaseobject where _class="Disease"`).
drug_2_compartment |  
drugactiontype |  
drugactiontype_2_instanceof |  
entityfunctionalstatus |  
entityfunctionalstatus_2_functionalstatus |  
entityset |  
entityset_2_compartment |  
entityset_2_relatedspecies |  
entitywithaccessionedsequence_2_hasmodifiedresidue |  
event_2_authored |  
event_2_crossreference |  
event_2_edited |  
event_2_figure |  
event_2_inferredfrom |  
event_2_internalreviewed |  
event_2_name |  
event_2_negativeprecedingevent |  
event_2_orthologousevent |  
event_2_precedingevent |  
event_2_relatedspecies |  
event_2_reviewed |  
event_2_revised |  
event_2_structuremodified |  
evidencetype |  
externalontology_2_instanceof |  
externalontology_2_name |  
externalontology_2_synonym |  
failedreaction | This table is just a list of database identifiers for nodes with the Reactome category "FailedReaction". This information is already contained in the databaseobject table (`select DB_ID from databaseobject where _class="FailedReaction"`).
figure |  
fragmentdeletionmodification |  
fragmentinsertionmodification |  
fragmentmodification |  
fragmentreplacedmodification |  
frontpage |  
frontpage_2_frontpageitem |  
functionalstatus |  
functionalstatustype |  
functionalstatustype_2_name |  
geneticallymodifiedresidue |  
genomeencodedentity |  
genomeencodedentity_2_compartment |  
go_biologicalprocess_2_name |  
go_cellularcomponent |  
go_cellularcomponent_2_componentof |  
go_cellularcomponent_2_haspart |  
go_cellularcomponent_2_instanceof |  
go_cellularcomponent_2_name |  
go_cellularcomponent_2_surroundedby |  
go_molecularfunction |  
go_molecularfunction_2_ecnumber |  
go_molecularfunction_2_name |  
groupmodifiedresidue |  
instanceedit_2_author |  
interactionevent |  
interactionevent_2_partners |  
interchaincrosslinkedresidue |  
interchaincrosslinkedresidue_2_equivalentto |  
interchaincrosslinkedresidue_2_secondreferencesequence |  
intrachaincrosslinkedresidue |  
markerreference |  
markerreference_2_cell |  
modifiednucleotide |  
modifiedresidue | This table is just a list of database identifiers for nodes with the Reactome category "ModifiedResidue". This information is already contained in the databaseobject table (`select DB_ID from databaseobject where _class="ModifiedResidue"`).
negativegeneexpressionregulation | This table is just a list of database identifiers for nodes with the Reactome category "NegativeGeneExpressionRegulation". This information is already contained in the databaseobject table (`select DB_ID from databaseobject where _class="NegativeGeneExpressionRegulation"`).
negativeprecedingevent |  
negativeprecedingevent_2_precedingevent |  
negativeprecedingeventreason |  
negativeregulation |  
nonsensemutation |  
ontology |  
otherentity |  
otherentity_2_compartment |  
pathway |  
pathway_2_compartment |  
pathwaydiagram |  
pathwaydiagram_2_representedpathway |  
person_2_affiliation |  
person_2_crossreference |  
person_2_figure |  
physicalentity |  
physicalentity_2_celltype |  
physicalentity_2_edited |  
physicalentity_2_figure |  
physicalentity_2_inferredfrom |  
physicalentity_2_inferredto |  
physicalentity_2_name |  
physicalentity_2_reviewed |  
physicalentity_2_revised |  
polymer |  
polymer_2_compartment |  
polymer_2_repeatedunit |  
polymerisation |  
positivegeneexpressionregulation | This table is just a list of database identifiers for nodes with the Reactome category "PositiveGeneExpressionRegulation". This information is already contained in the databaseobject table (`select DB_ID from databaseobject where _class="PositiveGeneExpressionRegulation"`).
positiveregulation | This table is just a list of database identifiers for nodes with the Reactome category "PositiveRegulation". This information is already contained in the databaseobject table (`select DB_ID from databaseobject where _class in ("PositiveGeneExpressionRegulation", "Requirement", "PositiveRegulation")`).
proteindrug |  
psimod |  
publication |  
reaction |  
reactionlikeevent |  
reactionlikeevent_2_catalystactivity |  
reactionlikeevent_2_compartment |  
reactionlikeevent_2_entityfunctionalstatus |  
reactionlikeevent_2_entityonothercell |  
reactionlikeevent_2_hasinteraction |  
reactionlikeevent_2_reactiontype |  
reactionlikeevent_2_regulationreference |  
reactionlikeevent_2_requiredinputcomponent |  
reactiontype |  
referencednasequence |  
referenceentity_2_crossreference |  
referenceentity_2_name |  
referenceentity_2_otheridentifier |  
referencegeneproduct |  
referencegeneproduct_2_chain |  
referencegeneproduct_2_referencegene |  
referencegeneproduct_2_referencetranscript |  
referencegroup |  
referenceisoform |  
referenceisoform_2_isoformparent |  
referencemolecule |  
referencernasequence |  
referencernasequence_2_referencegene |  
referencesequence |  
referencesequence_2_comment |  
referencesequence_2_description |  
referencesequence_2_genename |  
referencesequence_2_keyword |  
referencesequence_2_secondaryidentifier |  
referencetherapeutic |  
referencetherapeutic_2_activedrugids |  
referencetherapeutic_2_approvalsource |  
referencetherapeutic_2_prodrugids |  
regulation_2_activeunit |  
regulationreference |  
replacedresidue |  
replacedresidue_2_psimod |  
requirement |  
retractionstatus |  
reviewstatus |  
rnadrug | This table is just a list of database identifiers for nodes with the Reactome category "RNADrug". This information is already contained in the databaseobject table (`select DB_ID from databaseobject where _class="RNADrug"`).
sequenceontology |  
simpleentity_2_compartment |  
species |  
stableidentifier_2_history |  
stableidentifierhistory |  
stableidentifierhistory_2_historystatus |  
stableidentifierreleasestatus |  
taxon |  
taxon_2_crossreference |  
taxon_2_name |  
transcriptionalmodification |  
translationalmodification |  
url |   

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
| `biolink:BiologicalProcess` | `Reaction` | The transformation of input physical entities into output ones in a single step. Transformations include chemical changes, as in metabolism, the transport of an entity between locations, the association of entities to form a complex, and the dissociation of complexes. These transformations can be catalyzed and regulated. | Historically, RTX-KG2 has mapped Reactome's `Reaction` category to `biolink:BiologicalProcess`. However, given [`reaction` is an alias of `biolink:MolecularActivity`](https://github.com/biolink/biolink-model/blob/2a2149c23a1e0141d8e51c9c3d1fb83e350a3a17/biolink-model.yaml#L7724), this seems better labeled with that category. |
| `biolink:BiologicalEntity` | `OtherEntity` | PhysicalEntities that we are unable or unwilling to describe in chemical detail and which, therefore, cannot be put in any other class. OtherEntity can be used to represent complex structures in the cell that take part in a reaction but which we can't or don't want to define molecularly. | RTX-KG2 categroizes this as a `biolink:BiologicalEntity` because Reactome seems to use this miscellaneous category primarily (if not exclusively) for biological material (per its source identifier description as well as the general focus of the knowledge source). |
| `biolink:SmallMolecule` | `SimpleEntity` | A defined chemical species not encoded directly or indirectly in the genome, typically a small molecule such as ATP or ethanol. The detailed structure of a `SimpleEntity` is specified by linking it to the information provided for the molecule in the ChEBI external database via the _referenceEntity_ slot. Separate `SimpleEntity` instances are needed for each subcellular location (_compartment_) in which a molecule is found, e.g., ATP \[cytosol\] and ATP \[nucleoplasm\]. SimpleEntities such as ATP that occur in many species are not assigned a species. | Historically, RTX-KG2 has mapped Reactome's `SimpleEntity` to `biolink:BiologicalEntity`. However, given `SimpleEntity` is primarily assigned to small molecules, it seems more accurate to categorize these nodes as `biolink:SmallMolecule`. |
| `biolink:BiologicalEntity` | `GenomeEncodedEntity` | Any informational macromolecule (DNA, RNA, protein) or entity derived from one by post-synthetic modifications, for example, covalently modified forms of proteins. `GenomeEncodedEntity` is a subclass of `PhysicalEntity` and has one subclass: `EntityWithAccessionedSequence`. Unlike an EntityWithAccessionedSequence, a GenomeEncodedEntity is not required to have a reference entity, though a reference to an entity in a database can be specified in the _crossReference_ attribute. | |
| `biolink:BiologicalProcess` | `BlackBoxEvent` | This class can be used to describe an incompletely specified `ReactionLikeEvent` or series of RLEs. A BlackBoxEvent for a reaction is for one that is known to happen but whose molecular details are incompletely known, e.g., a partially purified protein catalyzes a reaction so no EWAS can be identified, or degradation of a protein yields unspecified polypeptide products. BBEs may also be used to represent a multistep process (such as the transcription and translation of a gene into a protein) when the details don't need to be specified in full. A fully annotated sequence of events for a BBE may be depicted in full elsewhere in Reactome. | |
| `biolink:BiologicalEntity` | `DefinedSet` | A collection of well-characterized physical entities that are functionally indistinguishable for the purpose of Reactome annotation, e.g., a collection of isoforms of a protein that all mediate the identical metabolic reaction. A set is formally a list of instances linked by logical ORs. Sets may be ordered or unordered as specified in the _isOrdered_ attribute. A specific member of an ordered set has a correspondence with a specific member of another ordered set, as specified by their positions within the sets. For example, consider an ordered set containing substrate1 and substrate2 that reacts to yield an ordered set containing product1 and product2. In this case, substrate1 will yield only product1 and substrate2 will yield only product2. In the case of unordered sets, any member in an input set can correspond to any member in an output set. Sets in Reactome are considered ordered by default. | |
| `biolink:SmallMolecule` | `ChemicalDrug` | A subclass of the `Drug` class. A ChemicalDrug is a synthetic substance or non-human natural substance that is not a protein, RNA, or DNA molecule and is administered to the body to modify a metabolic reaction or reactions, usually to treat a disease or abnormal state. Mandatory attributes of ChemicalDrug include one or more _diseases_ treated by a drug, and _referenceEntity_, which is a reference to a corresponding entity in a database such as Guide to Pharmacology. | |
| `biolink:BiologicalEntity` | `Complex` | A `PhysicalEntity` that represents a larger functional unit formed by the covalent or noncovalent association of two or more `PhysicalEntity` components (which can themselves be `Complexes` or `Sets`). Usually, complex components are proteins, but they can also be RNA or DNA molecules, or `SimpleEntity` chemicals, e.g. metal ions, nucleotides, lipids that function as enzyme cofactors. Complexes include homomultimers, e.g., [ESR1 homodimer (nucleoplasm)](https://reactome.org/content/detail/R-HSA-1254384), heteromultimers, e.g., [FANCD2:FANCI \[nucleoplasm\]](https://reactome.org/content/detail/R-HSA-420764), and complexes composed of other complexes, e.g., [PPARG:RXRA:Corepressor Complex \[nucleoplasm\]](https://reactome.org/content/detail/R-HSA-381226). | |
| `biolink:PathologicalProcess` | `FailedReaction` | A `FailedReaction` instance is a step in a disease process that is directly affected by a loss-of-function (LoF) mutation (germline or somatic). This type of disease event has its normal `ReactionLikeEvent` counterpart (the reaction mediated by the un-mutated gene product) specified at its normalReaction attribute and is represented as having inputs (an abnormal physicalEntity plus any wild-type entities that are inputs in the normal reaction), but no outputs. FailedReaction instances are labeled with disease term(s) that are used to populate the disease attribute of the associated abnormal `PhysicalEntity` (`GenomeEncodedEntitiy`, `Complex` or `EntitySet`). | |
| `biolink:Pathway` | `Pathway` | A sequence of two or more causally connected `ReactionlikeEvents` and/or other `Pathways`, identified as _hasEvent_ attributes of the pathway being annotated. | |
| `biolink:BiologicalProcess` | `Depolymerisation` | A subclass of `ReactionlikeEvent` that is used to annotate depolymerisation and follows the pattern: Polymer -> Polymer + Unit (reverse situation of Polymerisation). | |
| `biolink:BiologicalProcess` | `PositiveRegulation` | `PositiveRegulation` is an optional class used to populate the _regulatedBy_ attribute of a `ReactionLikeEvent` that describes a stimulatory effect of a Regulator, a `PhysicalEntity`, on that event. The mechanism of action of a Regulator, if known, can be described through the optional _activity_ attribute which is populated by terms from an External Ontology GO_MolecularFunction. If the Regulator is a Complex, the specific Complex component(s) that play the regulatory role can be specified as activeUnit(s) of the PositiveRegulation instance. | |
| `biolink:BiologicalProcess` | `NegativeGeneExpressionRegulation` | `NegativeGeneExpressionRegulation` is a subclass of `NegativeRegulation` that describes a direct inhibitory effect of a Regulator on a `BlackBoxEvent` that represents gene expression. The Regulator is a complex of regulatory component(s) such as transcription factors with the target gene or mRNA molecule. The regulatory component(s) are defined as activeUnit(s) of the `NegativeGeneExpressionRegulation` instance. | |
| `biolink:BiologicalProcess` | `NegativeRegulation` | `NegativeRegulation` is an optional class used to populate the regulatedBy attribute of a `ReactionLikeEvent` that describes an inhibitory effect of a Regulator, a `PhysicalEntity`, on that event. The mechanism of action of a Regulator, if known, can be described through the optional activity attribute which is populated by terms from an ExternalOntology GO_MolecularFunction. If the Regulator is a Complex, the specific Complex component(s) that play the regulatory role can be specified as activeUnit(s) of the NegativeRegulation instance. | |
| `biolink:BiologicalEntity` | `CandidateSet` | A collection of physical entities that are functionally indistinguishable for the purpose of Reactome annotation, some of which are well-characterized (the “members” of the set) and some of which are incompletely characterized (the “candidates” of the set), as judged by the curator who assembles the set and the outside expert reviewers who evaluate it. Members of a set are related by an "OR" function. That is, either one member OR another member can participate in a reaction. Sets may be ordered or unordered as specified in the _isOrdered_ attribute. A specific member of an ordered set has a correspondence with a specific member of another ordered set, as specified by their positions within the sets. For example, consider an ordered set containing substrate1 and substrate2 that reacts to yield an ordered set containing product1 and product2. In this case, substrate1 will yield product1 and substrate2 will yield product2. In the case of unordered sets, any member in an input set can correspond to any member in an output set. Sets in Reactome are considered ordered by default. | |
| `biolink:BiologicalProcess` | `Requirement` | A subclass of `PositiveRegulation` that denotes a regulatory `PhysicalEntity` without which the regulated event cannot occur. | |
| `biolink:ChemicalEntity` | `ProteinDrug` | A `ProteinDrug` is a substance that contains an active component that is a protein, whether modified or unmodified, and is administered to the body to modify a metabolic reaction or reactions, usually to treat a disease or abnormal state. A subclass of the `Drug` class. Mandatory attributes of ProteinDrug include disease, which is a reference to the disease treated by a drug, and referenceEntity, which is a reference to a corresponding entity in a database such as Guide to Pharmacology. Examples of `ProteinDrugs` are semaglutide, a chemically modified form of GLP1, and etanercept, a fusion between the TNFalpha receptor and the Fc portion of IgG1. Naturally occurring human proteins such as insulin are considered instances of the class EntityWithAccessionedSequence rather than instances of the class ProteinDrug. Artificially modified versions of human proteins, such as glargine or lispro insulin, are considered `ProteinDrugs`. | RTX-KG2 has historically mapped Reactome's `ProteinDrug`s to `biolink:ChemicalEntity`. The most specific common ancestor of `biolink:Protein` and `biolink:Drug` is `biolink:NamedThing`, so this mapping privileges `biolink:Drug` over `biolink:Protein`. This may be a spot to clarify mapping priorities. |
| `biolink:MolecularEntity` | `Polymer` | Molecules that consist of indeterminate numbers of repeated units, and complexes that contain repeated units whose stoichiometry is variable or unknown. The repeated unit(s) (identified in the _repeatedUnit_ attribute) can be any `PhysicalEntity`. The presence of more than one _repeatedUnit_ value implies that the relative numbers of units in the polymer are unknown. If the units are present in known proportions, a `Complex` of the appropriate numbers of units is used as the _repeatedUnit_. The size range of a `Polymer` can be specified with _minUnitCount_ and _maxUnitCount_ values. | Historically, RTX-KG2 has mapped Reactome's `Polymer`s to `biolink:BiologicalEntity`. However, given the emphasis on molecules in Reactome's definition, these nodes seem better categorized under `biolink:MolecularEntity`. |
| `biolink:BiologicalEntity` | `EntityWithAccessionedSequence` | The subclass of `GenomeEncodedEntities` that can be associated through the _referenceEntity_ attribute with reference molecules in UniProt (proteins) or ENSEMBL (DNA, RNA) databases. A protein that has been partially purified and whose enzymatic properties are known but whose amino acid sequence is not, is a GenomeEncodedEntity but not an EntityWithAccessionedSequence. | |
| `biolink:BiologicalProcess` | `Polymerisation` | A subclass of `ReactionLikeEvent` in which two or more identical molecules or complexes are assembled into a polymer. | |
| `biolink:BiologicalProcess` | `PositiveGeneExpressionRegulation` | PositiveGeneExpressionRegulation is a subclass of PositiveRegulation that describes a direct stimulatory effect of a Regulator on a BlackBoxEvent that represents gene expression, so that the Regulator represents a complex of regulatory component(s) such as transcription factors with the target gene or mRNA molecule. The regulatory component(s) are defined as _activeUnit(s)_ of the PositiveGeneExpressionRegulaton instance. | |




------------------

## Ingest Contributors
 - Erica Wood