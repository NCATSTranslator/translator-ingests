# Clinical Trials Knowledge Provider (CTKP) Reference Ingest Guide (RIG)

---------------

## Supporting Data Source Information

  | Source  | Infores | Access URL |  File  |  Description  | 
  |----------|----------|----------|----------|----------|
  | Aggregate Analysis of ClinicalTrial.gov (AACT)  | [infores:aact](https://w3id.org/information-resource-registry/aact) |   |   |   |

### Additional Notes:
- 

-----------------------

## Translator Knowledge Source Information

### Infores
- [infores:ctkp](https://w3id.org/information-resource-registry/ctkp)
  
### Description
 The Clinical Trials KP provides information on Clinical Trials, ultimately derived from researcher submissions to clinicaltrials.gov, via the Aggregate Analysis of Clinical Trials (AACT) database. Information on select trials includes the NCT Identifier of the trial, interventions used, diseases/conditions relevant to the trial, adverse events, etc.  

### Source Category(ies)
- [Primary Knowledge Source](https://biolink.github.io/biolink-model/primary_knowledge_source/)   

### Citation
- 

### Terms of Use
- 

### Data Access Locations
- 
   
### Provision Mechanisms and Formats
- Mechanism(s): 
- Formats: 
   
### Releases and Versioning


### Utility
CTKP provides an improved version of clinical trials information that addresses quality and computability issues with sources it uses (AACT, ct.gov), as well as treatment assertions derived from this data. This type of knowledge is critical for many Translator query types. 
   
### Edge Types

| # | Association Type | Subject Category |  Predicate | Object Category | Qualifier Types |  AT / KL  | Edge Properties |  UI Explanation |
|----------|----------|----------|----------|----------|----------|---------|----------|----------|
| 1 | Association | ChemicalEntity, MolecularMixture  | in_clinical_trials_for | DiseaseOrPhenotypicFeature  |  n/a  |  manual_agent, knowledge_assertion  |   | The `in_clinical_trials_for` predicate reports that an intervention was the tested in a clinical trial for a particular disease - based on a registered trial in ct.gov. |
| 2 | Association | ChemicalEntity, MolecularMixture  | treats | DiseaseOrPhenotypicFeature  |  n/a  |  manual_agent, knowledge_assertion  |   |  The `treats` predicate reports here that an intervention was shown to successfully treat a particular disease in virtue of its passing Phase 3 or being interrogated in a Phase 4 trial. |

**Notes/Rationale (o)**:


### Node Types

| Biolink Category |  Source Identifier Type(s) | Notes |
|------------------|----------------------------|--------|
| ChemicalEntity |    |    |
| MolecularMixture  |   |   |
| Small Molecule  |  |  |
| DiseaseOrPhenotypicFeature  |   |

-----------------

## Provenance Information

### Ingest Contributors
- **Gwenlyn Glusman**: code author, domain expertise
- **Matthew Brush**: data modeling

### Artifacts:


### Additional Notes (o)
