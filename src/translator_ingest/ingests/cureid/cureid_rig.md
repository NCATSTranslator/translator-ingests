# CURE ID

## Source Information

**InfoRes ID:** infores:cureid

**Description:** CURE ID is a free platform developed by FDA and NIH to share novel uses of existing drugs - also known as drug repurposing/off-label use - and explore what others have tried. The goal is to find potential treatments for challenging diseases that lack good treatment options. Our mission is to use the information collected from users sharing their treatment experiences publicly on CURE ID to help inform what drugs are studied in clinical trials. The findings from these trials will enable the medical community to learn whether a drug is effective or not for it’s new use.

**Citations:**
- TODO: ref goes here

**Data Access Locations:**
- https://opendata.ncats.nih.gov/public/cureid/
- https://cure.ncats.io/

**Data Provision Mechanisms:** file_download

**Data Formats:** tsv

## Ingest Information

**Ingest Categories:** primary_knowledge_provider

**Utility:** CURE ID is a user documented source of real world evidence on drug repurposing opportunities for rare and difficult to treat diseases. It provides chemical-disease associations, adverse events, and treatment outcomes, among other information. Information is collected from  case reports and series published in the medical literature.

**Scope:** This initial ingest focuses on case report entities and relationships for RASopathies as documented in CURE ID.

### Relevant Files

| File Name | Location | Description |
| --- | --- | --- |
| cureid_data.tsv | https://opendata.ncats.nih.gov/public/cureid/ | Manually and LLM curated associations between chemicals, diseases, phenotypic features, genes, and variants from case reports in Cure ID and series published in the medical literature. |

### Included Content

| File Name | Included Records | Fields Used |
| --- | --- | --- |
| cureid_data.tsv | All records | subject_type, subject_final_curie, subject_final_label, object_type, object_final_curie, object_final_label, association_category, biolink_predicate, outcome, pmid, link |

## Target Information

**Target InfoRes ID:** infores:cureid

### Edge Types

| Subject Categories | Predicates | Object Categories | Knowledge Level | Agent Type | UI Explanation |
| --- | --- | --- | --- | --- | --- |
| biolink:ChemicalEntity, biolink:SmallMolecule | biolink:applied_to_treat | biolink:Disease, biolink:PhenotypicFeature | knowledge_assertion | manual_agent | TODO |
| biolink:SmallMolecule | biolink:has_adverse_event | biolink:Disease, biolink:PhenotypicFeature | knowledge_assertion | manual_agent | TODO |
| biolink:Disease | biolink:has_phenotype | biolink:Disease, biolink:PhenotypicFeature | knowledge_assertion | manual_agent | TODO |
| biolink:Gene | biolink:gene_associated_with_condition | biolink:Disease | knowledge_assertion | manual_agent | TODO |

### Node Types

| Node Category | Source Identifier Types | Additional Notes |
| --- | --- | --- |
| biolink:SmallMolecule | CHEBI |  |
| biolink:Disease | MONDO, UMLS |  |
| biolink:PhenotypicFeature | HP, NCIT, UMLS |  |
| biolink:Gene | NCBIGene |  |
| biolink:ChemicalEntity | UNII |  |

