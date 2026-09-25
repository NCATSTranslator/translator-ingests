# CURE ID

## Source Information

**InfoRes ID:** infores:cureid

**Description:** CURE ID is a public FDA/NIH platform for collecting and sharing de-identified case reports describing novel uses of existing medications, including drug repurposing and off-label treatment experiences. Reports may be submitted by healthcare providers, public health officials, patients, and caregivers, and are reviewed before publication. The Translator ingest currently uses the public CURE ID RASopathies export to represent observed treatment, adverse event, phenotype, and gene-condition relationships from case reports and linked literature.

**Citations:**
- Farid T, Ruzhnikov MRZ, Duggal M, Tumas KC, Strongin S, Sid E, Fuchs SR, Sacks L, Pichard DC, Pilgrim-Grayson C, Mathé EA, Stone HA. CURE ID: A Platform to Collect Real-World Treatment Data for Drug Repurposing in Rare Genetic Disorders. Am J Med Genet C Semin Med Genet. 2025 Sep;199(3):189-193. doi: 10.1002/ajmg.c.32153. Epub 2026 Jan 7. PMID: [ 41496707 ](https://pubmed.ncbi.nlm.nih.gov/41496707/)

**Terms of Use:**
- URL: https://cure.ncats.io/terms
- Description: CURE ID terms and conditions govern use of the platform and describe case report submission criteria, privacy expectations, moderation, and use of submitted content.

**Data Access Locations:**
- https://opendata.ncats.nih.gov/public/cureid/
- https://cure.ncats.io/

**Data Provision Mechanisms:** file_download

**Data Formats:** tsv

**Data Versioning and Releases:** The public open-data directory includes a cureid_version.json metadata file with a version date and original source file name for the current RASopathies export.

**Source Status:** maintained_as_needed_updates

## Ingest Information

**Ingest Categories:** primary_knowledge_provider

**Utility:** CURE ID is a user documented source of real world evidence on drug repurposing opportunities for rare and difficult to treat diseases. It provides treatment, adverse-event, phenotype, and gene-condition observations that can help Translator surface case-report-level evidence for attempted therapies and disease presentations.

**Scope:** This ingest focuses on RASopathies case-report entities and relationships from the public CURE ID export. It emits disease, phenotype, chemical, and gene nodes plus edges for treatment use, adverse events, disease phenotypes, and gene-condition associations.

### Relevant Files

| File Name | Location | Description |
| --- | --- | --- |
| cureid_data.tsv | https://opendata.ncats.nih.gov/public/cureid/ | Manually and LLM curated associations between chemicals, diseases, phenotypic features, genes, and variants from case reports in Cure ID and series published in the medical literature. |

### Included Content

| File Name | Included Records | Fields Used |
| --- | --- | --- |
| cureid_data.tsv | All records | subject_type, subject_final_curie, subject_final_label, object_type, object_final_curie, object_final_label, association_category, biolink_predicate, outcome, pmid, link |

### Filtered Content

| File Name | Filtered Records | Rationale |
| --- | --- | --- |
| cureid_data.tsv | Records outside the public RASopathies export | The current Translator ingest is scoped to the CURE ID RASopathies export rather than the full CURE ID case-report corpus. |

### Future Content Considerations

**edge_content:** Consider expanding beyond RASopathies to additional CURE ID disease collections, such as Long COVID, once the source provides stable public exports and the ingest has been reviewed for the larger corpus.
  - Relevant files: CURE ID public exports

**edge_property_content:** Consider modeling aggregate evidence counts, supporting text, treatment outcomes, and multiple source record URLs when ingesting association-view JSONL exports rather than the current TSV export.
  - Relevant files: CURE ID association-view JSONL exports

## Target Information

### Edge Types

| Subject Categories | Predicates | Object Categories | Knowledge Level | Agent Type | UI Explanation |
| --- | --- | --- | --- | --- | --- |
| biolink:ChemicalEntity, biolink:SmallMolecule | biolink:applied_to_treat | biolink:Disease, biolink:PhenotypicFeature | knowledge_assertion | manual_agent | CURE ID provides curated case-report records in which a submitted or literature-derived treatment experience reports that an existing medication was applied to treat a disease or phenotype. The Translator edge preserves linked PubMed IDs when present and stores the CURE ID source record URL in provenance. |
| biolink:SmallMolecule | biolink:has_adverse_event | biolink:Disease, biolink:PhenotypicFeature | observation | manual_agent | CURE ID provides curated case-report records in which a submitted or literature-derived treatment experience reports an adverse event observed with a medication. The ingest maps CURE ID outcome strings to the Biolink FDA adverse event level when the source outcome supports that mapping. |
| biolink:Disease | biolink:has_phenotype | biolink:Disease, biolink:PhenotypicFeature | knowledge_assertion | manual_agent | CURE ID provides curated case-report records describing phenotypes or symptoms observed in patients with a disease. The ingest maps these disease-to-phenotype relationships to Biolink while preserving linked PubMed IDs when present. |
| biolink:Gene | biolink:associated_with | biolink:Disease | knowledge_assertion | manual_agent | CURE ID provides curated case-report records that include gene-condition relationships relevant to the reported diagnosis or treatment context. The ingest represents these as generic gene-disease associations because CURE ID does not specify a more precise causal or mechanistic predicate. |

### Node Types

| Node Category | Source Identifier Types |
| --- | --- |
| biolink:SmallMolecule | CHEBI |
| biolink:Disease | MONDO, UMLS |
| biolink:PhenotypicFeature | HP, NCIT, UMLS |
| biolink:Gene | NCBIGene |
| biolink:ChemicalEntity | UNII |

## Provenance Information

**Contributors:**
- Keith Kelleher - ingest code author
- Jessica Maine - data cleansing, domain expertise
- Tahsin Farid - domain expertise
- Keyla Tumas - domain expertise

