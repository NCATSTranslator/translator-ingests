# For 2026-09-28 graph release (translator_kg release version `1.0.2`)

Note that this release's individual-resource graphs have their own varying release versions from `1.0.1` - `1.0.4`.

<details><summary>16 of 29 resource ingests used new source data (click for table).</summary>
<p>

| Resource | Previous source data version | New source data version |
| --- | --- | --- |
| alliance | `9.0.0` | `9.1.0` |
| bindingdb | `202606` | `202609` |
| ctd | `May_2026` | `August_2026` |
| ctkp | `3.1.38` | `4.2.1` |
| dakp | `0.5.6` | `0.5.7` |
| dgidb | `2024_12_06` | `2026-09` |
| diseases | `2026_06_12` | `2026_09_18` |
| gene2phenotype | `2026_06_21` | `2026_09_28` |
| geneticskp | `2026-06-21` | `2026-09-28` |
| go_cam | `2026-05-19` | `2026-08-05` |
| goa | `2026-05-19` | `2026-08-05` |
| gtopdb | `2026.2` | `2026.3` |
| hpoa | `2026-06-06` | `2026-09-01` |
| ncbi_gene | `2026_06_18` | `2026_09_28` |
| signor | `2026_March` | `2026_July` |
| ubergraph | `2026-05-31` | `2026-08-02` |

</p>
</details> 


### General changes:
* node-attribute taxon data: now using the key `taxon` which holds data exclusively from NodeNorm. The `taxon` node-attribute is present on `Gene`/`Protein`, `MacromolecularComplex`, `Disease`, and `PhenotypicFeature` nodes. Removed `in_taxon` and `in_taxon_label` node attributes, which stored resource-derived taxon data.
* NodeNorm is using the new Babel release `2026jul22`, which had wide-ranging impacts on the graph: increases in normalized nodes/edges for some resources, decreases for other resources, and changes in Node categories. Tried to note the major edge increases/decreases in individual-resource notes below. Overall, the use of the new Babel lead to a small increase in final normalized nodes/edges.
* Metadata changes, including file names/contents (including versioning).

### Alliance
* substantial increase in edges, due to new Babel update including support for MP, EMAPA, and MGI IDs.

### BindingDB:
* edge attribute for affinity data changed from `has_affinity` to `has_supporting_studies`

### ChEMBL:
* overall increase in edges due to new Babel update (more successfully-normalized nodes). 

### CTD:
* Small subset of "chem `affects` gene" edges (<1%) changed to "gene `affects` chem" after fixing bug in directionality
* Decrease in `qualified_predicate` on edges (now only added if `object_direction_qualifier` present)
* Added `directly_physically_interacts_with` edges from ingesting binding data
* Added `affects_sensitivity_to`, `decreases_sensitivity_to`, and `increases_sensitivity_to` edges from ingesting "response to substance" data (majority of these edges are gene→chem)

### CTKP:
* big increase in edges due to major data update, which included the addition of non-chemical interventions (subject, mostly `Procedure`), and new Babel update (more successfully-normalized nodes). 

### DGIdb:
* New `supporting_data_sources` added to handle resource's `2026-09` data

### GeneticsKP:
* big decrease in edges, mainly because resource appears to have removed edges for the following 6 Node categories: `biolink:AnatomicalEntity`, `biolink:ChemicalEntity`, `biolink:ClinicalAttribute`, `biolink:InformationContentEntity`, `biolink:Procedure`, `biolink:SmallMolecule`

### GO-CAM:
* substantial changes to predicates used (now mostly `biolink:regulates` and `biolink:precedes`), with added qualifiers. This is due to code/data-modeling changes, particularly the resource predicate mappings. 
* added edge attribute `has_evidence_of_type`
* may have other changes due to code refactor/data-modeling changes. See https://github.com/NCATSTranslator/translator-ingests/pull/350 for details.

### GOA:
* replaced invalid `biolink:involved_in` predicate with valid `biolink:actively_involved_in`
* added edges involving `MacromolecularComplex`, due to new Babel update's support for ComplexPortal IDs

### HPOA:
* Big increase in `associated_with` edges, mostly because 6/21 build used broken/truncated data file. See https://github.com/NCATSTranslator/Feedback/issues/1337 for details.
* Remove negated edges (now filtered out) and `negated` edge attribute
* Potentially diff values for edge attributes `has_evidence_of_type` and `agent_type` because now derived from source data
* small decrease in edges due to new Babel update, from terms that now fail normalization (mostly HP mode-of-inheritance and onset terms)

### ICEES:
* `agent_type` changed from `not_provided` to `data_analysis_pipeline`

### PathBank
* small decrease in `occurs_in` edges due to new Babel update, from potentially-obsolete terms that now fail normalization (GO:0005615 *extracellular space* and GO:0005620 *periplasmic space*)

### TMKP:
* Added edge attribute `has_confidence_score` after bug fix
* Changed `knowledge_level` on `treats_or_applied_or_studied_to_treat` edges from `not_provided` back to `knowledge_assertion`, after parser fix
* Fix to `has_supporting_study_result`'s nested attributes, and now the `attribute_type_id` for the publication is `biolink:publications` - not `biolink:supporting_document`.

### Ubergraph:
* temporarily remove data for 2 CURIEs (`NCIT:C184960` and `NCIT:C16375`), due to a NodeNorm bug that leads to validation failure and build blocked. See https://github.com/NCATSTranslator/translator-ingests/pull/535 for details.
* increase in edges due to new Babel update, mostly the support for MP IDs
