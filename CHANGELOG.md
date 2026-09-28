This code release is intended for use in the upcoming KGX release. So this changelog covers merged PRs from 2026-06-21 (last formal KGX release) onward.

## Major Resource Code Changes
* multiple ingests: **remove source-derived taxon node properties** https://github.com/NCATSTranslator/translator-ingests/pull/528
* **bgee**: fix download links https://github.com/NCATSTranslator/translator-ingests/pull/499
* **bgee, go_cam, goa**: fix `get_latest_version()` failures https://github.com/NCATSTranslator/translator-ingests/pull/446 
* **bindingdb, go_cam, gtopdb**: major parser changes plus test updates https://github.com/NCATSTranslator/translator-ingests/pull/350
* **chembl**: major performance improvement https://github.com/NCATSTranslator/translator-ingests/pull/498
* **ctd**: fix bug in "chem–gene interaction edge" direction https://github.com/NCATSTranslator/translator-ingests/pull/401; add sensitivity https://github.com/NCATSTranslator/translator-ingests/pull/402 and binding https://github.com/NCATSTranslator/translator-ingests/pull/464 edges
* **dgidb**: dynamically pull latest data release and parser rewrite to handle latest data at the time (2026-09) https://github.com/NCATSTranslator/translator-ingests/pull/527
* **goa**: fix incorrect predicate https://github.com/NCATSTranslator/translator-ingests/pull/472
* **hpoa**: derive `has_evidence_of_type` and `agent_type` from data, plus a general, minor addition of `encoding="utf-8"` to file I/O https://github.com/NCATSTranslator/translator-ingests/pull/462; remove negated edges https://github.com/NCATSTranslator/translator-ingests/pull/489
* **icees**: improve parsing of data, update RIG https://github.com/NCATSTranslator/translator-ingests/pull/467
* **tmkp**: fix ingestion of confidence score https://github.com/NCATSTranslator/translator-ingests/pull/511; fix parsing of download file (previously changed) https://github.com/NCATSTranslator/translator-ingests/pull/514


## Minor Resource Code Changes
* **cohd, hpoa**: test updates https://github.com/NCATSTranslator/translator-ingests/pull/496
* **ctkp**: code and RIG changes to support improved CTKP data https://github.com/NCATSTranslator/translator-ingests/pull/468
* **drug_rep_hub**: minor, add a test https://github.com/NCATSTranslator/translator-ingests/pull/502
* **geneticskp**: change download location to allow for data updates https://github.com/NCATSTranslator/translator-ingests/pull/469
* **hpoa**: address technical debt https://github.com/NCATSTranslator/translator-ingests/pull/509
* **semmeddb**: refactor LLM-PMID-checker filter, merged to main so we can build entire graph from main branch again https://github.com/NCATSTranslator/translator-ingests/pull/530
* **signor**: remove ingest of edges involving complexes, update RIG https://github.com/NCATSTranslator/translator-ingests/pull/491


## General Pipeline Changes
* ORION dependency bump to v2.0.9 and update our code to take advantage of it. **Changes metadata file names/contents (including versioning**), and **explicitly requests taxa/description data during the NodeNorming process and adds them as node properties**. https://github.com/NCATSTranslator/translator-ingests/pull/533
* Fix issues with OVERWRITE and error handling in merge_single https://github.com/NCATSTranslator/translator-ingests/pull/532
* Make data / releases / logs locations configurable https://github.com/NCATSTranslator/translator-ingests/pull/520 
* Release metadata improvements https://github.com/NCATSTranslator/translator-ingests/pull/515 
* Build multi-source KGs from releases, not data https://github.com/NCATSTranslator/translator-ingests/pull/443
* Record source-data download timestamp https://github.com/NCATSTranslator/translator-ingests/pull/455
* Minor: GitHub Actions version bumps https://github.com/NCATSTranslator/translator-ingests/pull/492
* Minor: clean up shared unit-test code https://github.com/NCATSTranslator/translator-ingests/pull/495
* Add env var to set ORION dependency to same biolink-model version https://github.com/NCATSTranslator/translator-ingests/pull/487
* Change KGX release versions to semantic versioning https://github.com/NCATSTranslator/translator-ingests/pull/393


## RIG-Specific Updates

From [June–July 2026 "RIG review & completion" campaign](https://github.com/NCATSTranslator/translator-ingests/issues/407) to make RIGS richer, more complete, and compliant with RIG schema `0.1.5`.

General changes: 
* Simple schema-conformance fixes to RIGS https://github.com/NCATSTranslator/translator-ingests/pull/448
* remove deprecated RIG field from scripts https://github.com/NCATSTranslator/translator-ingests/pull/501
* pin RIG schema to updated version and fix validate-rigs command https://github.com/NCATSTranslator/translator-ingests/pull/447
* update RIG template https://github.com/NCATSTranslator/translator-ingests/pull/391


<details><summary>Resource-specific PRs</summary>
<p>

* alliance https://github.com/NCATSTranslator/translator-ingests/pull/442
* bgee https://github.com/NCATSTranslator/translator-ingests/pull/475
* chembl https://github.com/NCATSTranslator/translator-ingests/pull/483
* cohd https://github.com/NCATSTranslator/translator-ingests/pull/482
* ctd https://github.com/NCATSTranslator/translator-ingests/pull/460
* ctkp https://github.com/NCATSTranslator/translator-ingests/pull/451
* cureid https://github.com/NCATSTranslator/translator-ingests/pull/471
* dakp https://github.com/NCATSTranslator/translator-ingests/pull/481
* dgidb https://github.com/NCATSTranslator/translator-ingests/pull/457
* diseases https://github.com/NCATSTranslator/translator-ingests/pull/456
* drug_rep_hub https://github.com/NCATSTranslator/translator-ingests/pull/484
* drugcentral https://github.com/NCATSTranslator/translator-ingests/pull/459
* gene2phenotype https://github.com/NCATSTranslator/translator-ingests/pull/405
* geneticskp https://github.com/NCATSTranslator/translator-ingests/pull/449
* go_cam https://github.com/NCATSTranslator/translator-ingests/pull/453
* goa https://github.com/NCATSTranslator/translator-ingests/pull/485
* gtopdb https://github.com/NCATSTranslator/translator-ingests/pull/477, https://github.com/NCATSTranslator/translator-ingests/pull/493
* intact https://github.com/NCATSTranslator/translator-ingests/pull/404
* ncbi_gene https://github.com/NCATSTranslator/translator-ingests/pull/454
* panther https://github.com/NCATSTranslator/translator-ingests/pull/466
* pathbank https://github.com/NCATSTranslator/translator-ingests/pull/479
* pubtator https://github.com/NCATSTranslator/translator-ingests/pull/461
* semmeddb https://github.com/NCATSTranslator/translator-ingests/pull/480
* sider https://github.com/NCATSTranslator/translator-ingests/pull/476
* signor https://github.com/NCATSTranslator/translator-ingests/pull/478
* tmkp https://github.com/NCATSTranslator/translator-ingests/pull/452
* ttd https://github.com/NCATSTranslator/translator-ingests/pull/458
* ubergraph https://github.com/NCATSTranslator/translator-ingests/pull/450

</p>
</details> 
