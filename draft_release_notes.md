This release note covers merge commits from 2026-06-21 (last formal KGX release) to now. 

## Major Resource Code Changes
* multiple ingests: **remove source-derived taxon node properties** #528
* **bgee, go_cam, goa**: fix `get_latest_version()` failures #446 
* **bindingdb, go_cam, gtopdb**: major parser changes plus test updates #350
* **bgee**: fix download links #499
* **chembl**: major performance improvement #498
* **ctd**: fix bug in "chem–gene interaction edge" direction #401; add sensitivity #402 and binding #464 edges
* **dgidb**: dynamically pull latest data release and parser rewrite to handle latest data at the time (2026-09) #527
* **goa**: fix incorrect predicate #472
* **hpoa**: derive `has_evidence_of_type` and `agent_type` from data, plus a general, minor addition of `encoding="utf-8"` to file I/O #462; remove negated edges #489
* **icees**: improve parsing of data, update RIG #467
* **tmkp**: fix ingestion of confidence score #511; fix parsing of download file (previously changed) #514


## Minor Resource Code Changes
* **cohd, hpoa**: test updates #496
* **ctkp**: code and RIG changes to support improved CTKP data #468
* **drug_rep_hub**: minor, add a test #502
* **geneticskp**: change download location to allow for data updates #469
* **hpoa**: address technical debt #509
* **signor**: remove ingest of edges involving complexes, update RIG #491


## General Pipeline Changes
* Make data / releases / logs locations configurable #520 
* Release metadata improvements #515 
* Build multi-source KGs from releases, not data #443
* Record source-data download timestamp #455
* Minor: GitHub Actions version bumps #492
* Minor: clean up shared unit-test code #495
* Add env var to set ORION dependency to same biolink-model version #487
* Change KGX release versions to semantic versioning #393


## RIG-Specific Updates

From [June–July 2026 "RIG review & completion" campaign](https://github.com/NCATSTranslator/translator-ingests/issues/407) to make RIGS richer, more complete, and compliant with RIG schema `0.1.5`.

General changes: 
* Simple schema-conformance fixes to RIGS #448
* remove deprecated RIG field from scripts #501
* pin RIG schema to updated version and fix validate-rigs command #447
* update RIG template #391


<details><summary>Resource-specific PRs</summary>
<p>

* alliance #442
* bgee #475
* chembl #483
* cohd #482
* ctd #460
* ctkp #451
* cureid #471
* dakp #481
* dgidb #457
* diseases #456
* drug_rep_hub #484
* drugcentral #459
* gene2phenotype #405
* geneticskp #449
* go_cam #453
* goa #485
* gtopdb #477, #493
* intact #404
* ncbi_gene #454
* panther #466
* pathbank #479
* pubtator #461
* semmeddb #480
* sider #476
* signor #478
* tmkp #452
* ttd #458
* ubergraph #450

</p>
</details> 
