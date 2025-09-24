# Things to do on the Panther ingest

- Review panther.yaml compliance with the latest Ingest-Metadata schema specification (maybe regenerate it?)
- Implement **`get_latest_version()'** method, by using web (service API?) access to the Panther website
- (Perhaps) convert the NCBI GeneInfo table ("tx_map") into a distinct Koza mapping table (ask Kevin for help)
- Add unit test cases which better test fringe conditions, e.g., NCBI GeneInfo table remapped identifiers
- Figure out if and how the 'Type of ortholog' == [LDO, O, P, X ,LDX] should be handled (e.g., different predicates? Edge qualifiers?)
- Sync PR again with master
- Check if PR tests are passing - CodeSpell, etc.
- Conduct full production testing validation of the ingest
