# Things to do on the Panther ingest

- (Perhaps) convert the NCBI GeneInfo table ("ntg_map") into a distinct Koza mapping table (ask Kevin for help)
- Add unit test cases which better test fringe conditions, e.g., NCBI GeneInfo table remapped identifiers
- Figure out if and how the 'Type of ortholog' == [LDO, O, P, X ,LDX] should be handled (e.g., different predicates? Edge qualifiers?)
- Sync PR again with master
- Check if PR tests are passing - CodeSpell, etc.
- Conduct full production testing validation of the ingest
