# Things to do on the Panther ingest

- _**NCBI Gene 'fallback' mappings**_:
    ~~- (Perhaps) convert the NCBI GeneInfo table ("ntg_map") into a distinct Koza mapping table (ask Kevin for help)~~
    ~~- Add unit test cases which better test fringe conditions, e.g., NCBI GeneInfo table remapped identifiers~~
    - Computationally heavy; decision is to exclude the NCBIGene identifier mapping for now, in the first iteration but to just fall back onto the UnitProt identifier instead (need to note this limitation in the RIG?)
~~- Figure out if and how the 'Type of ortholog' == [LDO, O, P, X ,LDX] should be handled~~  - ignore, see http://data.pantherdb.org/ftp/ortholog/17.0/README
~~- Sync PR again with master~~
- Check if PR tests are passing - CodeSpell, etc. - is a Docker run with Python 3.8 failing(?) Why?
- Conduct full production testing generation of the KGX file of the ingest
