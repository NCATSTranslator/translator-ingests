# Things to do on the Panther ingest

- _**NCBI Gene 'fallback' mappings**_:
    ~~- (Perhaps) convert the NCBI GeneInfo table ("ntg_map") into a distinct Koza mapping table (ask Kevin for help)~~
    ~~- Add unit test cases which better test fringe conditions, e.g., NCBI GeneInfo table remapped identifiers~~
    - Computationally heavy; decision is to exclude the NCBIGene identifier mapping for now, in the first iteration but to just fall back onto the UnitProt identifier instead (need to note this limitation in the RIG?)
~~- Figure out if and how the 'Type of ortholog' == [LDO, O, P, X ,LDX] should be handled~~  - ignore, see http://data.pantherdb.org/ftp/ortholog/17.0/README
~~- Sync PR again with master~~
- Check if PR tests are passing - CodeSpell, etc. 
- Conduct full production testing ingest generation of a KGX file
    - Need to use Linux or OSX make to run this (**`just run`** doesn't yet work on Windows)
- consider if any of the ORION metadata (below) should be added into the Translator Ingest project RIG or elsewhere

## ORION Metadata

```json
{
    "id": "PANTHER",
    "name": "Protein Analysis THrough Evolutionary Relationships (PANTHER)",
    "description": "The Protein ANalysis THrough Evolutionary Relationships (PANTHER) classification system provides an openly available annotation library of gene family phylogenetic trees, with persistent identifiers attached to all nodes in the trees and annotation of each protein member of the family by its family and protein class, subfamily, orthologs, paralogs, GO Phylogenetic Annotation Project function and Reactome pathways.",
    "url": "https://www.pantherdb.org/",
    "attribution": "http://pantherdb.org/publications.jsp#HowToCitePANTHER",
    "citation": "https://doi.org/10.1002/pro.4218",
    "full_citation": "Paul D. Thomas, Dustin Ebert, Anushya Muruganujan, Tremayne Mushayahama, Laurent-Philippe Albou and Huaiyu Mi\nProtein Society. 2022;31(1):8-22. doi:10.1002/pro.4218",
    "license": "http://pantherdb.org/tou.jsp",
    "contentUrl": "ftp.pantherdb.org/sequence_classifications/"
}
```