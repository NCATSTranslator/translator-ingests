# alliance-phenotype-association-ingest Report

{{ get_nodes_report() }}

{{ get_edges_report() }}

# Alliance Phenotype Association Ingest Pipeline

This pipeline transforms the Alliance of Genome Resources phenotype association files to kgx tsv following the Biolink Model. 

The association files used are part of the process for importing MOD data into the Alliance KG, and in this case are an initial limited format. This format doesn't supply any category/type information for the subject of the phenotype associations - which makes it a challenge whether to produce a gene to phenotype association vs a genotype to phenotype association.

This ingest solves that problem by download gene, allele and genotype files and using a post-procesing step to create lookup lists of all genes, alleles and genotypes so that a category can be assigned in the ingest process. This runs in the Makefile under the target `post-download`.  Within the koza transformation, the lists of genes, alleles and genotypes are loaded as lookup maps.

## Filtering 

This pipeline only captures phenotype associations which are expressed as a phenotype term, not handling post-composed phenotype annotations. ZFIN phenotype associations are ingested elsewhere, directly from ZFIN and mapped to ZP. 
Support for FB & SGD phenotype associations is in progress, via the https://github.com/monarch-initiative/uphenotizer project which aims to add uPheno terms necessary for post-composed FB & SGD annotations.

## Example transform

Given an entry in PHENOTYPE_MGI.json:

```
{
  "objectId": "MGI:87853",
  "phenotypeTermIdentifiers": [
    {
      "termId": "MP:0002075",
      "termOrder": 1
    }
  ],
  "phenotypeStatement": "abnormal coat/hair pigmentation",
  "evidence": {
    "publicationId": "PMID:1473152",
    "crossReference": {
      "id": "MGI:52036",
      "pages": [
        "reference"
      ]
    }
  },
  "primaryGeneticEntityIDs": [
    "MGI:3714610"
  ],
  "dateAssigned": "2010-03-15T00:00:00-04:00"
}

```

The resulting biolink class will be: 

```
    category: biolink:GeneToPhenotypicFeatureAssociation
    subject: MGI:87853
    predicate: biolink:has_phenotype
    object: MP:0002075
    publications: PMID:1473152
    primary_knowledge_source: infores:mgi
    aggregator_knowledge_source: infores:monarchinitiative|infores:agrkb
    knowledge_level: knowledge_assertion
    agent_type: manual_agent
```

