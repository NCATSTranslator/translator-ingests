# alliance-genotype Report

{{ get_nodes_report() }}

{{ get_edges_report() }}

{ % include 'docs/nodes_report.md' %}
{ % include 'docs/edges_report.md' %}

# Alliance Genotype Ingest Pipeline

The Alliance of Genome Resources contains a subset of model organism data from member databases that is harmonized to the same model. Over time, as the alliance adds additional data types, individual MOD ingests can be replaced by collective Alliance ingest. The Alliance has bulk data downloads, ingest data formats, and an API. The preference should be bulk downloads first, followed by ingest formats, finally by API calls. In some cases it may continue to be more practical to load from individual MODs when data is not yet fully harmonized in the Alliance.

* [Alliance Bulk Downloads](https://www.alliancegenome.org/downloads)
* [Alliance schemas](https://github.com/alliance-genome/agr_schemas)

This pipeline converts Alliance AGM (Affected Genomic Model) files from MGI, ZFIN & RGD to KGX TSV following the Biolink Model. Additionally, it processes Alliance allele data and creates relationships between genotypes, alleles, and genes.

## [Genotypes](#genotype)

Genotypes from Alliance member databases (currently MGI, ZFIN, and RGD) are loaded from AGM JSON files. 

__**Biolink captured**__

* biolink:Genotype
    * id (row['primaryID'])
    * name (row['name'])
    * type (row['subtype'] if available)
    * in_taxon (row['taxonId'])
    * in_taxon_label (mapped from taxon ID)

Example transform:
```
 {
     'primaryID': 'ZFIN:ZDB-FISH-150901-9455', 
      'name': 'acvr1l<sup>sk42/sk42</sup>; f2Tg',
      'affectedGenomicModelComponents': [
            {'alleleID': 'ZFIN:ZDB-ALT-060821-6', 'zygosity': 'GENO:0000137'},
            {'alleleID': 'ZFIN:ZDB-ALT-100701-1', 'zygosity': 'GENO:0000136'}],
       'crossReference': { 'id': 'ZFIN:ZDB-FISH-150901-9455', 'pages': ['Fish']}, 
       'taxonId': 'NCBITaxon:7955'
}
```

Is transformed into
```
    category: biolink:Genotype
    id: ZFIN:ZDB-FISH-150901-9455
    name: acvr1l<sup>sk42/sk42</sup>; f2Tg
    in_taxon: NCBITaxon:7955
    in_taxon_label: Danio rerio
```

## [Alleles](#allele)

Alleles (sequence variants) are loaded from Alliance VARIANT-ALLELE TSV files.

__**Biolink captured**__

* biolink:SequenceVariant
    * id (row['AlleleId'])
    * name (row['AlleleSymbol'])
    * synonym (parsed from row['AlleleSynonyms'])
    * in_taxon (row['Taxon'])
    * in_taxon_label (row['SpeciesName'])

Example transform:
```
# Source data
{
    "AlleleId": "ZFIN:ZDB-ALT-123456-7",
    "AlleleSymbol": "tyr<b1>",
    "AlleleSynonyms": "tyrosinase b1,tyr-b1",
    "Taxon": "NCBITaxon:7955",
    "SpeciesName": "Danio rerio",
    "AlleleAssociatedGeneId": "ZFIN:ZDB-GENE-000508-1",
    "AlleleAssociatedGeneSymbol": "tyr",
    "VariantsTypeId": "SO:0001059"
}
```

Is transformed into:
```
category: biolink:SequenceVariant
id: ZFIN:ZDB-ALT-123456-7
name: tyr<b1>
in_taxon: NCBITaxon:7955
in_taxon_label: Danio rerio
synonym: ["tyrosinase b1", "tyr-b1"]
```

## [Genotype to Variant Association](#genotype_to_variant)

Associations between genotypes and their component alleles (variants) are created from the AGM JSON files.

__**Biolink captured**__

* biolink:GenotypeToVariantAssociation
    * id (random uuid)
    * subject (genotype.id from row['primaryID'])
    * predicate (biolink:has_sequence_variant)
    * object (allele['alleleID'] from row['affectedGenomicModelComponents'])
    * qualifier (allele['zygosity'] if available)
    * primary_knowledge_source (mapped from source prefix in the primaryID)
    * aggregator_knowledge_source (["infores:monarchinitiative", "infores:agrkb"])
    * knowledge_level (knowledge_assertion)
    * agent_type (manual_agent)

Example transform:
```
# Source data (partial)
{
    'primaryID': 'ZFIN:ZDB-FISH-150901-9455',
    'affectedGenomicModelComponents': [
        {'alleleID': 'ZFIN:ZDB-ALT-060821-6', 'zygosity': 'GENO:0000137'},
        {'alleleID': 'ZFIN:ZDB-ALT-100701-1', 'zygosity': 'GENO:0000136'}
    ],
    'taxonId': 'NCBITaxon:7955'
}
```

Is transformed into:
```
category: biolink:GenotypeToVariantAssociation
id: fa8b4567-e9c6-4ebd-a6f1-fcd82f3f0a83  # random UUID
subject: ZFIN:ZDB-FISH-150901-9455
predicate: biolink:has_sequence_variant
object: ZFIN:ZDB-ALT-060821-6
qualifier: GENO:0000137
primary_knowledge_source: infores:zfin
aggregator_knowledge_source: ["infores:monarchinitiative", "infores:agrkb"]
knowledge_level: knowledge_assertion
agent_type: manual_agent
```

And a second association:
```
category: biolink:GenotypeToVariantAssociation
id: 8a7d6c2f-3b58-4e91-9f12-a7b8e6d234e0  # random UUID 
subject: ZFIN:ZDB-FISH-150901-9455
predicate: biolink:has_sequence_variant
object: ZFIN:ZDB-ALT-100701-1
qualifier: GENO:0000136
primary_knowledge_source: infores:zfin
aggregator_knowledge_source: ["infores:monarchinitiative", "infores:agrkb"]
knowledge_level: knowledge_assertion
agent_type: manual_agent
```

## [Genotype to Gene Association](#genotype_to_gene)

Associations between genotypes and genes are created by linking through alleles. The allele to gene mapping is precomputed from the VARIANT-ALLELE files.

__**Biolink captured**__

* biolink:GenotypeToGeneAssociation
    * id (random uuid)
    * subject (genotype.id from row['primaryID'])
    * predicate (biolink:has_part)
    * object (gene ID from allele_to_gene_lookup)
    * primary_knowledge_source (mapped from source prefix in the primaryID)
    * aggregator_knowledge_source (["infores:monarchinitiative", "infores:agrkb"])
    * knowledge_level (knowledge_assertion)
    * agent_type (manual_agent)

Example transform:
```
# Source data (partial)
{
    'primaryID': 'ZFIN:ZDB-FISH-150901-9455',
    'affectedGenomicModelComponents': [
        {'alleleID': 'ZFIN:ZDB-ALT-060821-6', 'zygosity': 'GENO:0000137'}
    ],
    'taxonId': 'NCBITaxon:7955'
}

# With allele_to_gene_lookup containing:
# 'ZFIN:ZDB-ALT-060821-6': {'AlleleAssociatedGeneId': 'ZFIN:ZDB-GENE-030616-554'}
```

Is transformed into:
```
category: biolink:GenotypeToGeneAssociation
id: 5d9c3a17-85f2-4bd2-9e3f-c8a7b45e1234  # random UUID
subject: ZFIN:ZDB-FISH-150901-9455
predicate: biolink:has_part
object: ZFIN:ZDB-GENE-030616-554
primary_knowledge_source: infores:zfin
aggregator_knowledge_source: ["infores:monarchinitiative", "infores:agrkb"]
knowledge_level: knowledge_assertion
agent_type: manual_agent
```

## [Variant to Gene Association](#variant_to_gene)

Associations between alleles (variants) and genes are created from the VARIANT-ALLELE files.

__**Biolink captured**__

* biolink:VariantToGeneAssociation
    * id (random uuid)
    * subject (row['AlleleId'])
    * predicate (biolink:is_sequence_variant_of)
    * original_predicate (row['VariantsTypeId'])
    * object (row['AlleleAssociatedGeneId'])
    * primary_knowledge_source (mapped from the source prefix in the AlleleId)
    * aggregator_knowledge_source (["infores:monarchinitiative", "infores:agrkb"])
    * knowledge_level (knowledge_assertion)
    * agent_type (manual_agent)

Example transform:
```
# Source data
{
    "AlleleId": "ZFIN:ZDB-ALT-123456-7",
    "AlleleSymbol": "tyr<b1>",
    "Taxon": "NCBITaxon:7955",
    "SpeciesName": "Danio rerio",
    "AlleleAssociatedGeneId": "ZFIN:ZDB-GENE-000508-1",
    "AlleleAssociatedGeneSymbol": "tyr",
    "VariantsTypeId": "SO:0001059"  # sequence_alteration
}
```

Is transformed into:
```
category: biolink:VariantToGeneAssociation
id: 2e4c6a8d-1b3f-4e7a-9d5c-8a6b4c2e3d5f  # random UUID
subject: ZFIN:ZDB-ALT-123456-7
predicate: biolink:is_sequence_variant_of
original_predicate: SO:0001059
object: ZFIN:ZDB-GENE-000508-1
primary_knowledge_source: infores:zfin
aggregator_knowledge_source: ["infores:monarchinitiative", "infores:agrkb"]
knowledge_level: knowledge_assertion
agent_type: manual_agent
```

## Citation

Harmonizing model organism data in the Alliance of Genome Resources. 2022. Alliance of Genome Resources Consortium. Genetics, Volume 220, Issue 4, April 2022. Published Online: 25 February 2022. doi: doi.org/10.1093/genetics/iyac022. PMID: 35380658;  PMCID: PMC8982023.
