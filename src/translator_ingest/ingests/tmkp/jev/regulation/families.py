"""
The TMKP regulation edge families this pipeline handles. Both are (regulator) affects (target), qualified causes
increased/decreased activity_or_abundance; they differ in what the regulator is.

- chem_gene: chemical -> protein, relation biolink:ChemicalToGeneAssociation
- gene_gene: protein -> protein, relation biolink:GeneRegulatoryRelationship
"""

PROTEIN = "human protein (a UniProt entry: entry name, then protein name)"

FAMILIES = {
    "chem_gene": {
        "relation": "biolink:ChemicalToGeneAssociation",
        "kind": {"subject": "chemical or drug", "object": PROTEIN},
        "chemical_regulator": True,
    },
    "gene_gene": {
        "relation": "biolink:GeneRegulatoryRelationship",
        "kind": {"subject": PROTEIN, "object": PROTEIN},
        "chemical_regulator": False,
    },
}


def in_family(family: str, predicate: str, relation: str) -> bool:
    """
    True for a TMKP edge row of the family (treats edges share some relations and are filtered separately).

    >>> in_family("gene_gene", "biolink:affects", "biolink:GeneRegulatoryRelationship")
    True
    >>> in_family("chem_gene", "biolink:treats_or_applied_or_studied_to_treat", "biolink:ChemicalToGeneAssociation")
    False
    """
    return relation == FAMILIES[family]["relation"] and predicate != "biolink:treats_or_applied_or_studied_to_treat"
