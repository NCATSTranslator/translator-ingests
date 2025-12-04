# BindingDB Test Data - Homo sapiens

This file contains test data extracted from BindingDB_All.tsv for rows where:
- `Target Source Organism According to Curator or DataSource` == "Homo sapiens"
- PMID is present

## Test Record 1 - Caspase-3 with Ki value

```python
{
    "BindingDB Reactant_set_id": "199",
    "Ligand SMILES": "CN(Cc1ccc(s1)C(=O)N[C@@H](CC(O)=O)C(=O)CSCc1ccccc1Cl)Cc1ccc(O)c(c1)C(O)=O",
    "Ligand InChI": "InChI=1S/C27H27ClN2O7S2/c1-30(12-16-6-8-22(31)19(10-16)27(36)37)13-18-7-9-24(39-18)26(35)29-21(11-25(33)34)23(32)15-38-14-17-4-2-3-5-20(17)28/h2-10,21,31H,11-15H2,1H3,(H,29,35)(H,33,34)(H,36,37)",
    "Ligand InChI Key": "FIEQQFOHZKVJLV-UHFFFAOYSA-N",
    "BindingDB MonomerID": "219",
    "BindingDB Ligand Name": "Thiophene Scaffold 47c",
    "Target Name": "Caspase-3",
    "Target Source Organism According to Curator or DataSource": "Homo sapiens",
    "Ki (nM)": "90",
    "IC50 (nM)": "",
    "Kd (nM)": "",
    "EC50 (nM)": "",
    "kon (M-1-s-1)": "",
    "koff (s-1)": "",
    "pH": "7.4",
    "Temp (C)": "25.00",
    "Curation/DataSource": "Curated from the literature by BindingDB",
    "Article DOI": "10.1021/jm020230j",
    "BindingDB Entry DOI": "10.7270/Q2B56GW5",
    "PMID": "12408711",
    "PubChem CID": "5327301",
    "PubChem SID": "8030144",
    "ChEBI ID of Ligand": "",
    "ChEMBL ID of Ligand": "CHEMBL3885650",
    "UniProt (SwissProt) Primary ID of Target Chain 1": "P42574",
    "UniProt (SwissProt) Recommended Name of Target Chain 1": "Caspase-3",
}
```

## Test Record 2 - Caspase-1 with Ki value

```python
{
    "BindingDB Reactant_set_id": "200",
    "Ligand SMILES": "OC(=O)C[C@H](NC(=O)c1ccc(CNS(=O)(=O)c2ccc(O)c(c2)C(O)=O)cc1)C=O",
    "Ligand InChI": "InChI=1S/C19H18N2O9S/c22-10-13(7-17(24)25)21-18(26)12-3-1-11(2-4-12)9-20-31(29,30)14-5-6-16(23)15(8-14)19(27)28/h1-6,8,10,13,20,23H,7,9H2,(H,21,26)(H,24,25)(H,27,28)",
    "Ligand InChI Key": "FMTZTJFXNZOBIQ-UHFFFAOYSA-N",
    "BindingDB MonomerID": "220",
    "BindingDB Ligand Name": "Inhibitor 3",
    "Target Name": "Caspase-1",
    "Target Source Organism According to Curator or DataSource": "Homo sapiens",
    "Ki (nM)": "160",
    "IC50 (nM)": "",
    "Kd (nM)": "",
    "EC50 (nM)": "",
    "kon (M-1-s-1)": "",
    "koff (s-1)": "",
    "pH": "7.4",
    "Temp (C)": "25.00",
    "Curation/DataSource": "Curated from the literature by BindingDB",
    "Article DOI": "10.1021/jm020230j",
    "BindingDB Entry DOI": "10.7270/Q2B56GW5",
    "PMID": "12408711",
    "PubChem CID": "5327302",
    "PubChem SID": "8030145",
    "ChEBI ID of Ligand": "",
    "ChEMBL ID of Ligand": "",
    "UniProt (SwissProt) Primary ID of Target Chain 1": "P29466",
    "UniProt (SwissProt) Recommended Name of Target Chain 1": "Caspase-1",
}
```

## Test Record 3 - Caspase-1 with higher Ki value

```python
{
    "BindingDB Reactant_set_id": "201",
    "Ligand SMILES": "OC(=O)C[C@H](NC(=O)c1ccc(CNS(=O)(=O)c2ccc(O)c(c2)C(O)=O)nc1)C=O",
    "Ligand InChI": "InChI=1S/C18H17N3O9S/c22-9-12(5-16(24)25)21-17(26)10-1-2-11(19-7-10)8-20-31(29,30)13-3-4-15(23)14(6-13)18(27)28/h1-4,6-7,9,12,20,23H,5,8H2,(H,21,26)(H,24,25)(H,27,28)",
    "Ligand InChI Key": "BZEURKGKCWHMON-UHFFFAOYSA-N",
    "BindingDB MonomerID": "221",
    "BindingDB Ligand Name": "Pyridine Scaffold 4",
    "Target Name": "Caspase-1",
    "Target Source Organism According to Curator or DataSource": "Homo sapiens",
    "Ki (nM)": "3900",
    "IC50 (nM)": "",
    "Kd (nM)": "",
    "EC50 (nM)": "",
    "kon (M-1-s-1)": "",
    "koff (s-1)": "",
    "pH": "7.4",
    "Temp (C)": "25.00",
    "Curation/DataSource": "Curated from the literature by BindingDB",
    "Article DOI": "10.1021/jm020230j",
    "BindingDB Entry DOI": "10.7270/Q2B56GW5",
    "PMID": "12408711",
    "PubChem CID": "5327304",
    "PubChem SID": "8030146",
    "ChEBI ID of Ligand": "",
    "ChEMBL ID of Ligand": "",
    "UniProt (SwissProt) Primary ID of Target Chain 1": "P29466",
    "UniProt (SwissProt) Recommended Name of Target Chain 1": "Caspase-1",
}
```

## Notes

- All three records are from the same publication (PMID: 12408711, DOI: 10.1021/jm020230j)
- All three are Human (Homo sapiens) targets
- Record 1: Caspase-3 inhibitor with Ki = 90 nM
- Records 2-3: Caspase-1 inhibitors with Ki = 160 nM and 3900 nM respectively
- All measurements were done at pH 7.4 and 25°C
- All have UniProt IDs for the target proteins
- Record 1 has a ChEMBL ID (CHEMBL3885650), others don't
- All have PubChem CID and SID identifiers

## Usage in test_bindingdb.py

These dictionaries can be used directly in `@pytest.mark.parametrize` to test the BindingDB transform function. You would update the test to:

1. Import the actual bindingdb transform function (not the template)
2. Use these dictionaries as test_record parameters
3. Verify that the transform creates appropriate:
   - ChemicalEntity nodes (using MonomerID or PubChem/ChEMBL IDs)
   - Protein/Gene nodes (using UniProt IDs)
   - ChemicalAffectsGeneAssociation edges with Ki/IC50/Kd/EC50 as qualifiers
