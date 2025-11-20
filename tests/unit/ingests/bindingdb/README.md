# BindingDB Unit Test Data

This directory contains test data for the BindingDB ingest (largely Anthropic CLAUDE.ai extracted and documented).

## Files

### `test_data.py`
Contains Python dictionaries representing real BindingDB records for Homo sapiens targets. Import these in your tests:

```python
from tests.unit.ingests.bindingdb.test_data import (
    CASPASE3_KI_RECORD,
    CASPASE1_KI_RECORD,
    CASPASE1_WEAK_KI_RECORD,
    NO_PMID_RECORD
)
```

### `test_data_homo_sapiens.tsv`
TSV file with the same data in tab-separated format. Can be used for integration tests or manual inspection.

### `TEST_DATA.md`
Detailed documentation of each test record with expected transformation results.

## Test Records

### 1. `CASPASE3_KI_RECORD`
- **Target**: Caspase-3 (UniProt: P42574)
- **Ligand**: Thiophene Scaffold 47c (MonomerID: 219)
- **Binding**: Ki = 90 nM
- **PMID**: 12408711
- **IDs**: PubChem CID 5327301, ChEMBL CHEMBL3885650

### 2. `CASPASE1_KI_RECORD`
- **Target**: Caspase-1 (UniProt: P29466)
- **Ligand**: Inhibitor 3 (MonomerID: 220)
- **Binding**: Ki = 160 nM
- **PMID**: 12408711
- **IDs**: PubChem CID 5327302

### 3. `CASPASE1_WEAK_KI_RECORD`
- **Target**: Caspase-1 (UniProt: P29466)
- **Ligand**: Pyridine Scaffold 4 (MonomerID: 221)
- **Binding**: Ki = 3900 nM (weaker binder)
- **PMID**: 12408711
- **IDs**: PubChem CID 5327304

### 4. `NO_PMID_RECORD`
- **Purpose**: Test record filtering when PMID is missing
- **Expected**: Should be skipped/return None

## Example Usage in test_bindingdb.py

```python
import pytest
from tests.unit.ingests.bindingdb.test_data import (
    CASPASE3_KI_RECORD,
    CASPASE1_KI_RECORD,
    NO_PMID_RECORD
)
from translator_ingest.ingests.bindingdb.bindingdb import transform_ingest_by_record

@pytest.mark.parametrize(
    "test_record,expected_nodes,expected_edge",
    [
        (
            NO_PMID_RECORD,
            None,  # Should be filtered out
            None,
        ),
        (
            CASPASE3_KI_RECORD,
            [
                {
                    "id": "BindingDB:219",  # or appropriate ID format
                    "name": "Thiophene Scaffold 47c",
                    "category": ["biolink:ChemicalEntity"]
                },
                {
                    "id": "UniProtKB:P42574",
                    "name": "Caspase-3",
                    "category": ["biolink:Protein"]
                },
            ],
            {
                "category": ["biolink:ChemicalAffectsGeneAssociation"],
                "subject": "BindingDB:219",
                "predicate": "biolink:affects",
                "object": "UniProtKB:P42574",
                "publications": ["PMID:12408711"],
                "qualifiers": [
                    {
                        "qualifier_type_id": "biolink:binding_constant_ki",
                        "qualifier_value": "90"
                    }
                ],
            },
        ),
    ],
)
def test_ingest_transform(test_record, expected_nodes, expected_edge):
    result = transform_ingest_by_record(mock_koza, test_record)
    validate_transform_result(
        result=result,
        expected_nodes=expected_nodes,
        expected_edges=expected_edge,
        node_test_slots=NODE_TEST_SLOTS,
        edge_test_slots=ASSOCIATION_TEST_SLOTS,
    )
```

## Notes

- All records are from the same publication (PMID: 12408711)
- All targets are Human (Homo sapiens)
- All measurements done at pH 7.4, 25°C
- IDs may need to be adjusted based on final BindingDB transform implementation
- The actual node and edge structure will depend on your transform function implementation

## Data Source

Extracted from `data/bindingdb/BindingDB_All.tsv` using:
```bash
awk -F'\t' '$8 == "Homo sapiens" && length($20) > 0' BindingDB_All.tsv | head -4
```

Where column 8 is "Target Source Organism" and column 20 is "PMID".
