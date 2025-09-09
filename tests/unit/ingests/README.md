# Knowledge Source Ingest-specific Unit Tests

This directory contains unit tests for the Knowledge Source Ingests.

The [**_ingests_template unit test**](./_ingest_template/test_ingest_template.py) module contains the working unit tests for the sample [example ingest template](../../../src/translator_ingest/ingests/_ingest_template/_ingest_template.py).  These unit tests use reusable (shared) generic code for mock koza data and a "**test runner**" compliant with Translator Ingests certain parse method signatures (additional details are provided below).

## Mock Koza Data

**`MockKozaWriter`** and **`MockKozaTransform`** classes, with an example **`mock_koza_transform()`** pytest fixtures are provided [here](./__init__.py). In some cases, you will need to replace the **`mock_koza_transform()`** fixture with your own custom code, to provide more customized mock data for unit tests of the specific ingest, which will consume such a fixture and give it directly to the target **`@koza.transform_record`** decorated method that generates the output data for validation in the test runner method (next section).

## Transform Record KnowledgeGraph Result Validation

A generic [**`validate_transform_result()`**](./__init__.py#L92) method takes a list of expected results and a list of actual results, and compares them.  This method is used in the unit tests of a specific ingest, to validate the output data from the target **`@koza.transform_record`** decorated knowledge source data parser method.

The method parameters are:

- **result:** The **`koza.model.graphs.KnowledgeGraph | None`** result from a single call to the **`@koza.transform_record`** decorated method to be tested.
- **expected_nodes:** An optional list of expected nodes. The list values can be scalar (node identifiers expected) or dictionary of expected node property values.
- **expected_edge:** An optional expected edge (as a Python dictionary of field slot names and values). The expected slot values can be scalar or list of dictionaries that are edge sources to match.
- **node_test_slots:** List of (snakecase) node slot string names to be tested (default: only the node 'id' slot is tested).
- **association_test_slots:** String list of edge slots to be tested (default: None - edge slots are not tested).

This method doesn't return anything, but it raises an **`AssertError`** if any test expectation fails.
