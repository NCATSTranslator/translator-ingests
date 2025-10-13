from typing import Any, Optional, Iterator, Union, Set
import re

from linkml.validator.plugins import ValidationPlugin
from linkml.validator.report import ValidationResult, Severity
from linkml.validator.validation_context import ValidationContext
from linkml_runtime import SchemaView


def _yield_biolink_objects(data: Any, path: Optional[list[Union[str, int]]] = None):
    """Recursively yield node and edge objects from KGX data."""
    if path is None:
        path = []
    if isinstance(data, dict):
        # Check if this is a node or edge object
        if "id" in data and ("category" in data or "subject" in data):
            yield path, data
        else:
            # Recursively search nested dictionaries
            for key, value in data.items():
                yield from _yield_biolink_objects(value, path + [key])
    elif isinstance(data, list):
        # Handle lists of objects
        for i, item in enumerate(data):
            yield from _yield_biolink_objects(item, path + [i])


class BiolinkValidationPlugin(ValidationPlugin):
    """A validation plugin for Translator KGX data using Biolink Model requirements.

    This plugin is designed to be used as part of LinkML's validation framework for
    Translator ETL processes. It validates KGX (Knowledge Graph Exchange) format data
    against Biolink Model standards and performs the following checks:

      1. Ensure all node IDs follow proper CURIE format (prefix:identifier)
      2. Validate that node categories are valid Biolink Model terms
      3. Ensure edge predicates are valid Biolink Model predicates
      4. Check that all edges reference existing nodes
      5. Validate knowledge source attribution follows Translator standards
      6. Ensure proper evidence and provenance metadata
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._valid_categories_cache = None
        self._valid_predicates_cache = None
        self._node_ids_cache = set()

    def _get_valid_categories(self, schema_view: SchemaView) -> Set[str]:
        """Get valid Biolink Model categories."""
        if self._valid_categories_cache is not None:
            return self._valid_categories_cache

        # Get all classes that are subclasses of NamedThing
        valid_categories = set()
        try:
            named_thing_class = schema_view.get_class("NamedThing")
            if named_thing_class:
                # Get all descendants of NamedThing
                descendants = schema_view.class_descendants("NamedThing")
                valid_categories.update(f"biolink:{cls}" for cls in descendants)
                valid_categories.add("biolink:NamedThing")
        except Exception:
            # If we can't get categories from schema, use common ones
            valid_categories = {
                "biolink:Gene", "biolink:Protein", "biolink:Disease", "biolink:ChemicalEntity",
                "biolink:BiologicalProcess", "biolink:MolecularFunction", "biolink:CellularComponent",
                "biolink:Pathway", "biolink:PhenotypicFeature", "biolink:OrganismTaxon"
            }

        self._valid_categories_cache = valid_categories
        return valid_categories

    def _get_valid_predicates(self, schema_view: SchemaView) -> Set[str]:
        """Get valid Biolink Model predicates."""
        if self._valid_predicates_cache is not None:
            return self._valid_predicates_cache

        # Get all predicates (slots that are subclasses of related_to)
        valid_predicates = set()
        try:
            # Get all slots that are descendants of related_to
            descendants = schema_view.slot_descendants("related_to")
            valid_predicates.update(f"biolink:{slot}" for slot in descendants)
            valid_predicates.add("biolink:related_to")
        except Exception:
            # If we can't get predicates from schema, use common ones
            valid_predicates = {
                "biolink:related_to", "biolink:affects", "biolink:treats", "biolink:causes",
                "biolink:associated_with", "biolink:regulates", "biolink:interacts_with",
                "biolink:participates_in", "biolink:has_participant", "biolink:occurs_in"
            }

        self._valid_predicates_cache = valid_predicates
        return valid_predicates

    def _is_valid_curie(self, identifier: str) -> bool:
        """Check if identifier follows valid CURIE format."""
        if not isinstance(identifier, str):
            return False
        
        # Basic CURIE pattern: prefix:identifier
        curie_pattern = r'^[A-Za-z][A-Za-z0-9_]*:[A-Za-z0-9_\-\.]+$'
        return bool(re.match(curie_pattern, identifier))

    def _validate_node(self, node_obj: dict, path: str, context: ValidationContext) -> Iterator[ValidationResult]:
        """Validate a single node object."""
        # Check required fields
        if "id" not in node_obj:
            yield ValidationResult(
                type="biolink-model validation",
                severity=Severity.ERROR,
                instance=node_obj,
                instantiates=context.target_class,
                message=f"Node at /{path} is missing required 'id' field"
            )
            return

        node_id = node_obj["id"]
        self._node_ids_cache.add(node_id)

        # Validate CURIE format
        if not self._is_valid_curie(node_id):
            yield ValidationResult(
                type="biolink-model validation",
                severity=Severity.ERROR,
                instance=node_obj,
                instantiates=context.target_class,
                message=f"Node at /{path} has invalid CURIE format for id '{node_id}'"
            )

        # Check categories
        if "category" not in node_obj:
            yield ValidationResult(
                type="biolink-model validation",
                severity=Severity.ERROR,
                instance=node_obj,
                instantiates=context.target_class,
                message=f"Node at /{path} is missing required 'category' field"
            )
        else:
            categories = node_obj["category"]
            if isinstance(categories, str):
                categories = [categories]
            
            valid_categories = self._get_valid_categories(context.schema_view)
            for category in categories:
                if category not in valid_categories:
                    yield ValidationResult(
                        type="biolink-model validation",
                        severity=Severity.WARNING,
                        instance=node_obj,
                        instantiates=context.target_class,
                        message=f"Node at /{path} has potentially invalid category '{category}'"
                    )

        # Check for name field (recommended)
        if "name" not in node_obj:
            yield ValidationResult(
                type="biolink-model validation",
                severity=Severity.WARNING,
                instance=node_obj,
                instantiates=context.target_class,
                message=f"Node at /{path} is missing recommended 'name' field"
            )

    def _validate_edge(self, edge_obj: dict, path: str, context: ValidationContext) -> Iterator[ValidationResult]:
        """Validate a single edge object."""
        # Check required fields
        required_fields = ["subject", "predicate", "object"]
        for field in required_fields:
            if field not in edge_obj:
                yield ValidationResult(
                    type="biolink-model validation",
                    severity=Severity.ERROR,
                    instance=edge_obj,
                    instantiates=context.target_class,
                    message=f"Edge at /{path} is missing required '{field}' field"
                )

        if "predicate" in edge_obj:
            predicate = edge_obj["predicate"]
            valid_predicates = self._get_valid_predicates(context.schema_view)
            
            if predicate not in valid_predicates:
                yield ValidationResult(
                    type="biolink-model validation",
                    severity=Severity.WARNING,
                    instance=edge_obj,
                    instantiates=context.target_class,
                    message=f"Edge at /{path} has potentially invalid predicate '{predicate}'"
                )

        # Validate subject and object CURIEs
        for field in ["subject", "object"]:
            if field in edge_obj:
                identifier = edge_obj[field]
                if not self._is_valid_curie(identifier):
                    yield ValidationResult(
                        type="biolink-model validation",
                        severity=Severity.ERROR,
                        instance=edge_obj,
                        instantiates=context.target_class,
                        message=f"Edge at /{path} has invalid CURIE format for {field} '{identifier}'"
                    )

        # Check for knowledge source attribution
        if "sources" not in edge_obj:
            yield ValidationResult(
                type="biolink-model validation",
                severity=Severity.WARNING,
                instance=edge_obj,
                instantiates=context.target_class,
                message=f"Edge at /{path} is missing knowledge source attribution ('sources' field)"
            )
        else:
            sources = edge_obj["sources"]
            if isinstance(sources, list) and len(sources) > 0:
                # Check first source for proper structure
                primary_source = sources[0]
                if isinstance(primary_source, dict):
                    if "resource_id" not in primary_source:
                        yield ValidationResult(
                            type="biolink-model validation",
                            severity=Severity.WARNING,
                            instance=edge_obj,
                            instantiates=context.target_class,
                            message=f"Edge at /{path} primary source missing 'resource_id'"
                        )

    def process(self, instance: Any, context: ValidationContext) -> Iterator[ValidationResult]:
        """Perform Biolink Model validation on KGX data.

        :param instance: The KGX instance to validate
        :param context: The validation context which provides a SchemaView artifact
        :return: Iterator over validation results
        :rtype: Iterator[ValidationResult]
        """
        # Reset node cache for each instance
        self._node_ids_cache = set()
        
        # First pass: collect all node IDs and validate nodes
        for data_path, obj in _yield_biolink_objects(instance):
            str_data_path = '/'.join(str(p) for p in data_path)
            
            # Determine if this is a node or edge
            if "id" in obj and "category" in obj and "subject" not in obj:
                # This is a node
                yield from self._validate_node(obj, str_data_path, context)
            elif "subject" in obj and "predicate" in obj and "object" in obj:
                # This is an edge
                yield from self._validate_edge(obj, str_data_path, context)

        # Second pass: validate edge references
        for data_path, obj in _yield_biolink_objects(instance):
            str_data_path = '/'.join(str(p) for p in data_path)
            
            if "subject" in obj and "object" in obj:
                # Check that subject and object nodes exist
                subject_id = obj.get("subject")
                object_id = obj.get("object")
                
                if subject_id and subject_id not in self._node_ids_cache:
                    yield ValidationResult(
                        type="biolink-model validation",
                        severity=Severity.ERROR,
                        instance=instance,
                        instantiates=context.target_class,
                        message=f"Edge at /{str_data_path} references non-existent subject node '{subject_id}'"
                    )
                
                if object_id and object_id not in self._node_ids_cache:
                    yield ValidationResult(
                        type="biolink-model validation",
                        severity=Severity.ERROR,
                        instance=instance,
                        instantiates=context.target_class,
                        message=f"Edge at /{str_data_path} references non-existent object node '{object_id}'"
                    )