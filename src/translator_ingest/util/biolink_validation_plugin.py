from typing import Any, Optional, Iterator, Union, Set
import re

from linkml.validator.plugins import ValidationPlugin
from linkml.validator.report import ValidationResult, Severity
from linkml.validator.validation_context import ValidationContext
from linkml_runtime.utils.schemaview import SchemaView
from bmt import Toolkit


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
      5. Validate domain and range constraints for edge predicates
      6. Validate knowledge source attribution follows Translator standards
      7. Ensure proper evidence and provenance metadata
    """

    def __init__(self, schema_view: Optional[SchemaView] = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._schema_view = schema_view
        self._valid_categories_cache = None
        self._valid_predicates_cache = None
        self._node_ids_cache = set()
        self._node_categories_cache = {}  # Maps node ID to its categories for domain/range validation
        self._bmt = Toolkit()  # BMT toolkit for domain/range validation

    def _get_valid_categories(self, schema_view: SchemaView) -> Set[str]:
        """Get valid Biolink Model categories."""
        if self._valid_categories_cache is not None:
            return self._valid_categories_cache

        # Get all classes that are subclasses of named thing
        valid_categories = set()
        try:
            named_thing_class = schema_view.get_class("named thing")
            if named_thing_class:
                # Get all descendants of named thing (space case)
                descendants = schema_view.class_descendants("named thing")
                valid_categories.update(f"biolink:{cls.replace(' ', '')}" for cls in descendants)
                valid_categories.add("biolink:NamedThing")
        except Exception as e:
            # Having a working schema with NamedThing descendants is required
            raise RuntimeError(f"Failed to get valid categories from Biolink schema: {e}")

        self._valid_categories_cache = valid_categories
        return valid_categories

    def _get_valid_predicates(self, schema_view: SchemaView) -> Set[str]:
        """Get valid Biolink Model predicates."""
        if self._valid_predicates_cache is not None:
            return self._valid_predicates_cache

        # Get all predicates (slots that are subclasses of related to)
        valid_predicates = set()
        try:
            # Get all slots that are descendants of related to (space case)
            descendants = schema_view.slot_descendants("related to")
            valid_predicates.update(f"biolink:{slot.replace(' ', '_')}" for slot in descendants)
            valid_predicates.add("biolink:related_to")
        except Exception as e:
            # Having a working schema with predicate descendants is required
            raise RuntimeError(f"Failed to get valid predicates from Biolink schema: {e}")

        self._valid_predicates_cache = valid_predicates
        return valid_predicates

    def _is_valid_curie(self, identifier: str) -> bool:
        """Check if the identifier follows a valid CURIE format."""
        if not isinstance(identifier, str):
            return False

        # Basic CURIE pattern: prefix:identifier
        # Prefix and identifier must start with alphanumeric
        curie_pattern = r"^[A-Za-z0-9][A-Za-z0-9_\-\.]*:[A-Za-z0-9][A-Za-z0-9_\-\.]*$"
        return bool(re.match(curie_pattern, identifier))
    
    def _category_matches_constraint(self, categories: list[str], constraint: str) -> bool:
        """Check if any category matches the domain/range constraint using BMT.
        
        Args:
            categories: List of categories (with biolink: prefix)
            constraint: Domain or range constraint (without biolink: prefix)
            
        Returns:
            True if any category matches the constraint
        """
        if not constraint or not categories:
            return True
            
        # Check each category against the constraint
        for category in categories:
            # Remove biolink: prefix for BMT lookup
            cat_name = category.replace('biolink:', '') if category.startswith('biolink:') else category
            
            # Get all ancestors including mixins
            ancestors = self._bmt.get_ancestors(cat_name, reflexive=True, mixin=True)
            
            # Check if the constraint is in the ancestors
            if constraint in ancestors:
                return True
                
        return False
    
    def _validate_domain_range(self, edge_obj: dict, path: str, predicate: str, 
                              schema_view: SchemaView) -> Iterator[ValidationResult]:
        """Validate domain and range constraints for an edge predicate."""
        # Remove biolink: prefix from predicate for lookup
        pred_name = predicate.replace('biolink:', '') if predicate.startswith('biolink:') else predicate
        
        # Get the slot definition
        slot = schema_view.get_slot(pred_name)
        if not slot:
            return
            
        # Check domain constraint
        if slot.domain:
            subject_id = edge_obj.get('subject')
            if subject_id and subject_id in self._node_categories_cache:
                subject_categories = self._node_categories_cache[subject_id]
                if not self._category_matches_constraint(subject_categories, slot.domain):
                    yield ValidationResult(
                        type="biolink-model validation",
                        severity=Severity.WARN,
                        instance=edge_obj,
                        instantiates=None,
                        message=f"Edge at /{path} violates domain constraint: predicate '{predicate}' "
                               f"expects domain '{slot.domain}' but subject has categories {subject_categories}",
                    )
                    
        # Check range constraint
        if slot.range:
            object_id = edge_obj.get('object')
            if object_id and object_id in self._node_categories_cache:
                object_categories = self._node_categories_cache[object_id]
                if not self._category_matches_constraint(object_categories, slot.range):
                    yield ValidationResult(
                        type="biolink-model validation",
                        severity=Severity.WARN,
                        instance=edge_obj,
                        instantiates=None,
                        message=f"Edge at /{path} violates range constraint: predicate '{predicate}' "
                               f"expects range '{slot.range}' but object has categories {object_categories}",
                    )

    def _validate_node(self, node_obj: dict, path: str, context: ValidationContext) -> Iterator[ValidationResult]:
        """Validate a single node object."""
        # Check required fields
        if "id" not in node_obj:
            yield ValidationResult(
                type="biolink-model validation",
                severity=Severity.ERROR,
                instance=node_obj,
                instantiates=context.target_class,
                message=f"Node at /{path} is missing required 'id' field",
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
                message=f"Node at /{path} has invalid CURIE format for id '{node_id}'",
            )

        # Check categories
        if "category" not in node_obj:
            yield ValidationResult(
                type="biolink-model validation",
                severity=Severity.ERROR,
                instance=node_obj,
                instantiates=context.target_class,
                message=f"Node at /{path} is missing required 'category' field",
            )
        else:
            categories = node_obj["category"]
            if isinstance(categories, str):
                categories = [categories]
            
            # Store categories for domain/range validation
            if node_id:
                self._node_categories_cache[node_id] = categories

            schema_view = self._schema_view or getattr(context, "schema_view", None)
            if schema_view:
                valid_categories = self._get_valid_categories(schema_view)
                for category in categories:
                    if category not in valid_categories:
                        yield ValidationResult(
                            type="biolink-model validation",
                            severity=Severity.WARN,
                            instance=node_obj,
                            instantiates=context.target_class,
                            message=f"Node at /{path} has potentially invalid category '{category}'",
                        )

        # Check for name field (recommended)
        if "name" not in node_obj:
            yield ValidationResult(
                type="biolink-model validation",
                severity=Severity.INFO,
                instance=node_obj,
                instantiates=context.target_class,
                message=f"Node at /{path} is missing recommended 'name' field",
            )

    def _validate_edge(self, edge_obj: dict, path: str, context: ValidationContext) -> Iterator[ValidationResult]:
        """Validate a single edge object."""
        # Get required fields from Association class in schema
        schema_view = self._schema_view or getattr(context, "schema_view", None)
        required_fields = ["subject", "predicate", "object"]  # fallback

        if schema_view:
            try:
                association_class = schema_view.get_class("Association")
                if association_class:
                    # Get slots that are required for Association
                    required_slots = []
                    for slot_name in schema_view.class_slots("Association"):
                        slot = schema_view.get_slot(slot_name)
                        if slot and slot.required:
                            required_slots.append(slot_name)
                    if required_slots:
                        required_fields = required_slots
            except Exception:
                # Fall back to hardcoded required fields if schema lookup fails
                pass

        for field in required_fields:
            if field not in edge_obj:
                yield ValidationResult(
                    type="biolink-model validation",
                    severity=Severity.ERROR,
                    instance=edge_obj,
                    instantiates=context.target_class,
                    message=f"Edge at /{path} is missing required '{field}' field",
                )

        if "predicate" in edge_obj:
            predicate = edge_obj["predicate"]
            schema_view = self._schema_view or getattr(context, "schema_view", None)
            if schema_view:
                valid_predicates = self._get_valid_predicates(schema_view)
                if predicate not in valid_predicates:
                    yield ValidationResult(
                        type="biolink-model validation",
                        severity=Severity.WARN,
                        instance=edge_obj,
                        instantiates=context.target_class,
                        message=f"Edge at /{path} has potentially invalid predicate '{predicate}'",
                    )
                else:
                    # Validate domain/range constraints for valid predicates
                    yield from self._validate_domain_range(edge_obj, path, predicate, schema_view)

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
                        message=f"Edge at /{path} has invalid CURIE format for {field} '{identifier}'",
                    )

        # Check for knowledge source attribution
        if "sources" not in edge_obj:
            yield ValidationResult(
                type="biolink-model validation",
                severity=Severity.INFO,
                instance=edge_obj,
                instantiates=context.target_class,
                message=f"Edge at /{path} is missing knowledge source attribution ('sources' field)",
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
                            severity=Severity.WARN,
                            instance=edge_obj,
                            instantiates=context.target_class,
                            message=f"Edge at /{path} primary source missing 'resource_id'",
                        )

    def process(self, instance: Any, context: ValidationContext) -> Iterator[ValidationResult]:
        """Perform Biolink Model validation on KGX data.

        :param instance: The KGX instance to validate
        :param context: The validation context which provides a SchemaView artifact
        :return: Iterator over validation results
        :rtype: Iterator[ValidationResult]
        """
        # Reset caches for each instance
        self._node_ids_cache = set()
        self._node_categories_cache = {}

        # First pass: collect all node IDs and validate nodes
        for data_path, obj in _yield_biolink_objects(instance):
            str_data_path = "/".join(str(p) for p in data_path)

            # Determine if this is a node or edge
            if "id" in obj and "category" in obj and "subject" not in obj:
                # This is a node
                yield from self._validate_node(obj, str_data_path, context)
            elif "subject" in obj and "predicate" in obj and "object" in obj:
                # This is an edge
                yield from self._validate_edge(obj, str_data_path, context)

        # Second pass: validate edge references
        for data_path, obj in _yield_biolink_objects(instance):
            str_data_path = "/".join(str(p) for p in data_path)

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
                        message=f"Edge at /{str_data_path} references non-existent subject node '{subject_id}'",
                    )

                if object_id and object_id not in self._node_ids_cache:
                    yield ValidationResult(
                        type="biolink-model validation",
                        severity=Severity.ERROR,
                        instance=instance,
                        instantiates=context.target_class,
                        message=f"Edge at /{str_data_path} references non-existent object node '{object_id}'",
                    )
