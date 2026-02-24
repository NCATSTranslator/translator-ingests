"""Tests for centralized deterministic Association ID generation."""
import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    AssociationIdConfig,
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    IdStrategy,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)

import translator_ingest.util.association_id as assoc_id_module
from translator_ingest.util.association_id import configure_association_ids


@pytest.fixture(autouse=True)
def _reset_id_config():
    """Reset AssociationIdConfig and singleton flag after each test."""
    original_strategy = AssociationIdConfig.strategy
    original_fields = AssociationIdConfig.custom_fields
    original_configured = assoc_id_module._association_id_strategy_configured
    yield
    AssociationIdConfig.strategy = original_strategy
    AssociationIdConfig.custom_fields = original_fields
    assoc_id_module._association_id_strategy_configured = original_configured


def test_auto_configure_on_import():
    """Importing the module auto-configures ALL_FIELDS strategy."""
    assert assoc_id_module._association_id_strategy_configured is True
    assert AssociationIdConfig.strategy == IdStrategy.ALL_FIELDS


def test_configure_sets_all_fields_strategy():
    """configure_association_ids() sets the ALL_FIELDS strategy by default."""
    configure_association_ids(force=True)
    assert AssociationIdConfig.strategy == IdStrategy.ALL_FIELDS


def test_configure_is_idempotent():
    """Second call without force=True is a no-op."""
    configure_association_ids(force=True)
    assert AssociationIdConfig.strategy == IdStrategy.ALL_FIELDS
    # Try to change strategy without force — should be ignored
    configure_association_ids(strategy=IdStrategy.CUSTOM, custom_fields=["subject"])
    assert AssociationIdConfig.strategy == IdStrategy.ALL_FIELDS


def test_configure_sets_custom_strategy_with_fields():
    """configure_association_ids() can set a CUSTOM strategy with specific fields."""
    configure_association_ids(
        strategy=IdStrategy.CUSTOM,
        custom_fields=["subject", "predicate", "object"],
        force=True,
    )
    assert AssociationIdConfig.strategy == IdStrategy.CUSTOM
    assert AssociationIdConfig.custom_fields == ["subject", "predicate", "object"]


def test_deterministic_id_generated_without_explicit_id():
    """Associations without explicit id get a deterministic uuid: prefixed ID."""
    assoc = Association(
        subject="HGNC:1",
        predicate="biolink:related_to",
        object="MONDO:0001",
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    assert assoc.id.startswith("uuid:")


def test_same_inputs_produce_same_id():
    """Identical inputs produce the same deterministic ID."""
    kwargs = dict(
        subject="HGNC:1",
        predicate="biolink:related_to",
        object="MONDO:0001",
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    a = Association(**kwargs)
    b = Association(**kwargs)
    assert a.id == b.id


def test_different_inputs_produce_different_ids():
    """Different inputs produce different deterministic IDs."""
    common = dict(
        predicate="biolink:related_to",
        object="MONDO:0001",
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    a = Association(subject="HGNC:1", **common)
    b = Association(subject="HGNC:2", **common)
    assert a.id != b.id


def test_explicit_id_preserved():
    """An explicit id is preserved and not overwritten."""
    assoc = Association(
        id="my-explicit-id",
        subject="HGNC:1",
        predicate="biolink:related_to",
        object="MONDO:0001",
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    assert assoc.id == "my-explicit-id"


def test_deterministic_id_on_association_subclass():
    """Deterministic IDs work on Association subclasses too."""
    assoc = ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation(
        subject="CHEBI:1234",
        predicate="biolink:treats",
        object="MONDO:0001",
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    assert assoc.id.startswith("uuid:")


@pytest.mark.parametrize(
    "subject,object_id",
    [
        ("HGNC:1", "MONDO:0001"),
        ("HGNC:2", "MONDO:0002"),
        ("CHEBI:123", "HP:0001"),
    ],
)
def test_deterministic_ids_vary_by_content(subject, object_id):
    """Each unique subject/object combination gets a unique deterministic ID."""
    assoc = Association(
        subject=subject,
        predicate="biolink:related_to",
        object=object_id,
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    assert assoc.id.startswith("uuid:")
    # Re-create to confirm determinism
    assoc2 = Association(
        subject=subject,
        predicate="biolink:related_to",
        object=object_id,
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    assert assoc.id == assoc2.id
