"""
Centralized configuration for deterministic Association ID generation.

The configuration is applied once at module import time. Any code that
imports from this module (or imports a module that does) gets the
configured strategy automatically — whether invoked via pipeline.py,
Koza directly, or in tests.
"""
from biolink_model.datamodel.pydanticmodel_v2 import AssociationIdConfig, IdStrategy

_association_id_strategy_configured = False


def configure_association_ids(
    strategy: IdStrategy = IdStrategy.ALL_FIELDS,
    custom_fields: list[str] | None = None,
    force: bool = False,
) -> None:
    """Configure how Association IDs are generated across all ingests.

    Safe to call multiple times — only the first call takes effect
    unless force=True.

    Args:
        strategy: The ID strategy to use. Defaults to ALL_FIELDS.
        custom_fields: Field names for CUSTOM strategy only.
        force: If True, reconfigure even if already configured (for testing).
    """
    global _association_id_strategy_configured
    if _association_id_strategy_configured and not force:
        return
    AssociationIdConfig.strategy = strategy
    if custom_fields is not None:
        AssociationIdConfig.custom_fields = custom_fields
    _association_id_strategy_configured = True


# Apply default configuration on first import
configure_association_ids()
