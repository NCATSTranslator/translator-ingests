"""Declarative GtoPdb Type/Action interaction semantics."""

from dataclasses import dataclass
from typing import Any, Literal

from biolink_model.datamodel.pydanticmodel_v2 import (
    CausalMechanismQualifierEnum as CMQ,
    GeneOrGeneProductOrChemicalEntityAspectEnum as Aspect,
)

Polarity = Literal["positive", "negative"]
Relation = Literal["affects", "related"]


@dataclass(frozen=True)
class PrimaryAssociationRule:
    """Relation and qualifiers for a primary edge before endogenous projection."""

    relation: Relation = "affects"
    polarity: Polarity | None = None
    mechanism: CMQ | None = None
    qualified: bool = True
    aspect: Aspect | None = Aspect.activity


@dataclass(frozen=True)
class PhysicalInteractionRule:
    """Qualifiers for a direct physical edge, independent of any primary edge."""

    mechanism: CMQ | None = None


@dataclass(frozen=True)
class InteractionRule:
    """Independent edge definitions for one source Type/Action mapping.

    Either edge can be absent. Use a whole-rule ``None`` to emit nothing.

    >>> BINDING_ONLY.primary is None
    True
    >>> BINDING_ONLY.physical.mechanism.value
    'binding'
    >>> BINDING_INHIBITION.primary.mechanism.value
    'inhibition'
    >>> BINDING_INHIBITION.physical.mechanism.value
    'binding'
    """

    primary: PrimaryAssociationRule | None
    physical: PhysicalInteractionRule | None

    def __post_init__(self) -> None:
        """Require an edge definition; ``None`` is the only representation of a skip."""
        if self.primary is None and self.physical is None:
            raise ValueError("Use None to skip an interaction instead of an empty InteractionRule")


# Primary qualifiers describe only the pharmacological association.
PRIMARY_ACTIVATION = PrimaryAssociationRule(polarity="positive", mechanism=CMQ.activation)
PRIMARY_POTENTIATION = PrimaryAssociationRule(polarity="positive", mechanism=CMQ.potentiation)
PRIMARY_POSITIVE_BINDING = PrimaryAssociationRule(polarity="positive", mechanism=CMQ.binding)
PRIMARY_AGONISM = PrimaryAssociationRule(polarity="positive", mechanism=CMQ.agonism)
PRIMARY_BIASED_AGONISM = PrimaryAssociationRule(polarity="positive", mechanism=CMQ.biased_agonism)
PRIMARY_INVERSE_AGONISM = PrimaryAssociationRule(polarity="negative", mechanism=CMQ.inverse_agonism)
PRIMARY_MIXED_AGONISM = PrimaryAssociationRule(polarity="positive", mechanism=CMQ.mixed_agonism)
PRIMARY_PARTIAL_AGONISM = PrimaryAssociationRule(polarity="positive", mechanism=CMQ.partial_agonism)
PRIMARY_ANTAGONISM = PrimaryAssociationRule(polarity="negative", mechanism=CMQ.antagonism)
PRIMARY_NON_COMPETITIVE_ANTAGONISM = PrimaryAssociationRule(
    polarity="negative", mechanism=CMQ.non_competitive_antagonism
)
PRIMARY_INHIBITION = PrimaryAssociationRule(polarity="negative", mechanism=CMQ.inhibition)
PRIMARY_COMPETITIVE_INHIBITION = PrimaryAssociationRule(polarity="negative", mechanism=CMQ.competitive_inhibition)
PRIMARY_FEEDBACK_INHIBITION = PrimaryAssociationRule(polarity="negative", mechanism=CMQ.feedback_inhibition)
PRIMARY_IRREVERSIBLE_INHIBITION = PrimaryAssociationRule(polarity="negative", mechanism=CMQ.irreversible_inhibition)
PRIMARY_ALLOSTERIC_MODULATION = PrimaryAssociationRule(mechanism=CMQ.allosteric_modulation, qualified=False)
PRIMARY_BIPHASIC_ALLOSTERIC_MODULATION = PrimaryAssociationRule(
    mechanism=CMQ.biphasic_allosteric_modulation, qualified=False
)
PRIMARY_MIXED_ALLOSTERIC_MODULATION = PrimaryAssociationRule(mechanism=CMQ.mixed_allosteric_modulation, qualified=False)
PRIMARY_NEGATIVE_ALLOSTERIC_MODULATION = PrimaryAssociationRule(
    polarity="negative", mechanism=CMQ.negative_allosteric_modulation
)
PRIMARY_POSITIVE_ALLOSTERIC_MODULATION = PrimaryAssociationRule(
    polarity="positive", mechanism=CMQ.positive_allosteric_modulation
)
PRIMARY_ANTIBODY_AGONISM = PrimaryAssociationRule(polarity="positive", mechanism=CMQ.antibody_agonism)
PRIMARY_ANTIBODY_INHIBITION = PrimaryAssociationRule(polarity="negative", mechanism=CMQ.antibody_inhibition)
PRIMARY_BINDING = PrimaryAssociationRule(mechanism=CMQ.binding, qualified=False)
PRIMARY_CHANNEL_BLOCKAGE = PrimaryAssociationRule(polarity="negative", mechanism=CMQ.molecular_channel_blockage)
PRIMARY_GATING_INHIBITION = PrimaryAssociationRule(polarity="negative", mechanism=CMQ.gating_inhibition)
PRIMARY_UNDIRECTED_CHANNEL_BLOCKAGE = PrimaryAssociationRule(mechanism=CMQ.molecular_channel_blockage, qualified=False)
PRIMARY_UNDIRECTED_GATING_INHIBITION = PrimaryAssociationRule(mechanism=CMQ.gating_inhibition, qualified=False)
PRIMARY_RELATED = PrimaryAssociationRule(relation="related", qualified=False, aspect=None)
PRIMARY_NEUTRAL = PrimaryAssociationRule(qualified=False)
PRIMARY_POSITIVE_EFFECT = PrimaryAssociationRule(polarity="positive")
PRIMARY_NEGATIVE_EFFECT = PrimaryAssociationRule(polarity="negative")

# Physical mechanisms come from the mapping's separate physical-edge column.
DIRECT_PHYSICAL = PhysicalInteractionRule()
BINDING_PHYSICAL = PhysicalInteractionRule(mechanism=CMQ.binding)
ALLOSTERIC_PHYSICAL = PhysicalInteractionRule(mechanism=CMQ.allosteric_modulation)

# Compose named immutable rules; neither edge implies that the other is present.
ACTIVATION = InteractionRule(primary=PRIMARY_ACTIVATION, physical=None)
PHYSICAL_ACTIVATION = InteractionRule(primary=PRIMARY_ACTIVATION, physical=DIRECT_PHYSICAL)
POTENTIATION = InteractionRule(primary=PRIMARY_POTENTIATION, physical=None)
POSITIVE_BINDING = InteractionRule(primary=PRIMARY_POSITIVE_BINDING, physical=BINDING_PHYSICAL)
AGONISM = InteractionRule(primary=PRIMARY_AGONISM, physical=DIRECT_PHYSICAL)
BIASED_AGONISM = InteractionRule(primary=PRIMARY_BIASED_AGONISM, physical=DIRECT_PHYSICAL)
INVERSE_AGONISM = InteractionRule(primary=PRIMARY_INVERSE_AGONISM, physical=DIRECT_PHYSICAL)
MIXED_AGONISM = InteractionRule(primary=PRIMARY_MIXED_AGONISM, physical=DIRECT_PHYSICAL)
PARTIAL_AGONISM = InteractionRule(primary=PRIMARY_PARTIAL_AGONISM, physical=DIRECT_PHYSICAL)
ANTAGONISM = InteractionRule(primary=PRIMARY_ANTAGONISM, physical=DIRECT_PHYSICAL)
NON_COMPETITIVE_ANTAGONISM = InteractionRule(primary=PRIMARY_NON_COMPETITIVE_ANTAGONISM, physical=DIRECT_PHYSICAL)
INHIBITION = InteractionRule(primary=PRIMARY_INHIBITION, physical=DIRECT_PHYSICAL)
COMPETITIVE_INHIBITION = InteractionRule(primary=PRIMARY_COMPETITIVE_INHIBITION, physical=DIRECT_PHYSICAL)
FEEDBACK_INHIBITION = InteractionRule(primary=PRIMARY_FEEDBACK_INHIBITION, physical=None)
IRREVERSIBLE_INHIBITION = InteractionRule(primary=PRIMARY_IRREVERSIBLE_INHIBITION, physical=DIRECT_PHYSICAL)
ALLOSTERIC_MODULATION = InteractionRule(primary=PRIMARY_ALLOSTERIC_MODULATION, physical=ALLOSTERIC_PHYSICAL)
BIPHASIC_ALLOSTERIC_MODULATION = InteractionRule(
    primary=PRIMARY_BIPHASIC_ALLOSTERIC_MODULATION, physical=ALLOSTERIC_PHYSICAL
)
MIXED_ALLOSTERIC_MODULATION = InteractionRule(primary=PRIMARY_MIXED_ALLOSTERIC_MODULATION, physical=ALLOSTERIC_PHYSICAL)
NEGATIVE_ALLOSTERIC_MODULATION = InteractionRule(
    primary=PRIMARY_NEGATIVE_ALLOSTERIC_MODULATION, physical=ALLOSTERIC_PHYSICAL
)
POSITIVE_ALLOSTERIC_MODULATION = InteractionRule(
    primary=PRIMARY_POSITIVE_ALLOSTERIC_MODULATION, physical=ALLOSTERIC_PHYSICAL
)
ANTIBODY_AGONISM = InteractionRule(primary=PRIMARY_ANTIBODY_AGONISM, physical=DIRECT_PHYSICAL)
ANTIBODY_INHIBITION = InteractionRule(primary=PRIMARY_ANTIBODY_INHIBITION, physical=DIRECT_PHYSICAL)
BINDING = InteractionRule(primary=PRIMARY_BINDING, physical=BINDING_PHYSICAL)
MOLECULAR_CHANNEL_BLOCKAGE = InteractionRule(primary=PRIMARY_CHANNEL_BLOCKAGE, physical=DIRECT_PHYSICAL)
GATING_INHIBITION = InteractionRule(primary=PRIMARY_GATING_INHIBITION, physical=DIRECT_PHYSICAL)
UNDIRECTED_CHANNEL_BLOCKAGE = InteractionRule(primary=PRIMARY_UNDIRECTED_CHANNEL_BLOCKAGE, physical=DIRECT_PHYSICAL)
UNDIRECTED_GATING_INHIBITION = InteractionRule(primary=PRIMARY_UNDIRECTED_GATING_INHIBITION, physical=DIRECT_PHYSICAL)
RELATED = InteractionRule(primary=PRIMARY_RELATED, physical=None)
NEUTRAL_PHYSICAL = InteractionRule(primary=PRIMARY_NEUTRAL, physical=DIRECT_PHYSICAL)
POSITIVE_EFFECT = InteractionRule(primary=PRIMARY_POSITIVE_EFFECT, physical=None)
NEGATIVE_EFFECT = InteractionRule(primary=PRIMARY_NEGATIVE_EFFECT, physical=None)

BINDING_AGONISM = InteractionRule(primary=PRIMARY_AGONISM, physical=BINDING_PHYSICAL)
BINDING_ANTAGONISM = InteractionRule(primary=PRIMARY_ANTAGONISM, physical=BINDING_PHYSICAL)
BINDING_INHIBITION = InteractionRule(primary=PRIMARY_INHIBITION, physical=BINDING_PHYSICAL)
ALLOSTERIC_AGONISM = InteractionRule(primary=PRIMARY_AGONISM, physical=ALLOSTERIC_PHYSICAL)
ALLOSTERIC_ANTAGONISM = InteractionRule(primary=PRIMARY_ANTAGONISM, physical=ALLOSTERIC_PHYSICAL)
ALLOSTERIC_BIASED_AGONISM = InteractionRule(primary=PRIMARY_BIASED_AGONISM, physical=ALLOSTERIC_PHYSICAL)
ALLOSTERIC_INHIBITION = InteractionRule(primary=PRIMARY_INHIBITION, physical=ALLOSTERIC_PHYSICAL)
ALLOSTERIC_INVERSE_AGONISM = InteractionRule(primary=PRIMARY_INVERSE_AGONISM, physical=ALLOSTERIC_PHYSICAL)
ALLOSTERIC_PARTIAL_AGONISM = InteractionRule(primary=PRIMARY_PARTIAL_AGONISM, physical=ALLOSTERIC_PHYSICAL)
ALLOSTERIC_POTENTIATION = InteractionRule(primary=PRIMARY_POTENTIATION, physical=ALLOSTERIC_PHYSICAL)
PHYSICAL_ONLY = InteractionRule(primary=None, physical=DIRECT_PHYSICAL)
BINDING_ONLY = InteractionRule(primary=None, physical=BINDING_PHYSICAL)
ALLOSTERIC_ONLY = InteractionRule(primary=None, physical=ALLOSTERIC_PHYSICAL)


# Direct source-of-truth mapping from GtoPdb Type and Action values.
# None explicitly skips an interaction; the source label "None" is still text.
# Deliberate mechanism mappings: Full agonist uses agonism for all three types;
# Voltage-dependent inhibition uses gating_inhibition. Both decisions apply
# before endogenous projection and preserve the independent physical-edge rule.
RULES: dict[str, dict[str, InteractionRule | None]] = {
    "Activator": {
        "Activation": ACTIVATION,
        "Agonist": AGONISM,
        "Binding": POSITIVE_BINDING,
        "Full agonist": AGONISM,
        "None": POSITIVE_EFFECT,
        "Partial agonist": PARTIAL_AGONISM,
        "Positive": POSITIVE_EFFECT,
        "Potentiation": POTENTIATION,
    },
    "Agonist": {
        "Activation": ACTIVATION,
        "Agonist": AGONISM,
        "Biased agonist": BIASED_AGONISM,
        "Binding": BINDING_AGONISM,
        "Full agonist": AGONISM,
        "Inverse agonist": INVERSE_AGONISM,
        "Irreversible agonist": AGONISM,
        "Mixed": MIXED_AGONISM,
        "None": AGONISM,
        "Partial agonist": PARTIAL_AGONISM,
        "Unknown": AGONISM,
    },
    "Allosteric modulator": {
        "Activation": PHYSICAL_ACTIVATION,
        "Agonist": ALLOSTERIC_AGONISM,
        "Antagonist": ALLOSTERIC_ANTAGONISM,
        "Biased agonist": ALLOSTERIC_BIASED_AGONISM,
        "Binding": ALLOSTERIC_MODULATION,
        "Biphasic": BIPHASIC_ALLOSTERIC_MODULATION,
        "Full agonist": ALLOSTERIC_AGONISM,
        "Inhibition": ALLOSTERIC_INHIBITION,
        "Inverse agonist": ALLOSTERIC_INVERSE_AGONISM,
        "Mixed": MIXED_ALLOSTERIC_MODULATION,
        "Negative": NEGATIVE_ALLOSTERIC_MODULATION,
        "Neutral": ALLOSTERIC_ONLY,
        "None": ALLOSTERIC_ONLY,
        "Partial agonist": ALLOSTERIC_PARTIAL_AGONISM,
        "Positive": POSITIVE_ALLOSTERIC_MODULATION,
        "Potentiation": ALLOSTERIC_POTENTIATION,
    },
    "Antagonist": {
        "Antagonist": ANTAGONISM,
        "Binding": BINDING_ANTAGONISM,
        "Inhibition": ANTAGONISM,
        "Inverse agonist": INVERSE_AGONISM,
        "Irreversible inhibition": IRREVERSIBLE_INHIBITION,
        "Mixed": ANTAGONISM,
        "Non-competitive": NON_COMPETITIVE_ANTAGONISM,
        "Partial agonist": PHYSICAL_ONLY,
    },
    "Antibody": {
        "Agonist": ANTIBODY_AGONISM,
        "Antagonist": ANTIBODY_INHIBITION,
        "Binding": BINDING,
        "Inhibition": ANTIBODY_INHIBITION,
        "None": NEUTRAL_PHYSICAL,
    },
    "Channel blocker": {
        "Antagonist": MOLECULAR_CHANNEL_BLOCKAGE,
        "Inhibition": MOLECULAR_CHANNEL_BLOCKAGE,
        "None": UNDIRECTED_CHANNEL_BLOCKAGE,
        "Pore blocker": UNDIRECTED_CHANNEL_BLOCKAGE,
    },
    "Fusion protein": {
        "Binding": BINDING_ONLY,
        "Inhibition": INHIBITION,
    },
    "Gating inhibitor": {
        "Antagonist": GATING_INHIBITION,
        "Inhibition": GATING_INHIBITION,
        "None": UNDIRECTED_GATING_INHIBITION,
        "Pore blocker": GATING_INHIBITION,
        "Slows inactivation": GATING_INHIBITION,
        "Voltage-dependent inhibition": GATING_INHIBITION,
    },
    "Inhibitor": {
        "Antagonist": ANTAGONISM,
        "Binding": BINDING_INHIBITION,
        "Competitive": COMPETITIVE_INHIBITION,
        "Feedback inhibition": FEEDBACK_INHIBITION,
        "Inhibition": INHIBITION,
        "Irreversible inhibition": IRREVERSIBLE_INHIBITION,
        "Non-competitive": NON_COMPETITIVE_ANTAGONISM,
        "None": INHIBITION,
        "Unknown": INHIBITION,
    },
    "None": {
        "Binding": BINDING_ONLY,
        "Competitive": PHYSICAL_ONLY,
        "Inhibition": INHIBITION,
        "None": RELATED,
        "Potentiation": POTENTIATION,
    },
    "Subunit-specific": {
        "Inhibition": INHIBITION,
        "Mixed": None,
        "Potentiation": POTENTIATION,
    },
}


TYPE_FALLBACKS: dict[str, InteractionRule] = {
    "Activator": POSITIVE_EFFECT,
    "Inhibitor": NEGATIVE_EFFECT,
}


def resolve_rule(type_value: Any, action_value: Any) -> InteractionRule | None:
    """Resolve exact source labels and nullable scalars without coercing them.

    An explicit None entry skips the interaction. Type-level fallbacks apply
    only when the action is absent from the table.

    >>> resolve_rule("Activator", "future action").primary.polarity
    'positive'
    >>> resolve_rule("Subunit-specific", "Mixed") is None
    True
    >>> resolve_rule("Allosteric modulator", None) is None
    True
    >>> resolve_rule("None", "None").primary.relation
    'related'
    >>> resolve_rule("Agonist", "Activation").primary.mechanism.value
    'activation'
    >>> resolve_rule("Agonist", "Activation").physical is None
    True
    >>> resolve_rule("Allosteric modulator", "Activation").physical is DIRECT_PHYSICAL
    True
    """
    return RULES.get(type_value, {}).get(action_value, TYPE_FALLBACKS.get(type_value))
