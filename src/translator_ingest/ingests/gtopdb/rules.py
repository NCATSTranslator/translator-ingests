"""Declarative GtoPdb Type/Action interaction semantics.

Rules preserve the behavior of the 2026.2 source vocabulary. They intentionally
keep source string values distinct: ``"None"`` is a source value, not Python
``None``.
"""

from dataclasses import dataclass
from typing import Literal

from biolink_model.datamodel.pydanticmodel_v2 import CausalMechanismQualifierEnum


Polarity = Literal["positive", "negative"]
Relation = Literal["affects", "related"]


@dataclass(frozen=True)
class InteractionRule:
    """One source Type/Action mapping before endogenous projection."""

    relation: Relation = "affects"
    polarity: Polarity | None = None
    mechanism: CausalMechanismQualifierEnum | None = None
    qualified: bool = True
    physical_interaction: bool = False
    skip: bool = False


def _rule(
    *,
    polarity: Polarity | None = None,
    mechanism: CausalMechanismQualifierEnum | None = None,
    qualified: bool = True,
    physical_interaction: bool = False,
) -> InteractionRule:
    return InteractionRule(
        polarity=polarity,
        mechanism=mechanism,
        qualified=qualified,
        physical_interaction=physical_interaction,
    )


def _add(
    rules: dict[tuple[str, str], InteractionRule],
    type_value: str,
    actions: tuple[str, ...],
    rule: InteractionRule,
) -> None:
    for action in actions:
        key = (type_value, action)
        if key in rules:
            raise ValueError(f"Duplicate GtoPdb interaction rule: {key}")
        rules[key] = rule


RULES: dict[tuple[str, str], InteractionRule] = {}

# Activator
_add(RULES, "Activator", ("Agonist", "Full agonist"), _rule(
    polarity="positive", mechanism=CausalMechanismQualifierEnum.agonism, physical_interaction=True,
))
_add(RULES, "Activator", ("Binding",), _rule(
    polarity="positive", mechanism=CausalMechanismQualifierEnum.binding, physical_interaction=True,
))
_add(RULES, "Activator", ("Partial agonist",), _rule(
    polarity="positive", mechanism=CausalMechanismQualifierEnum.partial_agonism, physical_interaction=True,
))
_add(RULES, "Activator", ("Activation", "None", "Positive"), _rule(
    polarity="positive", mechanism=CausalMechanismQualifierEnum.activation,
))
_add(RULES, "Activator", ("Potentiation",), _rule(
    polarity="positive", mechanism=CausalMechanismQualifierEnum.potentiation,
))

# Agonist
_add(RULES, "Agonist", (
    "Activation", "Agonist", "Binding", "Full agonist", "Irreversible agonist", "None", "Unknown",
), _rule(polarity="positive", mechanism=CausalMechanismQualifierEnum.agonism, physical_interaction=True))
_add(RULES, "Agonist", ("Biased agonist",), _rule(
    polarity="positive", mechanism=CausalMechanismQualifierEnum.biased_agonism, physical_interaction=True,
))
_add(RULES, "Agonist", ("Inverse agonist",), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.inverse_agonism, physical_interaction=True,
))
_add(RULES, "Agonist", ("Mixed",), _rule(
    polarity="positive", mechanism=CausalMechanismQualifierEnum.mixed_agonism, physical_interaction=True,
))
_add(RULES, "Agonist", ("Partial agonist",), _rule(
    polarity="positive", mechanism=CausalMechanismQualifierEnum.partial_agonism, physical_interaction=True,
))

# Allosteric modulator
_add(RULES, "Allosteric modulator", ("Activation",), _rule(
    polarity="positive", mechanism=CausalMechanismQualifierEnum.activation, physical_interaction=True,
))
_add(RULES, "Allosteric modulator", ("Agonist", "Full agonist"), _rule(
    polarity="positive", mechanism=CausalMechanismQualifierEnum.agonism, physical_interaction=True,
))
_add(RULES, "Allosteric modulator", ("Antagonist",), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.antagonism, physical_interaction=True,
))
_add(RULES, "Allosteric modulator", ("Biased agonist",), _rule(
    polarity="positive", mechanism=CausalMechanismQualifierEnum.biased_agonism, physical_interaction=True,
))
_add(RULES, "Allosteric modulator", ("Binding",), _rule(
    mechanism=CausalMechanismQualifierEnum.allosteric_modulation,
    qualified=False,
    physical_interaction=True,
))
_add(RULES, "Allosteric modulator", ("Biphasic",), _rule(
    mechanism=CausalMechanismQualifierEnum.biphasic_allosteric_modulation,
    qualified=False,
    physical_interaction=True,
))
_add(RULES, "Allosteric modulator", ("Inhibition",), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.inhibition, physical_interaction=True,
))
_add(RULES, "Allosteric modulator", ("Inverse agonist",), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.inverse_agonism, physical_interaction=True,
))
_add(RULES, "Allosteric modulator", ("Mixed",), _rule(
    mechanism=CausalMechanismQualifierEnum.mixed_allosteric_modulation,
    qualified=False,
    physical_interaction=True,
))
_add(RULES, "Allosteric modulator", ("Negative",), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.negative_allosteric_modulation, physical_interaction=True,
))
_add(RULES, "Allosteric modulator", ("Partial agonist",), _rule(
    polarity="positive", mechanism=CausalMechanismQualifierEnum.partial_agonism, physical_interaction=True,
))
_add(RULES, "Allosteric modulator", ("Positive",), _rule(
    polarity="positive", mechanism=CausalMechanismQualifierEnum.positive_allosteric_modulation, physical_interaction=True,
))
_add(RULES, "Allosteric modulator", ("Potentiation",), _rule(
    polarity="positive", mechanism=CausalMechanismQualifierEnum.potentiation, physical_interaction=True,
))
_add(RULES, "Allosteric modulator", ("Neutral", "None"), InteractionRule(skip=True))

# Antagonist
_add(RULES, "Antagonist", ("Antagonist", "Binding", "Inhibition", "Mixed"), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.antagonism, physical_interaction=True,
))
_add(RULES, "Antagonist", ("Inverse agonist",), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.inverse_agonism, physical_interaction=True,
))
_add(RULES, "Antagonist", ("Irreversible inhibition",), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.irreversible_inhibition, physical_interaction=True,
))
_add(RULES, "Antagonist", ("Non-competitive",), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.non_competitive_antagonism, physical_interaction=True,
))
_add(RULES, "Antagonist", ("Partial agonist",), InteractionRule(skip=True))

# Antibody
_add(RULES, "Antibody", ("Agonist",), _rule(
    polarity="positive", mechanism=CausalMechanismQualifierEnum.antibody_agonism, physical_interaction=True,
))
_add(RULES, "Antibody", ("Antagonist", "Inhibition"), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.antibody_inhibition, physical_interaction=True,
))
_add(RULES, "Antibody", ("Binding",), _rule(
    mechanism=CausalMechanismQualifierEnum.binding, qualified=False, physical_interaction=True,
))
_add(RULES, "Antibody", ("None",), _rule(qualified=False, physical_interaction=True))

# Channel blocker
_add(RULES, "Channel blocker", ("Antagonist", "Inhibition"), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.molecular_channel_blockage, physical_interaction=True,
))
_add(RULES, "Channel blocker", ("None", "Pore blocker"), _rule(
    mechanism=CausalMechanismQualifierEnum.molecular_channel_blockage,
    qualified=False,
    physical_interaction=True,
))

# Fusion protein
_add(RULES, "Fusion protein", ("Binding",), InteractionRule(skip=True))
_add(RULES, "Fusion protein", ("Inhibition",), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.molecular_channel_blockage, physical_interaction=True,
))

# Gating inhibitor
_add(RULES, "Gating inhibitor", (
    "Antagonist", "Inhibition", "Pore blocker", "Slows inactivation", "Voltage-dependent inhibition",
), _rule(polarity="negative", mechanism=CausalMechanismQualifierEnum.gating_inhibition, physical_interaction=True))
_add(RULES, "Gating inhibitor", ("None",), _rule(
    mechanism=CausalMechanismQualifierEnum.gating_inhibition, qualified=False, physical_interaction=True,
))

# Inhibitor
_add(RULES, "Inhibitor", ("Antagonist", "Binding"), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.antagonism, physical_interaction=True,
))
_add(RULES, "Inhibitor", ("Competitive",), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.competitive_inhibition, physical_interaction=True,
))
_add(RULES, "Inhibitor", ("Inhibition", "None", "Unknown"), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.inhibition, physical_interaction=True,
))
_add(RULES, "Inhibitor", ("Irreversible inhibition",), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.irreversible_inhibition, physical_interaction=True,
))
_add(RULES, "Inhibitor", ("Non-competitive",), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.non_competitive_antagonism, physical_interaction=True,
))
_add(RULES, "Inhibitor", ("Feedback inhibition",), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.feedback_inhibition,
))

# Source Type value "None"
_add(RULES, "None", ("Binding", "Competitive"), InteractionRule(skip=True))
_add(RULES, "None", ("Inhibition",), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.inhibition, physical_interaction=True,
))
_add(RULES, "None", ("None",), InteractionRule(relation="related", qualified=False))
_add(RULES, "None", ("Potentiation",), _rule(
    polarity="positive", mechanism=CausalMechanismQualifierEnum.potentiation,
))

# Subunit-specific
_add(RULES, "Subunit-specific", ("Inhibition",), _rule(
    polarity="negative", mechanism=CausalMechanismQualifierEnum.inhibition, physical_interaction=True,
))
_add(RULES, "Subunit-specific", ("Mixed",), InteractionRule(skip=True))
_add(RULES, "Subunit-specific", ("Potentiation",), _rule(
    polarity="positive", mechanism=CausalMechanismQualifierEnum.potentiation,
))

# Preserve documented broad fallback behavior for unlisted values. The existing
# 2026.2 source vocabulary does not exercise the Subunit-specific fallback.
TYPE_FALLBACKS = {
    "Activator": _rule(polarity="positive"),
    "Inhibitor": _rule(polarity="negative"),
}


def resolve_rule(type_value: str, action_value: str) -> InteractionRule | None:
    """Return the exact source rule or the established type-level fallback."""
    return RULES.get((type_value, action_value), TYPE_FALLBACKS.get(type_value))
