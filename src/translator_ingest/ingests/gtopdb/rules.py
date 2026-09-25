"""Declarative GtoPdb Type/Action interaction semantics."""

from dataclasses import dataclass, replace
from typing import Literal

from biolink_model.datamodel.pydanticmodel_v2 import (
    CausalMechanismQualifierEnum as CMQ,
)

Polarity = Literal["positive", "negative"]
Relation = Literal["affects", "related"]


@dataclass(frozen=True)
class InteractionRule:
    """One source Type/Action mapping before endogenous projection."""

    relation: Relation = "affects"
    polarity: Polarity | None = None
    mechanism: CMQ | None = None
    qualified: bool = True
    physical_interaction: bool = False
    skip: bool = False


# Canonical rule shapes. Per-action entries below reuse these values or apply
# only their semantic differences with dataclasses.replace().
ACTIVATION = InteractionRule(polarity="positive", mechanism=CMQ.activation)
AGONISM = InteractionRule(
    polarity="positive", mechanism=CMQ.agonism, physical_interaction=True
)
ANTAGONISM = InteractionRule(
    polarity="negative", mechanism=CMQ.antagonism, physical_interaction=True
)
INHIBITION = InteractionRule(
    polarity="negative", mechanism=CMQ.inhibition, physical_interaction=True
)
SKIP = InteractionRule(skip=True)
RELATED = InteractionRule(relation="related", qualified=False)
NEUTRAL_PHYSICAL = InteractionRule(qualified=False, physical_interaction=True)


# Direct source-of-truth mapping from GtoPdb Type and Action values.
RULES: dict[str, dict[str, InteractionRule]] = {
    "Activator": {
        "Activation": ACTIVATION,
        "Agonist": AGONISM,
        "Binding": replace(AGONISM, mechanism=CMQ.binding),
        "Full agonist": AGONISM,
        "None": ACTIVATION,
        "Partial agonist": replace(AGONISM, mechanism=CMQ.partial_agonism),
        "Positive": ACTIVATION,
        "Potentiation": replace(ACTIVATION, mechanism=CMQ.potentiation),
    },
    "Agonist": {
        "Activation": AGONISM,
        "Agonist": AGONISM,
        "Biased agonist": replace(AGONISM, mechanism=CMQ.biased_agonism),
        "Binding": AGONISM,
        "Full agonist": AGONISM,
        "Inverse agonist": replace(
            AGONISM, polarity="negative", mechanism=CMQ.inverse_agonism
        ),
        "Irreversible agonist": AGONISM,
        "Mixed": replace(AGONISM, mechanism=CMQ.mixed_agonism),
        "None": AGONISM,
        "Partial agonist": replace(AGONISM, mechanism=CMQ.partial_agonism),
        "Unknown": AGONISM,
    },
    "Allosteric modulator": {
        "Activation": replace(AGONISM, mechanism=CMQ.activation),
        "Agonist": AGONISM,
        "Antagonist": ANTAGONISM,
        "Biased agonist": replace(AGONISM, mechanism=CMQ.biased_agonism),
        "Binding": replace(
            NEUTRAL_PHYSICAL, mechanism=CMQ.allosteric_modulation
        ),
        "Biphasic": replace(
            NEUTRAL_PHYSICAL, mechanism=CMQ.biphasic_allosteric_modulation
        ),
        "Full agonist": AGONISM,
        "Inhibition": INHIBITION,
        "Inverse agonist": replace(
            ANTAGONISM, mechanism=CMQ.inverse_agonism
        ),
        "Mixed": replace(
            NEUTRAL_PHYSICAL, mechanism=CMQ.mixed_allosteric_modulation
        ),
        "Negative": replace(
            ANTAGONISM, mechanism=CMQ.negative_allosteric_modulation
        ),
        "Neutral": SKIP,
        "None": SKIP,
        "Partial agonist": replace(AGONISM, mechanism=CMQ.partial_agonism),
        "Positive": replace(
            AGONISM, mechanism=CMQ.positive_allosteric_modulation
        ),
        "Potentiation": replace(AGONISM, mechanism=CMQ.potentiation),
    },
    "Antagonist": {
        "Antagonist": ANTAGONISM,
        "Binding": ANTAGONISM,
        "Inhibition": ANTAGONISM,
        "Inverse agonist": replace(
            ANTAGONISM, mechanism=CMQ.inverse_agonism
        ),
        "Irreversible inhibition": replace(
            ANTAGONISM, mechanism=CMQ.irreversible_inhibition
        ),
        "Mixed": ANTAGONISM,
        "Non-competitive": replace(
            ANTAGONISM, mechanism=CMQ.non_competitive_antagonism
        ),
        "Partial agonist": SKIP,
    },
    "Antibody": {
        "Agonist": replace(AGONISM, mechanism=CMQ.antibody_agonism),
        "Antagonist": replace(ANTAGONISM, mechanism=CMQ.antibody_inhibition),
        "Binding": replace(NEUTRAL_PHYSICAL, mechanism=CMQ.binding),
        "Inhibition": replace(ANTAGONISM, mechanism=CMQ.antibody_inhibition),
        "None": NEUTRAL_PHYSICAL,
    },
    "Channel blocker": {
        "Antagonist": replace(ANTAGONISM, mechanism=CMQ.molecular_channel_blockage),
        "Inhibition": replace(INHIBITION, mechanism=CMQ.molecular_channel_blockage),
        "None": replace(
            NEUTRAL_PHYSICAL, mechanism=CMQ.molecular_channel_blockage
        ),
        "Pore blocker": replace(
            NEUTRAL_PHYSICAL, mechanism=CMQ.molecular_channel_blockage
        ),
    },
    "Fusion protein": {
        "Binding": SKIP,
        "Inhibition": replace(INHIBITION, mechanism=CMQ.molecular_channel_blockage),
    },
    "Gating inhibitor": {
        "Antagonist": replace(ANTAGONISM, mechanism=CMQ.gating_inhibition),
        "Inhibition": replace(INHIBITION, mechanism=CMQ.gating_inhibition),
        "None": replace(NEUTRAL_PHYSICAL, mechanism=CMQ.gating_inhibition),
        "Pore blocker": replace(ANTAGONISM, mechanism=CMQ.gating_inhibition),
        "Slows inactivation": replace(ANTAGONISM, mechanism=CMQ.gating_inhibition),
        "Voltage-dependent inhibition": replace(
            ANTAGONISM, mechanism=CMQ.gating_inhibition
        ),
    },
    "Inhibitor": {
        "Antagonist": ANTAGONISM,
        "Binding": ANTAGONISM,
        "Competitive": replace(INHIBITION, mechanism=CMQ.competitive_inhibition),
        "Feedback inhibition": replace(
            INHIBITION, mechanism=CMQ.feedback_inhibition, physical_interaction=False
        ),
        "Inhibition": INHIBITION,
        "Irreversible inhibition": replace(
            INHIBITION, mechanism=CMQ.irreversible_inhibition
        ),
        "Non-competitive": replace(
            INHIBITION, mechanism=CMQ.non_competitive_antagonism
        ),
        "None": INHIBITION,
        "Unknown": INHIBITION,
    },
    "None": {
        "Binding": SKIP,
        "Competitive": SKIP,
        "Inhibition": INHIBITION,
        "None": RELATED,
        "Potentiation": replace(ACTIVATION, mechanism=CMQ.potentiation),
    },
    "Subunit-specific": {
        "Inhibition": INHIBITION,
        "Mixed": SKIP,
        "Potentiation": replace(ACTIVATION, mechanism=CMQ.potentiation),
    },
}


TYPE_FALLBACKS: dict[str, InteractionRule] = {
    "Activator": InteractionRule(polarity="positive"),
    "Inhibitor": InteractionRule(polarity="negative"),
}


def resolve_rule(type_value: str, action_value: str) -> InteractionRule | None:
    """Return the exact source rule or the established type-level fallback."""
    return RULES.get(type_value, {}).get(action_value, TYPE_FALLBACKS.get(type_value))
