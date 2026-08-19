"""Declarative GtoPdb Type/Action interaction semantics."""

from dataclasses import dataclass
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


# Direct source-of-truth mapping from GtoPdb (Type, Action) values.
RULES: dict[tuple[str, str], InteractionRule] = {
    # Activator
    ('Activator', 'Activation'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.activation, qualified=True, physical_interaction=False, skip=False),
    ('Activator', 'Agonist'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.agonism, qualified=True, physical_interaction=True, skip=False),
    ('Activator', 'Binding'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.binding, qualified=True, physical_interaction=True, skip=False),
    ('Activator', 'Full agonist'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.agonism, qualified=True, physical_interaction=True, skip=False),
    ('Activator', 'None'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.activation, qualified=True, physical_interaction=False, skip=False),
    ('Activator', 'Partial agonist'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.partial_agonism, qualified=True, physical_interaction=True, skip=False),
    ('Activator', 'Positive'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.activation, qualified=True, physical_interaction=False, skip=False),
    ('Activator', 'Potentiation'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.potentiation, qualified=True, physical_interaction=False, skip=False),

    # Agonist
    ('Agonist', 'Activation'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.agonism, qualified=True, physical_interaction=True, skip=False),
    ('Agonist', 'Agonist'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.agonism, qualified=True, physical_interaction=True, skip=False),
    ('Agonist', 'Biased agonist'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.biased_agonism, qualified=True, physical_interaction=True, skip=False),
    ('Agonist', 'Binding'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.agonism, qualified=True, physical_interaction=True, skip=False),
    ('Agonist', 'Full agonist'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.agonism, qualified=True, physical_interaction=True, skip=False),
    ('Agonist', 'Inverse agonist'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.inverse_agonism, qualified=True, physical_interaction=True, skip=False),
    ('Agonist', 'Irreversible agonist'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.agonism, qualified=True, physical_interaction=True, skip=False),
    ('Agonist', 'Mixed'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.mixed_agonism, qualified=True, physical_interaction=True, skip=False),
    ('Agonist', 'None'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.agonism, qualified=True, physical_interaction=True, skip=False),
    ('Agonist', 'Partial agonist'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.partial_agonism, qualified=True, physical_interaction=True, skip=False),
    ('Agonist', 'Unknown'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.agonism, qualified=True, physical_interaction=True, skip=False),

    # Allosteric modulator
    ('Allosteric modulator', 'Activation'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.activation, qualified=True, physical_interaction=True, skip=False),
    ('Allosteric modulator', 'Agonist'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.agonism, qualified=True, physical_interaction=True, skip=False),
    ('Allosteric modulator', 'Antagonist'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.antagonism, qualified=True, physical_interaction=True, skip=False),
    ('Allosteric modulator', 'Biased agonist'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.biased_agonism, qualified=True, physical_interaction=True, skip=False),
    ('Allosteric modulator', 'Binding'): InteractionRule(relation='affects', polarity=None, mechanism=CMQ.allosteric_modulation, qualified=False, physical_interaction=True, skip=False),
    ('Allosteric modulator', 'Biphasic'): InteractionRule(relation='affects', polarity=None, mechanism=CMQ.biphasic_allosteric_modulation, qualified=False, physical_interaction=True, skip=False),
    ('Allosteric modulator', 'Full agonist'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.agonism, qualified=True, physical_interaction=True, skip=False),
    ('Allosteric modulator', 'Inhibition'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.inhibition, qualified=True, physical_interaction=True, skip=False),
    ('Allosteric modulator', 'Inverse agonist'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.inverse_agonism, qualified=True, physical_interaction=True, skip=False),
    ('Allosteric modulator', 'Mixed'): InteractionRule(relation='affects', polarity=None, mechanism=CMQ.mixed_allosteric_modulation, qualified=False, physical_interaction=True, skip=False),
    ('Allosteric modulator', 'Negative'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.negative_allosteric_modulation, qualified=True, physical_interaction=True, skip=False),
    ('Allosteric modulator', 'Neutral'): InteractionRule(relation='affects', polarity=None, mechanism=None, qualified=True, physical_interaction=False, skip=True),
    ('Allosteric modulator', 'None'): InteractionRule(relation='affects', polarity=None, mechanism=None, qualified=True, physical_interaction=False, skip=True),
    ('Allosteric modulator', 'Partial agonist'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.partial_agonism, qualified=True, physical_interaction=True, skip=False),
    ('Allosteric modulator', 'Positive'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.positive_allosteric_modulation, qualified=True, physical_interaction=True, skip=False),
    ('Allosteric modulator', 'Potentiation'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.potentiation, qualified=True, physical_interaction=True, skip=False),

    # Antagonist
    ('Antagonist', 'Antagonist'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.antagonism, qualified=True, physical_interaction=True, skip=False),
    ('Antagonist', 'Binding'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.antagonism, qualified=True, physical_interaction=True, skip=False),
    ('Antagonist', 'Inhibition'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.antagonism, qualified=True, physical_interaction=True, skip=False),
    ('Antagonist', 'Inverse agonist'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.inverse_agonism, qualified=True, physical_interaction=True, skip=False),
    ('Antagonist', 'Irreversible inhibition'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.irreversible_inhibition, qualified=True, physical_interaction=True, skip=False),
    ('Antagonist', 'Mixed'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.antagonism, qualified=True, physical_interaction=True, skip=False),
    ('Antagonist', 'Non-competitive'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.non_competitive_antagonism, qualified=True, physical_interaction=True, skip=False),
    ('Antagonist', 'Partial agonist'): InteractionRule(relation='affects', polarity=None, mechanism=None, qualified=True, physical_interaction=False, skip=True),

    # Antibody
    ('Antibody', 'Agonist'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.antibody_agonism, qualified=True, physical_interaction=True, skip=False),
    ('Antibody', 'Antagonist'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.antibody_inhibition, qualified=True, physical_interaction=True, skip=False),
    ('Antibody', 'Binding'): InteractionRule(relation='affects', polarity=None, mechanism=CMQ.binding, qualified=False, physical_interaction=True, skip=False),
    ('Antibody', 'Inhibition'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.antibody_inhibition, qualified=True, physical_interaction=True, skip=False),
    ('Antibody', 'None'): InteractionRule(relation='affects', polarity=None, mechanism=None, qualified=False, physical_interaction=True, skip=False),

    # Channel blocker
    ('Channel blocker', 'Antagonist'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.molecular_channel_blockage, qualified=True, physical_interaction=True, skip=False),
    ('Channel blocker', 'Inhibition'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.molecular_channel_blockage, qualified=True, physical_interaction=True, skip=False),
    ('Channel blocker', 'None'): InteractionRule(relation='affects', polarity=None, mechanism=CMQ.molecular_channel_blockage, qualified=False, physical_interaction=True, skip=False),
    ('Channel blocker', 'Pore blocker'): InteractionRule(relation='affects', polarity=None, mechanism=CMQ.molecular_channel_blockage, qualified=False, physical_interaction=True, skip=False),

    # Fusion protein
    ('Fusion protein', 'Binding'): InteractionRule(relation='affects', polarity=None, mechanism=None, qualified=True, physical_interaction=False, skip=True),
    ('Fusion protein', 'Inhibition'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.molecular_channel_blockage, qualified=True, physical_interaction=True, skip=False),

    # Gating inhibitor
    ('Gating inhibitor', 'Antagonist'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.gating_inhibition, qualified=True, physical_interaction=True, skip=False),
    ('Gating inhibitor', 'Inhibition'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.gating_inhibition, qualified=True, physical_interaction=True, skip=False),
    ('Gating inhibitor', 'None'): InteractionRule(relation='affects', polarity=None, mechanism=CMQ.gating_inhibition, qualified=False, physical_interaction=True, skip=False),
    ('Gating inhibitor', 'Pore blocker'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.gating_inhibition, qualified=True, physical_interaction=True, skip=False),
    ('Gating inhibitor', 'Slows inactivation'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.gating_inhibition, qualified=True, physical_interaction=True, skip=False),
    ('Gating inhibitor', 'Voltage-dependent inhibition'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.gating_inhibition, qualified=True, physical_interaction=True, skip=False),

    # Inhibitor
    ('Inhibitor', 'Antagonist'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.antagonism, qualified=True, physical_interaction=True, skip=False),
    ('Inhibitor', 'Binding'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.antagonism, qualified=True, physical_interaction=True, skip=False),
    ('Inhibitor', 'Competitive'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.competitive_inhibition, qualified=True, physical_interaction=True, skip=False),
    ('Inhibitor', 'Feedback inhibition'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.feedback_inhibition, qualified=True, physical_interaction=False, skip=False),
    ('Inhibitor', 'Inhibition'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.inhibition, qualified=True, physical_interaction=True, skip=False),
    ('Inhibitor', 'Irreversible inhibition'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.irreversible_inhibition, qualified=True, physical_interaction=True, skip=False),
    ('Inhibitor', 'Non-competitive'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.non_competitive_antagonism, qualified=True, physical_interaction=True, skip=False),
    ('Inhibitor', 'None'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.inhibition, qualified=True, physical_interaction=True, skip=False),
    ('Inhibitor', 'Unknown'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.inhibition, qualified=True, physical_interaction=True, skip=False),

    # None
    ('None', 'Binding'): InteractionRule(relation='affects', polarity=None, mechanism=None, qualified=True, physical_interaction=False, skip=True),
    ('None', 'Competitive'): InteractionRule(relation='affects', polarity=None, mechanism=None, qualified=True, physical_interaction=False, skip=True),
    ('None', 'Inhibition'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.inhibition, qualified=True, physical_interaction=True, skip=False),
    ('None', 'None'): InteractionRule(relation='related', polarity=None, mechanism=None, qualified=False, physical_interaction=False, skip=False),
    ('None', 'Potentiation'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.potentiation, qualified=True, physical_interaction=False, skip=False),

    # Subunit-specific
    ('Subunit-specific', 'Inhibition'): InteractionRule(relation='affects', polarity='negative', mechanism=CMQ.inhibition, qualified=True, physical_interaction=True, skip=False),
    ('Subunit-specific', 'Mixed'): InteractionRule(relation='affects', polarity=None, mechanism=None, qualified=True, physical_interaction=False, skip=True),
    ('Subunit-specific', 'Potentiation'): InteractionRule(relation='affects', polarity='positive', mechanism=CMQ.potentiation, qualified=True, physical_interaction=False, skip=False),

}


TYPE_FALLBACKS: dict[str, InteractionRule] = {
    "Activator": InteractionRule(polarity="positive"),
    "Inhibitor": InteractionRule(polarity="negative"),
}


def resolve_rule(type_value: str, action_value: str) -> InteractionRule | None:
    """Return the exact source rule or the established type-level fallback."""
    return RULES.get((type_value, action_value), TYPE_FALLBACKS.get(type_value))
