from dataclasses import dataclass

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    ChemicalAffectsBiologicalEntityAssociation,
    DirectionQualifierEnum,
    GeneAffectsChemicalAssociation,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    GeneRegulatesGeneAssociation,
)


CHEMICAL_TYPES = frozenset({"Compound", "Bound", "ElementCollection"})
BIO_TYPES = frozenset({"Protein", "ProteinComplex", "NucleicAcid", "Reaction"})


@dataclass(frozen=True)
class InteractionEdgeMapping:
    """Resolved edge mapping for a PathBank interaction pair."""

    predicate: str
    association_class: type[Association]
    qualified_predicate: str | None = None
    object_aspect_qualifier: GeneOrGeneProductOrChemicalEntityAspectEnum | None = None
    object_direction_qualifier: DirectionQualifierEnum | None = None


def map_interaction_edge(
    interaction_type: str,
    left_type: str,
    right_type: str,
) -> InteractionEdgeMapping:
    """Map PathBank interaction type and element types to a Biolink edge model."""

    interaction_type_lower = interaction_type.lower() if interaction_type else ""
    is_causal_down = "inhibit" in interaction_type_lower or "repress" in interaction_type_lower
    is_causal_up = (
        "activat" in interaction_type_lower
        or "induc" in interaction_type_lower
        or "promot" in interaction_type_lower
    )
    is_physical = (
        "bind" in interaction_type_lower
        or "physical" in interaction_type_lower
        or "complex" in interaction_type_lower
        or "associat" in interaction_type_lower
    )

    is_left_chem = left_type in CHEMICAL_TYPES
    is_left_bio = left_type in BIO_TYPES
    is_right_chem = right_type in CHEMICAL_TYPES
    is_right_bio = right_type in BIO_TYPES

    if is_physical:
        return InteractionEdgeMapping(
            predicate="biolink:physically_interacts_with",
            association_class=Association,
        )

    if is_causal_down or is_causal_up:
        predicate = "biolink:regulates" if is_left_bio else "biolink:affects"
        object_direction_qualifier = (
            DirectionQualifierEnum.downregulated if is_causal_down else DirectionQualifierEnum.upregulated
        )
        association_class: type[Association] = Association
        if is_left_chem and (is_right_bio or is_right_chem):
            association_class = ChemicalAffectsBiologicalEntityAssociation
        elif is_left_bio and is_right_bio:
            association_class = GeneRegulatesGeneAssociation
        elif is_left_bio and is_right_chem:
            association_class = GeneAffectsChemicalAssociation

        return InteractionEdgeMapping(
            predicate=predicate,
            association_class=association_class,
            qualified_predicate="biolink:causes",
            object_aspect_qualifier=GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance,
            object_direction_qualifier=object_direction_qualifier,
        )

    return InteractionEdgeMapping(
        predicate="biolink:interacts_with",
        association_class=Association,
    )
