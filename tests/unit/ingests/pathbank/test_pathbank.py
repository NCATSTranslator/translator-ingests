import pytest
from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    ChemicalAffectsBiologicalEntityAssociation,
    DirectionQualifierEnum,
    GeneAffectsChemicalAssociation,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    GeneRegulatesGeneAssociation,
)

from translator_ingest.ingests.pathbank.interaction_mapping import map_interaction_edge


@pytest.mark.parametrize(
    "interaction_type,left_type,right_type,expected_predicate,expected_class,expected_direction",
    [
        (
            "Activation",
            "Protein",
            "Protein",
            "biolink:regulates",
            GeneRegulatesGeneAssociation,
            DirectionQualifierEnum.upregulated,
        ),
        (
            "Inhibition",
            "Compound",
            "Protein",
            "biolink:affects",
            ChemicalAffectsBiologicalEntityAssociation,
            DirectionQualifierEnum.downregulated,
        ),
        (
            "Activation",
            "Protein",
            "Compound",
            "biolink:regulates",
            GeneAffectsChemicalAssociation,
            DirectionQualifierEnum.upregulated,
        ),
    ],
)
def test_interaction_causal_mapping(
    interaction_type: str,
    left_type: str,
    right_type: str,
    expected_predicate: str,
    expected_class: type[Association],
    expected_direction: DirectionQualifierEnum,
) -> None:
    interaction_mapping = map_interaction_edge(
        interaction_type=interaction_type,
        left_type=left_type,
        right_type=right_type,
    )

    assert interaction_mapping.association_class is expected_class
    assert interaction_mapping.predicate == expected_predicate
    assert interaction_mapping.qualified_predicate == "biolink:causes"
    assert (
        interaction_mapping.object_aspect_qualifier
        == GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance
    )
    assert interaction_mapping.object_direction_qualifier == expected_direction


@pytest.mark.parametrize(
    "interaction_type,expected_predicate",
    [
        ("Physical association", "biolink:physically_interacts_with"),
        ("Unknown interaction type", "biolink:interacts_with"),
    ],
)
def test_interaction_non_causal_mapping(
    interaction_type: str,
    expected_predicate: str,
) -> None:
    interaction_mapping = map_interaction_edge(
        interaction_type=interaction_type,
        left_type="Protein",
        right_type="Protein",
    )

    assert interaction_mapping.association_class is Association
    assert interaction_mapping.predicate == expected_predicate
    assert interaction_mapping.qualified_predicate is None
