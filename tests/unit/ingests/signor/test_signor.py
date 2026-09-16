from pathlib import Path
from typing import Any

import koza
import pytest
from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    Association,
    CausalMechanismQualifierEnum,
    ChemicalAffectsGeneAssociation,
    ChemicalEntityToChemicalEntityAssociation,
    ChemicalGeneInteractionAssociation,
    DirectionQualifierEnum,
    GeneAffectsChemicalAssociation,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    GeneRegulatesGeneAssociation,
    KnowledgeLevelEnum,
    PairwiseGeneToGeneInteraction,
    ResourceRoleEnum,
    RetrievalSource,
)
from koza.io.writer.jsonl_writer import JSONLWriter
from koza.model.writer import WriterConfig

from translator_ingest.ingests.signor import signor

SIGNOR_SOURCES = [
    RetrievalSource(
        id="infores:signor",
        resource_id="infores:signor",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

# -- Fixtures: one per distinct (category, predicate, agent_type, knowledge_level) tuple --
EDGE_FIXTURES = [
    {
        "association_class": ChemicalAffectsGeneAssociation,
        "params": {
            "id": "6d9d6e8c-b9b0-47a6-829f-988ac073bbeb",
            "subject": "CHEBI:47519",
            "predicate": "biolink:affects",
            "object": "NCBIGene:54658",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
        },
    },
    {
        "association_class": ChemicalEntityToChemicalEntityAssociation,
        "params": {
            "id": "5f837816-b0ce-4c06-89b0-0abeb47d1ddd",
            "subject": "CHEBI:17368",
            "predicate": "biolink:affects",
            "object": "CHEBI:28997",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
        },
    },
    {
        "association_class": ChemicalEntityToChemicalEntityAssociation,
        "params": {
            "id": "002ee7e5-d4e1-500a-9bb3-ea92e3e1ff7d",
            "subject": "CHEBI:17172",
            "predicate": "biolink:directly_physically_interacts_with",
            "object": "CHEBI:57673",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
        },
    },
    {
        "association_class": ChemicalGeneInteractionAssociation,
        "params": {
            "id": "764b64a1-da2c-4d1f-a8a4-8550514e0cb0",
            "subject": "CHEBI:9453",
            "predicate": "biolink:directly_physically_interacts_with",
            "object": "NCBIGene:3269",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
        },
    },
    {
        "association_class": GeneAffectsChemicalAssociation,
        "params": {
            "id": "3c0bb9cb-83a8-4453-9aa0-ede7bd123c19",
            "subject": "NCBIGene:123041",
            "predicate": "biolink:affects",
            "object": "CHEBI:29101",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
        },
    },
    {
        "association_class": GeneRegulatesGeneAssociation,
        "params": {
            "id": "d30c7d77-b166-492b-84d0-bdf94af9f523",
            "subject": "NCBIGene:6272",
            "predicate": "biolink:regulates",
            "object": "NCBIGene:348",
            "qualified_predicate": "biolink:causes",
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.abundance,
            "object_direction_qualifier": DirectionQualifierEnum.upregulated,
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
        },
    },
    {
        "association_class": PairwiseGeneToGeneInteraction,
        "params": {
            "id": "8dc21c7d-aab7-419f-a678-7ba1b28d0597",
            "subject": "NCBIGene:136",
            "predicate": "biolink:directly_physically_interacts_with",
            "object": "NCBIGene:2775",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": SIGNOR_SOURCES,
        },
    },
]


@pytest.mark.parametrize(
    "fixture",
    EDGE_FIXTURES,
    ids=lambda f: f"{f['association_class'].__name__}_{f['params']['predicate'].split(':')[-1]}",
)
def test_pydantic_roundtrip(fixture):
    """Instantiate the association and round-trip through Pydantic serialization."""
    cls = fixture["association_class"]
    obj = cls(**fixture["params"])
    dumped = obj.model_dump()
    restored = cls.model_validate(dumped)
    assert restored == obj


MECHANISM_CASES = [
    ("transcriptional regulation", CausalMechanismQualifierEnum.transcriptional_regulation),
    ("translation regulation", CausalMechanismQualifierEnum.translational_regulation),
    ("precursor of", None),
    ("binding", CausalMechanismQualifierEnum.binding),
    ("stabilization", CausalMechanismQualifierEnum.stabilization),
    ("destabilization", CausalMechanismQualifierEnum.destabilization),
    ("cleavage", CausalMechanismQualifierEnum.cleavage),
    ("isomerization", CausalMechanismQualifierEnum.isomerization),
    ("chemical inhibition", CausalMechanismQualifierEnum.inhibition),
    ("chemical activation", CausalMechanismQualifierEnum.activation),
    ("catalytic activity", CausalMechanismQualifierEnum.catalytic_activity),
    ("small molecule catalysis", CausalMechanismQualifierEnum.catalytic_activity),
    ("gtpase-activating protein", CausalMechanismQualifierEnum.gtpase_activation),
    ("guanine nucleotide exchange factor", CausalMechanismQualifierEnum.guanyl_nucleotide_exchange),
    ("relocalization", CausalMechanismQualifierEnum.relocalization),
    ("chemical modification", CausalMechanismQualifierEnum.chemical_modification),
    ("post transcriptional regulation", CausalMechanismQualifierEnum.post_transcriptional_regulation),
    ("post translational modification", CausalMechanismQualifierEnum.molecular_modification),
    ("phosphorylation", CausalMechanismQualifierEnum.phosphorylation),
    ("dephosphorylation", CausalMechanismQualifierEnum.dephosphorylation),
    ("neddylation", CausalMechanismQualifierEnum.neddylation),
    ("lipidation", CausalMechanismQualifierEnum.lipidation),
    ("tyrosination", CausalMechanismQualifierEnum.tyrosination),
    ("carboxylation", CausalMechanismQualifierEnum.carboxylation),
    ("ubiquitination", CausalMechanismQualifierEnum.ubiquitination),
    ("monoubiquitination", CausalMechanismQualifierEnum.monoubiquitination),
    ("polyubiquitination", CausalMechanismQualifierEnum.polyubiquitination),
    ("deubiquitination", CausalMechanismQualifierEnum.deubiquitination),
    ("acetylation", CausalMechanismQualifierEnum.acetylation),
    ("oxidation", CausalMechanismQualifierEnum.oxidation),
    ("deacetylation", CausalMechanismQualifierEnum.deacetylation),
    ("glycosylation", CausalMechanismQualifierEnum.glycosylation),
    ("deglycosylation", CausalMechanismQualifierEnum.deglycosylation),
    ("methylation", CausalMechanismQualifierEnum.methylation),
    ("demethylation", CausalMechanismQualifierEnum.demethylation),
    ("trimethylation", CausalMechanismQualifierEnum.trimethylation),
    ("sumoylation", CausalMechanismQualifierEnum.sumoylation),
    ("desumoylation", CausalMechanismQualifierEnum.desumoylation),
    ("ADP-ribosylation", CausalMechanismQualifierEnum.ADP_ribosylation),
    ("palmitoylation", CausalMechanismQualifierEnum.palmitoylation),
    ("hydroxylation", CausalMechanismQualifierEnum.hydroxylation),
    ("s-nitrosylation", CausalMechanismQualifierEnum.s_nitrosylation),
]


@pytest.mark.parametrize(
    ("mechanism", "expected"),
    MECHANISM_CASES,
    ids=[mechanism for mechanism, _ in MECHANISM_CASES],
)
def test_map_causal_mechanism(
    mechanism: str,
    expected: CausalMechanismQualifierEnum | None,
) -> None:
    """Map every currently supported SIGNOR mechanism without changing its qualifier."""
    assert signor._map_causal_mechanism(mechanism) == expected


@pytest.mark.parametrize("mechanism", [None, ""])
def test_map_causal_mechanism_returns_none_for_falsey_values(mechanism: str | None) -> None:
    """Preserve the current treatment of absent mechanism values."""
    assert signor._map_causal_mechanism(mechanism) is None


def test_map_causal_mechanism_rejects_unknown_value() -> None:
    """Preserve the current exception type and message for an unknown mechanism."""
    with pytest.raises(
        NotImplementedError,
        match="Effect unsupported mechanism could not be mapped to required qualifiers\\.",
    ):
        signor._map_causal_mechanism("unsupported mechanism")


EFFECT_CASES = [
    ("up-regulates", GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance, 0),
    ("up-regulates activity", GeneOrGeneProductOrChemicalEntityAspectEnum.activity, 0),
    ("up-regulates quantity", GeneOrGeneProductOrChemicalEntityAspectEnum.abundance, 0),
    ("up-regulates quantity by expression", GeneOrGeneProductOrChemicalEntityAspectEnum.expression, 0),
    ("up-regulates quantity by stabilization", GeneOrGeneProductOrChemicalEntityAspectEnum.stability, 0),
    ("down-regulates", GeneOrGeneProductOrChemicalEntityAspectEnum.activity_or_abundance, 1),
    ("down-regulates activity", GeneOrGeneProductOrChemicalEntityAspectEnum.activity, 1),
    ("down-regulates quantity", GeneOrGeneProductOrChemicalEntityAspectEnum.abundance, 1),
    ("down-regulates quantity by destabilization", GeneOrGeneProductOrChemicalEntityAspectEnum.stability, 1),
    ("down-regulates quantity by repression", GeneOrGeneProductOrChemicalEntityAspectEnum.expression, 1),
]

DIRECTION_CASES = [
    ("gene product", (DirectionQualifierEnum.upregulated, DirectionQualifierEnum.downregulated)),
    ("other", (DirectionQualifierEnum.increased, DirectionQualifierEnum.decreased)),
]


@pytest.mark.parametrize(
    ("direction_type", "directions"),
    DIRECTION_CASES,
    ids=[direction_type for direction_type, _ in DIRECTION_CASES],
)
@pytest.mark.parametrize(
    ("effect", "expected_aspect", "direction_index"),
    EFFECT_CASES,
    ids=[effect for effect, _, _ in EFFECT_CASES],
)
def test_map_effect_qualifiers(
    effect: str,
    expected_aspect: GeneOrGeneProductOrChemicalEntityAspectEnum,
    direction_index: int,
    direction_type: str,
    directions: tuple[DirectionQualifierEnum, DirectionQualifierEnum],
) -> None:
    """Map each regulatory effect using either category-dependent direction pair."""
    assert signor._map_effect_qualifiers(effect, directions) == (expected_aspect, directions[direction_index])


@pytest.mark.parametrize("effect", ["form complex", "unknown", None, ""])
def test_map_effect_qualifiers_returns_none_for_unqualified_effects(effect: str | None) -> None:
    """Preserve effects that intentionally receive no aspect or direction qualifiers."""
    directions = (DirectionQualifierEnum.increased, DirectionQualifierEnum.decreased)
    assert signor._map_effect_qualifiers(effect, directions) == (None, None)


def test_map_effect_qualifiers_rejects_unknown_value() -> None:
    """Preserve the current exception type and message for an unknown effect."""
    directions = (DirectionQualifierEnum.increased, DirectionQualifierEnum.decreased)
    with pytest.raises(
        NotImplementedError,
        match="Effect unsupported effect could not be mapped to required qualifiers\\.",
    ):
        signor._map_effect_qualifiers("unsupported effect", directions)


@pytest.mark.parametrize(
    ("taxon", "expected"),
    [
        ("9606", "NCBITaxon:9606"),
        ("-1", None),
        (None, None),
        ("", "NCBITaxon:"),
        (" 9606 ", "NCBITaxon: 9606 "),
    ],
)
def test_species_context(taxon: str | None, expected: str | None) -> None:
    """Suppress only unknown taxa, preserving other source strings exactly."""
    assert signor._species_context(taxon) == expected


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        ({"CELL_DATA": "CL:1;CL:2", "TISSUE_DATA": "UBERON:1"}, ["CL:1", "CL:2"]),
        ({"CELL_DATA": None, "TISSUE_DATA": "UBERON:1;UBERON:2"}, ["UBERON:1", "UBERON:2"]),
        ({"CELL_DATA": None, "TISSUE_DATA": None}, None),
        ({"CELL_DATA": "", "TISSUE_DATA": "UBERON:1"}, [""]),
        ({"CELL_DATA": None, "TISSUE_DATA": ""}, [""]),
        ({"CELL_DATA": " CL:1 ;;CL:2 ", "TISSUE_DATA": None}, [" CL:1 ", "", "CL:2 "]),
        ({"CELL_DATA": None, "TISSUE_DATA": " UBERON:1 ;"}, [" UBERON:1 ", ""]),
        ({"CELL_DATA": "CL:1"}, ["CL:1"]),
        ({"CELL_DATA": ""}, [""]),
    ],
)
def test_anatomical_context(record: dict[str, Any], expected: list[str] | None) -> None:
    """Prefer any non-None cell value without stripping or dropping empty entries."""
    assert signor._anatomical_context(record) == expected


@pytest.mark.parametrize(
    ("record", "missing_field"),
    [
        ({}, "CELL_DATA"),
        ({"TISSUE_DATA": "UBERON:1"}, "CELL_DATA"),
        ({"CELL_DATA": None}, "TISSUE_DATA"),
    ],
)
def test_anatomical_context_preserves_missing_field_errors(record: dict[str, Any], missing_field: str) -> None:
    """Require the cell field and read the tissue field only when cell data is None."""
    with pytest.raises(KeyError) as error:
        signor._anatomical_context(record)
    assert error.value.args == (missing_field,)


def _make_test_association(identifier: str) -> ChemicalEntityToChemicalEntityAssociation:
    """Build a real association for evidence-assignment tests."""
    return ChemicalEntityToChemicalEntityAssociation(
        id=identifier,
        subject="CHEBI:17368",
        predicate="biolink:affects",
        object="CHEBI:28997",
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
        sources=SIGNOR_SOURCES,
    )


@pytest.mark.parametrize("association_count", [1, 2])
def test_apply_evidence_to_every_association(association_count: int) -> None:
    """Assign all available evidence to both single- and dual-edge outputs."""
    associations = tuple(_make_test_association(f"association-{index}") for index in range(association_count))
    publications = ["PMID:123", "PMID:456"]
    supporting_text = ["First sentence.", "Second sentence."]

    signor._apply_evidence(associations, publications, supporting_text, 0.75)

    for association in associations:
        assert association.publications == publications
        assert association.supporting_text == supporting_text
        assert association.has_confidence_score == 0.75


@pytest.mark.parametrize("publications", [None, []])
@pytest.mark.parametrize("confidence_score", [None, 0.0, ""])
def test_apply_evidence_ignores_falsey_values(
    publications: list[str] | None, confidence_score: float | str | None
) -> None:
    """Retain the transform's current truthy-only evidence assignment behavior."""
    association = _make_test_association("association")

    signor._apply_evidence((association,), publications, [], confidence_score)

    assert association.publications is None
    assert association.supporting_text is None
    assert association.has_confidence_score is None


@pytest.fixture
def signor_context(tmp_path: Path) -> koza.KozaTransform:
    """Provide a real Koza context and writer for transform regression tests."""
    writer = JSONLWriter(str(tmp_path), "signor", WriterConfig())
    return koza.KozaTransform(extra_fields={}, writer=writer, mappings={})


@pytest.fixture
def signor_record() -> dict[str, Any]:
    """Provide a prepared SIGNOR record with evidence and both context columns."""
    return {
        "subject_name": "Subject",
        "object_name": "Object",
        "subject_category": "protein",
        "object_category": "protein",
        "IDA": "P12345",
        "IDB": "Q12345",
        "EFFECT": "down-regulates quantity",
        "MECHANISM": "binding",
        "DIRECT": "YES",
        "PMID": "123|456",
        "SENTENCE": " First sentence. | Second sentence. ",
        "SCORE": "0.75",
        "TAX_ID": "9606",
        "CELL_DATA": "CL:0000000;CL:0000001",
        "TISSUE_DATA": "UBERON:0000001",
    }


@pytest.mark.parametrize(
    ("taxon", "cell", "tissue", "expected_species", "expected_anatomy"),
    [
        ("9606", "CL:1;CL:2", "UBERON:1", "NCBITaxon:9606", ["CL:1", "CL:2"]),
        ("-1", None, "UBERON:1;UBERON:2", None, ["UBERON:1", "UBERON:2"]),
        (None, None, None, None, None),
        ("9606", "", "UBERON:1", "NCBITaxon:9606", [""]),
        ("", None, "", "NCBITaxon:", [""]),
    ],
)
def test_transform_preserves_context_values_and_placement(
    signor_record: dict[str, Any],
    taxon: str | None,
    cell: str | None,
    tissue: str | None,
    expected_species: str | None,
    expected_anatomy: list[str] | None,
) -> None:
    """Apply species to both chemical/protein edges and anatomy only to the primary edge."""
    signor_record.update(
        subject_category="chemical",
        IDA="CHEBI:17368",
        TAX_ID=taxon,
        CELL_DATA=cell,
        TISSUE_DATA=tissue,
    )
    graph = signor._transform_record(signor_record)
    assert graph is not None
    primary, interaction = graph.edges
    assert primary.species_context_qualifier == expected_species
    assert interaction.species_context_qualifier == expected_species
    assert primary.anatomical_context_qualifier == expected_anatomy
    assert interaction.model_dump().get("anatomical_context_qualifier") is None


@pytest.mark.parametrize("direct", ["YES", "NO", None, "", "yes"])
@pytest.mark.parametrize(
    ("subject_category", "object_category", "primary_class", "secondary_class"),
    [
        ("protein", "protein", GeneRegulatesGeneAssociation, PairwiseGeneToGeneInteraction),
        ("protein", "complex", Association, None),
        ("protein", "chemical", GeneAffectsChemicalAssociation, ChemicalGeneInteractionAssociation),
        ("chemical", "protein", ChemicalAffectsGeneAssociation, ChemicalGeneInteractionAssociation),
        ("smallmolecule", "protein", ChemicalAffectsGeneAssociation, ChemicalGeneInteractionAssociation),
        (
            "smallmolecule",
            "chemical",
            ChemicalEntityToChemicalEntityAssociation,
            ChemicalEntityToChemicalEntityAssociation,
        ),
        (
            "smallmolecule",
            "smallmolecule",
            ChemicalEntityToChemicalEntityAssociation,
            ChemicalEntityToChemicalEntityAssociation,
        ),
    ],
)
def test_transform_preserves_edge_routing_and_evidence(
    signor_context: koza.KozaTransform,
    signor_record: dict[str, Any],
    direct: str | None,
    subject_category: str,
    object_category: str,
    primary_class: type[Association],
    secondary_class: type[Association] | None,
) -> None:
    """Preserve every emitted category route, edge order, qualifier placement, and evidence."""
    is_part_of = object_category == "complex"
    record = {
        **signor_record,
        "subject_category": subject_category,
        "object_category": object_category,
        "IDA": "P12345" if subject_category == "protein" else "CHEBI:17368",
        "IDB": {"protein": "Q12345", "complex": "C1"}.get(object_category, "CHEBI:28997"),
        "EFFECT": "form complex" if is_part_of else "down-regulates quantity",
        "DIRECT": direct,
    }
    (graph,) = signor.transform_ingest_all(signor_context, [record])
    nodes = list(graph.nodes)
    edges = list(graph.edges)
    expected_classes = [primary_class]
    if direct == "YES" and secondary_class is not None:
        expected_classes.append(secondary_class)
    assert [type(edge) for edge in edges] == expected_classes
    assert [node.id for node in nodes] == [
        "UniProtKB:P12345" if subject_category == "protein" else "CHEBI:17368",
        {"protein": "UniProtKB:Q12345", "complex": "SIGNOR:C1"}.get(object_category, "CHEBI:28997"),
    ]
    assert [node.name for node in nodes] == ["Subject", "Object"]

    protein_pair = subject_category == object_category == "protein"
    predicate = "biolink:part_of" if is_part_of else "biolink:regulates" if protein_pair else "biolink:affects"
    assert edges[0].predicate == predicate
    for index, edge in enumerate(edges):
        assert (edge.subject, edge.object) == (nodes[0].id, nodes[1].id)
        assert edge.sources == SIGNOR_SOURCES
        assert edge.knowledge_level == KnowledgeLevelEnum.knowledge_assertion
        assert edge.agent_type == AgentTypeEnum.manual_agent
        assert edge.publications == ["PMID:123", "PMID:456"]
        assert edge.supporting_text == ["First sentence.", "Second sentence."]
        # Pydantic coerces source strings during assignment, as in the original transform.
        assert edge.has_confidence_score == 0.75
        if index == 1:
            assert edge.predicate == "biolink:directly_physically_interacts_with"

        fields = edge.model_dump()
        qualified = not is_part_of and primary_class is not ChemicalEntityToChemicalEntityAssociation
        assert fields.get("qualified_predicate") == ("biolink:causes" if qualified else None)
        assert fields.get("object_aspect_qualifier") == ("abundance" if qualified else None)
        direction = "downregulated" if protein_pair else "decreased"
        assert fields.get("object_direction_qualifier") == (direction if qualified else None)
        mechanism = "binding" if qualified and type(edge) is not GeneRegulatesGeneAssociation else None
        assert fields.get("causal_mechanism_qualifier") == mechanism
        species = None if is_part_of or (not qualified and index == 1) else "NCBITaxon:9606"
        assert fields.get("species_context_qualifier") == species
        anatomy = ["CL:0000000", "CL:0000001"] if qualified and not protein_pair and index == 0 else None
        assert fields.get("anatomical_context_qualifier") == anatomy


@pytest.mark.parametrize(
    ("subject_category", "object_category"),
    [
        ("protein", "protein"),
        ("protein", "chemical"),
        ("chemical", "protein"),
        ("smallmolecule", "chemical"),
    ],
)
def test_transform_missing_direct_raises_key_error(
    signor_context: koza.KozaTransform,
    signor_record: dict[str, Any],
    subject_category: str,
    object_category: str,
) -> None:
    """Keep DIRECT required in each branch that can emit a physical-interaction edge."""
    signor_record.update(
        subject_category=subject_category,
        object_category=object_category,
        IDA="P12345" if subject_category == "protein" else "CHEBI:17368",
        IDB="Q12345" if object_category == "protein" else "CHEBI:28997",
    )
    del signor_record["DIRECT"]
    with pytest.raises(KeyError) as error:
        signor.transform_ingest_all(signor_context, [signor_record])
    assert error.value.args == ("DIRECT",)


@pytest.mark.parametrize("field", ["MECHANISM", "EFFECT"])
def test_transform_missing_mapping_field_raises_key_error(
    signor_context: koza.KozaTransform, signor_record: dict[str, Any], field: str
) -> None:
    """Keep missing fields distinct from present fields with empty values."""
    del signor_record[field]
    with pytest.raises(KeyError) as error:
        signor.transform_ingest_all(signor_context, [signor_record])
    assert error.value.args == (field,)


@pytest.mark.parametrize("field", ["MECHANISM", "EFFECT"])
def test_transform_unknown_mapping_value_raises_before_category_filtering(
    signor_context: koza.KozaTransform, signor_record: dict[str, Any], field: str
) -> None:
    """Reject unknown mappings even on category pairs that otherwise emit no edges."""
    signor_record.update({field: "unsupported", "subject_category": "complex"})
    with pytest.raises(NotImplementedError) as error:
        signor.transform_ingest_all(signor_context, [signor_record])
    assert str(error.value) == "Effect unsupported could not be mapped to required qualifiers."


def test_transform_evidence_does_not_leak_between_records(
    signor_context: koza.KozaTransform, signor_record: dict[str, Any]
) -> None:
    """Leave absent evidence unassigned after processing a record with complete evidence."""
    empty_evidence = {**signor_record, "PMID": "", "SENTENCE": "", "SCORE": 0.0}
    (graph,) = signor.transform_ingest_all(signor_context, [signor_record, empty_evidence])
    edges = list(graph.edges)
    assert len(edges) == 4
    for edge in edges[:2]:
        assert edge.publications == ["PMID:123", "PMID:456"]
        assert edge.supporting_text == ["First sentence.", "Second sentence."]
        assert edge.has_confidence_score == 0.75
    for edge in edges[2:]:
        assert edge.publications is None
        assert edge.supporting_text is None
        assert edge.has_confidence_score is None


@pytest.mark.parametrize(
    ("subject_category", "object_category", "effect"),
    [
        ("complex", "protein", "down-regulates quantity"),
        ("protein", "smallmolecule", "down-regulates quantity"),
        ("chemical", "chemical", "down-regulates quantity"),
        ("protein", "protein", "form complex"),
        ("protein", "protein", "unknown"),
        ("protein", "protein", None),
        ("protein", "protein", ""),
    ],
)
def test_transform_record_returns_none_for_filtered_records(
    signor_record: dict[str, Any],
    subject_category: str,
    object_category: str,
    effect: str | None,
) -> None:
    """Return no per-record graph when a recognized mapping has no supported edge route."""
    signor_record.update(
        subject_category=subject_category,
        object_category=object_category,
        EFFECT=effect,
    )
    assert signor._transform_record(signor_record) is None


@pytest.mark.parametrize("include_filtered_records", [False, True], ids=["empty", "all-filtered"])
def test_transform_batch_returns_one_empty_graph(
    signor_context: koza.KozaTransform,
    signor_record: dict[str, Any],
    include_filtered_records: bool,
) -> None:
    """Preserve a single empty output graph for empty and entirely filtered input iterators."""
    records = (
        [{**signor_record, "subject_category": "complex"}, {**signor_record, "EFFECT": "unknown"}]
        if include_filtered_records
        else []
    )
    (graph,) = signor.transform_ingest_all(signor_context, iter(records))
    assert list(graph.nodes) == []
    assert list(graph.edges) == []


def test_transform_batch_preserves_record_order_and_duplicate_nodes(
    signor_context: koza.KozaTransform,
    signor_record: dict[str, Any],
) -> None:
    """Combine mixed edge routes into one graph without deduplicating nodes or reordering edges."""
    protein_record = {**signor_record, "DIRECT": "NO"}
    filtered_record = {**signor_record, "subject_category": "complex"}
    chemical_record = {
        **signor_record,
        "subject_category": "chemical",
        "IDA": "CHEBI:17368",
        "PMID": "789",
    }
    records = iter([protein_record, filtered_record, chemical_record, protein_record])
    (graph,) = signor.transform_ingest_all(signor_context, records)
    assert [node.id for node in graph.nodes] == [
        "UniProtKB:P12345",
        "UniProtKB:Q12345",
        "CHEBI:17368",
        "UniProtKB:Q12345",
        "UniProtKB:P12345",
        "UniProtKB:Q12345",
    ]
    assert [(edge.subject, edge.predicate, edge.object, edge.publications) for edge in graph.edges] == [
        ("UniProtKB:P12345", "biolink:regulates", "UniProtKB:Q12345", ["PMID:123", "PMID:456"]),
        ("CHEBI:17368", "biolink:affects", "UniProtKB:Q12345", ["PMID:789"]),
        ("CHEBI:17368", "biolink:directly_physically_interacts_with", "UniProtKB:Q12345", ["PMID:789"]),
        ("UniProtKB:P12345", "biolink:regulates", "UniProtKB:Q12345", ["PMID:123", "PMID:456"]),
    ]


@pytest.mark.parametrize(("effect", "expected_aspect", "direction_index"), EFFECT_CASES)
@pytest.mark.parametrize(
    ("subject_category", "object_category", "expected_class"),
    [
        ("protein", "protein", GeneRegulatesGeneAssociation),
        ("protein", "chemical", GeneAffectsChemicalAssociation),
        ("chemical", "protein", ChemicalAffectsGeneAssociation),
        ("smallmolecule", "protein", ChemicalAffectsGeneAssociation),
        ("smallmolecule", "chemical", ChemicalEntityToChemicalEntityAssociation),
        ("smallmolecule", "smallmolecule", ChemicalEntityToChemicalEntityAssociation),
        ("protein", "complex", None),
        ("complex", "protein", None),
        ("protein", "smallmolecule", None),
        ("chemical", "chemical", None),
        ("chemical", "smallmolecule", None),
        ("Protein", "protein", None),
    ],
)
def test_transform_routes_every_regulatory_effect(
    signor_record: dict[str, Any],
    effect: str,
    expected_aspect: GeneOrGeneProductOrChemicalEntityAspectEnum,
    direction_index: int,
    subject_category: str,
    object_category: str,
    expected_class: type[Association] | None,
) -> None:
    """Keep category routing exact and preserve each effect's qualifiers on both edges."""
    signor_record.update(
        subject_category=subject_category,
        object_category=object_category,
        IDA="P12345" if subject_category == "protein" else "CHEBI:17368",
        IDB="Q12345" if object_category == "protein" else "CHEBI:28997",
        EFFECT=effect,
    )
    graph = signor._transform_record(signor_record)
    if expected_class is None:
        assert graph is None
        return

    assert graph is not None
    primary, interaction = graph.edges
    assert type(primary) is expected_class
    protein_pair = subject_category == object_category == "protein"
    directions = ("upregulated", "downregulated") if protein_pair else ("increased", "decreased")
    assert primary.predicate == ("biolink:regulates" if protein_pair else "biolink:affects")
    assert interaction.predicate == "biolink:directly_physically_interacts_with"
    for edge in (primary, interaction):
        fields = edge.model_dump()
        qualified = expected_class is not ChemicalEntityToChemicalEntityAssociation
        assert fields.get("qualified_predicate") == ("biolink:causes" if qualified else None)
        assert fields.get("object_aspect_qualifier") == (expected_aspect.value if qualified else None)
        assert fields.get("object_direction_qualifier") == (directions[direction_index] if qualified else None)


@pytest.mark.parametrize(("mechanism", "expected"), MECHANISM_CASES)
@pytest.mark.parametrize(
    ("subject_category", "object_category"),
    [("protein", "protein"), ("protein", "chemical"), ("chemical", "protein"), ("smallmolecule", "chemical")],
)
def test_transform_preserves_mechanism_placement(
    signor_record: dict[str, Any],
    mechanism: str,
    expected: CausalMechanismQualifierEnum | None,
    subject_category: str,
    object_category: str,
) -> None:
    """Keep mechanisms off gene-regulation and chemical-pair primaries where currently absent."""
    signor_record.update(
        subject_category=subject_category,
        object_category=object_category,
        IDA="P12345" if subject_category == "protein" else "CHEBI:17368",
        IDB="Q12345" if object_category == "protein" else "CHEBI:28997",
        MECHANISM=mechanism,
    )
    graph = signor._transform_record(signor_record)
    assert graph is not None
    primary, interaction = graph.edges
    expected_value = expected.value if expected is not None else None
    if subject_category == "smallmolecule":
        assert primary.model_dump().get("causal_mechanism_qualifier") is None
        assert interaction.model_dump().get("causal_mechanism_qualifier") is None
    else:
        primary_mechanism = None if object_category == subject_category == "protein" else expected_value
        assert primary.model_dump().get("causal_mechanism_qualifier") == primary_mechanism
        assert interaction.model_dump().get("causal_mechanism_qualifier") == expected_value


def test_complex_membership_does_not_require_direct(signor_record: dict[str, Any]) -> None:
    """Complex membership emits one unqualified edge without consulting DIRECT."""
    signor_record.update(object_category="complex", IDB="C1", EFFECT="form complex")
    del signor_record["DIRECT"]

    graph = signor._transform_record(signor_record)

    assert graph is not None
    (association,) = graph.edges
    assert type(association) is Association
    assert (association.subject, association.predicate, association.object) == (
        "UniProtKB:P12345",
        "biolink:part_of",
        "SIGNOR:C1",
    )
    assert association.publications == ["PMID:123", "PMID:456"]


@pytest.mark.parametrize("subject_category", ["complex", "chemical", "unsupported"])
def test_filtered_record_does_not_require_node_fields(signor_record: dict[str, Any], subject_category: str) -> None:
    """Resolve unsupported routes before constructing nodes or reading DIRECT."""
    signor_record.update(subject_category=subject_category, object_category="chemical")
    for field in ("IDA", "IDB", "subject_name", "object_name", "DIRECT"):
        del signor_record[field]

    assert signor._transform_record(signor_record) is None


def test_unsupported_subject_does_not_require_object_category(signor_record: dict[str, Any]) -> None:
    """Preserve category checks that short-circuit before reading the object category."""
    signor_record["subject_category"] = "unsupported"
    del signor_record["object_category"]

    assert signor._transform_record(signor_record) is None


@pytest.mark.parametrize("direct", ["YES", "NO"])
def test_prepare_and_transform_preserve_aggregation(
    signor_context: koza.KozaTransform, signor_record: dict[str, Any], direct: str
) -> None:
    """Exercise real preparation, evidence aggregation, filtering, and graph construction together."""
    raw_record = {
        **signor_record,
        "ENTITYA": "miR-34",
        "TYPEA": "protein",
        "ENTITYB": "Object",
        "TYPEB": "protein",
        "PMID": "123",
        "SENTENCE": " First sentence. ",
        "DIRECT": direct,
    }
    for field in ("subject_name", "subject_category", "object_name", "object_category"):
        del raw_record[field]
    records = [
        raw_record,
        dict(raw_record),
        {**raw_record, "PMID": "456", "SENTENCE": " Second sentence. "},
        {**raw_record, "TYPEA": "stimulus"},
        {**raw_record, "TYPEB": "fusion protein"},
        {**raw_record, "ENTITYA": None},
    ]

    prepared = list(signor.prepare(signor_context, iter(records)))
    assert len(prepared) == 1
    assert prepared[0]["subject_name"] == "miR-34a"
    assert prepared[0]["PMID"] == "123|456"
    (graph,) = signor.transform_ingest_all(signor_context, iter(prepared))

    assert [(node.id, node.name) for node in graph.nodes] == [
        ("UniProtKB:P12345", "miR-34a"),
        ("UniProtKB:Q12345", "Object"),
    ]
    edges = list(graph.edges)
    assert len(edges) == (2 if direct == "YES" else 1)
    assert edges[0].predicate == "biolink:regulates"
    for edge in edges:
        assert edge.publications == ["PMID:123", "PMID:456"]
        assert edge.supporting_text == ["First sentence.", "Second sentence."]
        assert edge.has_confidence_score == 0.75
