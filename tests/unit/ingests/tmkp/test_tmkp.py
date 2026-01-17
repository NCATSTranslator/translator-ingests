"""Tests for TMKP ingest validation functions."""

import pytest

from translator_ingest.ingests.tmkp.tmkp import (
    _get_id_prefix,
    _get_valid_prefixes_for_class,
    _get_predicate_domain_range_prefixes,
    _validate_edge_prefixes,
)


class TestGetIdPrefix:
    """Tests for _get_id_prefix function."""

    @pytest.mark.parametrize(
        "curie,expected",
        [
            ("MONDO:0008315", "MONDO"),
            ("DRUGBANK:DB01248", "DRUGBANK"),
            ("CHEBI:15365", "CHEBI"),
            ("NCBIGene:1234", "NCBIGene"),
            ("HP:0001234", "HP"),
        ],
    )
    def test_extracts_prefix_from_valid_curie(self, curie: str, expected: str):
        assert _get_id_prefix(curie) == expected

    @pytest.mark.parametrize(
        "curie",
        [
            "invalid",
            "nocolon",
            "",
        ],
    )
    def test_returns_empty_string_for_invalid_curie(self, curie: str):
        assert _get_id_prefix(curie) == ""


class TestGetValidPrefixesForClass:
    """Tests for _get_valid_prefixes_for_class function."""

    def test_chemical_entity_includes_drugbank(self):
        prefixes = _get_valid_prefixes_for_class("ChemicalEntity")
        assert "DRUGBANK" in prefixes

    def test_chemical_entity_includes_chebi(self):
        prefixes = _get_valid_prefixes_for_class("ChemicalEntity")
        assert "CHEBI" in prefixes

    def test_disease_or_phenotypic_feature_includes_mondo(self):
        prefixes = _get_valid_prefixes_for_class("DiseaseOrPhenotypicFeature")
        assert "MONDO" in prefixes

    def test_disease_or_phenotypic_feature_includes_hp(self):
        prefixes = _get_valid_prefixes_for_class("DiseaseOrPhenotypicFeature")
        assert "HP" in prefixes

    def test_returns_frozenset(self):
        prefixes = _get_valid_prefixes_for_class("ChemicalEntity")
        assert isinstance(prefixes, frozenset)

    def test_unknown_class_returns_empty(self):
        prefixes = _get_valid_prefixes_for_class("NonExistentClass")
        assert prefixes == frozenset()


class TestGetPredicateDomainRangePrefixes:
    """Tests for _get_predicate_domain_range_prefixes function."""

    def test_treats_returns_chemical_domain_and_disease_range(self):
        result = _get_predicate_domain_range_prefixes("biolink:treats")
        assert result is not None
        domain_prefixes, range_prefixes = result
        assert "DRUGBANK" in domain_prefixes
        assert "MONDO" in range_prefixes

    def test_unknown_predicate_returns_none(self):
        result = _get_predicate_domain_range_prefixes("biolink:nonexistent_predicate")
        assert result is None

    def test_returns_tuple_of_frozensets(self):
        result = _get_predicate_domain_range_prefixes("biolink:treats")
        assert result is not None
        domain_prefixes, range_prefixes = result
        assert isinstance(domain_prefixes, frozenset)
        assert isinstance(range_prefixes, frozenset)


class TestValidateEdgePrefixes:
    """Tests for _validate_edge_prefixes function."""

    def test_valid_treats_edge_chemical_to_disease(self):
        # Chemical treating disease is valid
        assert _validate_edge_prefixes(
            "DRUGBANK:DB01248", "MONDO:0008315", "biolink:treats"
        )

    def test_invalid_treats_edge_disease_to_chemical(self):
        # Disease treating chemical is invalid (reversed)
        assert not _validate_edge_prefixes(
            "MONDO:0008315", "DRUGBANK:DB01248", "biolink:treats"
        )

    def test_valid_treats_edge_chebi_to_mondo(self):
        # CHEBI chemical treating MONDO disease is valid
        assert _validate_edge_prefixes(
            "CHEBI:15365", "MONDO:0005148", "biolink:treats"
        )

    def test_unknown_predicate_returns_true(self):
        # Unknown predicates should pass (no constraints to check)
        assert _validate_edge_prefixes(
            "FOO:123", "BAR:456", "biolink:nonexistent_predicate"
        )

    def test_unknown_prefix_in_subject_fails_validation(self):
        # Unknown prefix in subject position for constrained predicate
        assert not _validate_edge_prefixes(
            "UNKNOWN:123", "MONDO:0008315", "biolink:treats"
        )

    def test_unknown_prefix_in_object_fails_validation(self):
        # Unknown prefix in object position for constrained predicate
        assert not _validate_edge_prefixes(
            "DRUGBANK:DB01248", "UNKNOWN:456", "biolink:treats"
        )
