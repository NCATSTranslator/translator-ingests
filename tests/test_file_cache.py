from pathlib import Path
from typing import Any

import pytest

from translator_ingest.util.ontology.cache.file_cache import FileCache

SAMPLE_TERM: dict[str, Any] = {
    "curie": "UBERON:0001987",
    "label": "placenta",
    "taxa": [],
    "types": ["biolink:GrossAnatomicalStructure"],
    "score": 1720.6982,
}


@pytest.fixture
def cache(tmp_path: Path) -> FileCache:
    """A FileCache backed by a scratch directory, isolated from the committed cache."""
    return FileCache(cache_dir=tmp_path)


def test_miss_before_save(cache: FileCache):
    """An unsaved query is neither contained nor retrievable."""
    assert not cache.contains("placenta", "UBERON")
    assert cache.retrieve("placenta", "UBERON") is None


def test_save_and_retrieve_round_trip(cache: FileCache):
    cache.save("placenta", "UBERON", SAMPLE_TERM)
    assert cache.contains("placenta", "UBERON")
    assert cache.retrieve("placenta", "UBERON") == SAMPLE_TERM


def test_save_is_an_upsert(cache: FileCache):
    """Saving the same key twice overwrites rather than erroring or duplicating."""
    cache.save("placenta", "UBERON", SAMPLE_TERM)
    updated = {**SAMPLE_TERM, "score": 1.0}
    cache.save("placenta", "UBERON", updated)
    assert cache.retrieve("placenta", "UBERON") == updated


def test_unresolved_tombstone_is_distinguishable_from_a_miss(cache: FileCache):
    """A query cached as 'no match found' (None) must still register as contained,
    unlike a query that was simply never looked up."""
    assert not cache.contains("nonexistent concept", "UBERON")

    cache.save("nonexistent concept", "UBERON", None)

    assert cache.contains("nonexistent concept", "UBERON")
    assert cache.retrieve("nonexistent concept", "UBERON") is None


def test_ontologies_are_partitioned(cache: FileCache):
    """The same query string in two different ontologies must not collide."""
    go_term = {**SAMPLE_TERM, "curie": "GO:0005737"}
    uberon_term = {**SAMPLE_TERM, "curie": "UBERON:0001987"}

    cache.save("cytoplasm", "GO", go_term)
    cache.save("cytoplasm", "UBERON", uberon_term)

    assert cache.retrieve("cytoplasm", "GO") == go_term
    assert cache.retrieve("cytoplasm", "UBERON") == uberon_term


def test_only_taxa_is_part_of_the_key(cache: FileCache):
    """The same query string under different taxon restrictions must not collide."""
    human_term = {"curie": "NCBIGene:7486", "label": "WRN", "taxa": ["NCBITaxon:9606"]}
    mouse_term = {"curie": "NCBIGene:22427", "label": "Wrn", "taxa": ["NCBITaxon:10090"]}

    cache.save("WRN", "NCBIGene", human_term, only_taxa="NCBITaxon:9606")
    cache.save("WRN", "NCBIGene", mouse_term, only_taxa="NCBITaxon:10090")

    assert not cache.contains("WRN", "NCBIGene")
    assert cache.retrieve("WRN", "NCBIGene", only_taxa="NCBITaxon:9606") == human_term
    assert cache.retrieve("WRN", "NCBIGene", only_taxa="NCBITaxon:10090") == mouse_term


def test_persists_across_instances(tmp_path: Path):
    """Entries must be readable by a fresh FileCache instance pointed at the same
    directory, proving the store round-trips through disk rather than memory."""
    FileCache(cache_dir=tmp_path).save("placenta", "UBERON", SAMPLE_TERM)

    reopened = FileCache(cache_dir=tmp_path)
    assert reopened.contains("placenta", "UBERON")
    assert reopened.retrieve("placenta", "UBERON") == SAMPLE_TERM


def test_dict_like_dunder_protocol(cache: FileCache):
    """__setitem__/__getitem__/__contains__ mirror save/retrieve/contains."""
    cache["placenta", "UBERON"] = SAMPLE_TERM
    assert ("placenta", "UBERON") in cache
    assert cache["placenta", "UBERON"] == SAMPLE_TERM

    human_term = {"curie": "NCBIGene:7486", "label": "WRN", "taxa": ["NCBITaxon:9606"]}
    cache["WRN", "NCBIGene", "NCBITaxon:9606"] = human_term
    assert ("WRN", "NCBIGene", "NCBITaxon:9606") in cache
    assert cache["WRN", "NCBIGene", "NCBITaxon:9606"] == human_term
