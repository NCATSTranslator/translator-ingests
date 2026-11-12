"""
SQLite-backed, dictionary-like file cache of ontology term lookups.

One SQLite database file is maintained per ontology namespace (e.g. GO.sqlite3,
MONDO.sqlite3, UBERON.sqlite3), physically located under this package's 'db'
subdirectory. This partitions the cache by ontology identity and keeps each
database self-contained and small. Within a given ontology's database, entries
are indexed by the query string (and, for the small subset of ontologies for
which it matters - chiefly NCBIGene - the 'only_taxa' taxon restriction).

These database files are checked into the repository: resolving a query
against the Translator Name Resolution Service (NRS) is comparatively
expensive and the mapping from a concept name to an ontology term is stable,
so the cache is shared across users and the production pipeline rather than
being rebuilt from scratch by everyone.
"""
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import json

# Ontology namespace strings (e.g. "GO", "MONDO", "NCBIGene") are used verbatim
# as database file stems, so we restrict them to characters that are safe on
# every common filesystem.
_SAFE_ONTOLOGY_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")


def _normalize_taxa(only_taxa: str | None) -> str:
    """Normalize the optional taxon restriction to a non-null string for use as
    part of the SQLite composite primary key.

    SQLite treats NULL as distinct from every other NULL in a unique/primary
    key index, so two rows with only_taxa=NULL would never be recognized as
    duplicates of one another. Normalizing None to '' avoids that pitfall.

    :param only_taxa: str | None, pipe-delimited taxon CURIEs, or None if unrestricted
    :return: str, '' if only_taxa is None, else only_taxa unchanged

    >>> _normalize_taxa(None)
    ''
    >>> _normalize_taxa("NCBITaxon:9606")
    'NCBITaxon:9606'
    """
    return only_taxa or ""


class FileCache:
    """
    Dictionary-like, SQLite-backed cache of ontology term lookups.

    Entries are partitioned into one SQLite database file per ontology
    namespace, physically stored under this package's 'db' subdirectory, and
    indexed within each database by (query, only_taxa).

    A cached entry may itself be None, representing an "unresolved" tombstone:
    the query was looked up against the Translator Name Resolution Service and
    no match was found. This is distinct from the query never having been
    looked up at all, and callers that need to tell the two apart should use
    contains() before retrieve() - the same idiom as `key in d` before `d[key]`.
    """

    def __init__(self, cache_dir: Path | None = None) -> None:
        """
        :param cache_dir: Path | None, directory under which per-ontology database
                                        files are stored; defaults to the 'db'
                                        subdirectory of this package
        """
        self._cache_dir = cache_dir if cache_dir is not None else Path(__file__).parent / "db"

    def _db_path(self, ontology: str) -> Path:
        """
        The path of the SQLite database file backing the given ontology, creating
        the cache directory (but not the database file itself) if necessary.

        :param ontology: str, the (non-empty string) ontology namespace (e.g. "GO", "MONDO", "NCBIGene")
        :return: Path, the (ontology-specific) database file path
        """
        assert ontology, "Non-empty ontology must be specified"
        assert _SAFE_ONTOLOGY_NAME.match(ontology), f"Unsafe ontology name for a cache filename: '{ontology}'"

        self._cache_dir.mkdir(parents=True, exist_ok=True)

        return self._cache_dir / f"{ontology}.sqlite3"

    def _connect(self, ontology: str) -> sqlite3.Connection:
        """
        Opens a connection to the given ontology's database file, creating the
        backing schema if this is the first time the database is used.

        :param ontology: str, the (non-empty string) ontology namespace (e.g. "GO", "MONDO", "NCBIGene")
        :return: sqlite3.Connection, an open connection with the schema already in place
        """
        conn = sqlite3.connect(self._db_path(ontology))
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ontology_cache (
                query     TEXT NOT NULL,
                only_taxa TEXT NOT NULL DEFAULT '',
                term_json TEXT,
                cached_at TEXT NOT NULL,
                PRIMARY KEY (query, only_taxa)
            )
            """
        )
        return conn

    def contains(self, query: str, ontology: str, only_taxa: str | None = None) -> bool:
        """
        Dictionary-like `key in d` check: was this (query, only_taxa) combination
        ever cached for this ontology, whether as a resolved match or as an
        unresolved tombstone?

        :param query: str, the query concept string to look up
        :param ontology: str, the (non-empty string) ontology namespace to query
        :param only_taxa: str | None, pipe-delimited taxon CURIEs the original lookup was restricted to, if any
        :return: bool, True if an entry exists in the cache for this key, False otherwise
        """
        with self._connect(ontology) as conn:
            row = conn.execute(
                "SELECT 1 FROM ontology_cache WHERE query = ? AND only_taxa = ?",
                (query, _normalize_taxa(only_taxa)),
            ).fetchone()
        return row is not None

    def retrieve(self, query: str, ontology: str, only_taxa: str | None = None) -> dict[str, Any] | None:
        """
        Dictionary-like `d.get(key)`: retrieves the cached ontology term, if any.

        Returns None both when the (query, only_taxa) key was never cached, and
        when it was cached as an unresolved tombstone (i.e. NRS previously
        returned no match). Callers that must distinguish those two cases
        should call contains() first.

        :param query: str, the query concept string to look up
        :param ontology: str, the (non-empty string) ontology namespace to query
        :param only_taxa: str | None, pipe-delimited taxon CURIEs the original lookup was restricted to, if any
        :return: dict[str, Any] | None, the cached ontology term match, if any
        """
        with self._connect(ontology) as conn:
            row = conn.execute(
                "SELECT term_json FROM ontology_cache WHERE query = ? AND only_taxa = ?",
                (query, _normalize_taxa(only_taxa)),
            ).fetchone()

        if row is None or row[0] is None:
            return None

        return json.loads(row[0])

    def save(
            self,
            query: str,
            ontology: str,
            ontology_term: dict[str, Any] | None,
            only_taxa: str | None = None,
    ) -> None:
        """
        Dictionary-like `d[key] = value`: upserts the given ontology term into the
        cache, indexed by (query, only_taxa) within the given ontology's database.

        :param query: str, the query concept string to look up
        :param ontology: str, the (non-empty string) ontology namespace to query
        :param ontology_term: dict[str, Any] | None, the best ranked ontology term match,
                                                       or None to record an unresolved tombstone
        :param only_taxa: str | None, pipe-delimited taxon CURIEs the original lookup was restricted to, if any
        :return: None
        """
        term_json = None if ontology_term is None else json.dumps(ontology_term)
        cached_at = datetime.now(timezone.utc).isoformat()

        with self._connect(ontology) as conn:
            conn.execute(
                """
                INSERT INTO ontology_cache (query, only_taxa, term_json, cached_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (query, only_taxa) DO UPDATE SET
                    term_json = excluded.term_json,
                    cached_at = excluded.cached_at
                """,
                (query, _normalize_taxa(only_taxa), term_json, cached_at),
            )

    def __contains__(self, key: tuple[str, str] | tuple[str, str, str]) -> bool:
        return self.contains(*key)

    def __getitem__(self, key: tuple[str, str] | tuple[str, str, str]) -> dict[str, Any] | None:
        return self.retrieve(*key)

    def __setitem__(
            self,
            key: tuple[str, str] | tuple[str, str, str],
            value: dict[str, Any] | None,
    ) -> None:
        query, ontology, *rest = key
        only_taxa = rest[0] if rest else None
        self.save(query, ontology, value, only_taxa)


file_cache = FileCache()
