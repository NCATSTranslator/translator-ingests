"""Validation of infores identifiers against the Biolink Information Resource Registry."""

from collections import Counter
from dataclasses import dataclass
from enum import StrEnum
from functools import lru_cache

from information_resource_registry import __version__ as INFORES_REGISTRY_VERSION
from information_resource_registry import load_catalog_dict

from translator_ingest.util.logging_utils import get_logger

logger = get_logger(__name__)

INFORES_REGISTRY_PACKAGE = "information-resource-registry"


class InforesStatus(StrEnum):
    """Validity of an infores identifier according to the registry catalog."""

    VALID = "valid"
    DEPRECATED = "deprecated"
    INVALID = "invalid"


@dataclass(frozen=True)
class InforesCatalog:
    """The infores registry catalog, with the registry version it was read from.

    :param version: version of the information-resource-registry package the catalog was read from, e.g. '1.2.2'
    :param statuses: maps each infores id in the catalog to its catalog ``status`` value
    """

    version: str
    statuses: dict[str, str]

    def get_infores_status(self, infores_id: str) -> InforesStatus:
        """Classify an infores id. Any catalog status other than 'deprecated' counts as valid."""
        if infores_id not in self.statuses:
            return InforesStatus.INVALID
        if self.statuses[infores_id] == InforesStatus.DEPRECATED:
            return InforesStatus.DEPRECATED
        return InforesStatus.VALID

    def provenance(self) -> dict[str, str]:
        """Describe which catalog this is, for recording in validation reports.

        >>> InforesCatalog(version="1.0.0", statuses={}).provenance()
        {'package': 'information-resource-registry', 'version': '1.0.0'}
        """
        return {
            "package": INFORES_REGISTRY_PACKAGE,
            "version": self.version,
        }


@lru_cache(maxsize=1)
def get_infores_catalog() -> InforesCatalog:
    """Load the infores catalog bundled with the information-resource-registry package."""
    catalog = load_catalog_dict()
    statuses = {resource["id"]: resource["status"] for resource in catalog["information_resources"]}
    logger.info(
        f"Loaded {len(statuses)} infores ids from {INFORES_REGISTRY_PACKAGE} {INFORES_REGISTRY_VERSION}"
    )
    return InforesCatalog(version=INFORES_REGISTRY_VERSION, statuses=statuses)


def get_edge_infores_ids(edge: dict) -> set[str]:
    """All infores ids in an edge's ``sources``"""
    infores_ids = set()
    for source in edge.get("sources") or []:
        if "resource_id" in source:
            infores_ids.add(source["resource_id"])
        infores_ids.update(source.get("upstream_resource_ids") or [])
    return {infores_id if infores_id else "" for infores_id in infores_ids}


@dataclass(frozen=True)
class InforesValidationResult:
    """Outcome of checking edge infores ids against the catalog.

    :param invalid: infores ids absent from the catalog, mapped to the number of edges referencing them
    :param deprecated: deprecated infores ids, mapped to the number of edges referencing them
    :param distinct_count: number of distinct infores ids referenced by the edges
    """

    invalid: dict[str, int]
    deprecated: dict[str, int]
    distinct_count: int


def check_infores_ids(infores_edge_counts: Counter[str], catalog: InforesCatalog) -> InforesValidationResult:
    """Check every infores id referenced by edges against the catalog.

    :param infores_edge_counts: number of edges referencing each infores id
    :param catalog: the infores catalog to check against
    """
    invalid = {}
    deprecated = {}
    for infores_id, edge_count in sorted(infores_edge_counts.items()):
        status = catalog.get_infores_status(infores_id)
        if status == InforesStatus.INVALID:
            invalid[infores_id] = edge_count
        elif status == InforesStatus.DEPRECATED:
            deprecated[infores_id] = edge_count
    return InforesValidationResult(invalid=invalid, deprecated=deprecated, distinct_count=len(infores_edge_counts))