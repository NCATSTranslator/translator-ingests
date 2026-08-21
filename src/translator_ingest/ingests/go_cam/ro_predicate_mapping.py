"""
GO-CAM causal predicate (RO) to Biolink edge shape.

A static map from each RO/BFO relation the source uses to the Biolink predicate and
qualifiers that represent it. The rules behind these values - why a regulatory predicate
becomes biolink:regulates rather than biolink:precedes, where the aspect and direction
come from, and why some predicates drop their edge - are recorded as structured data in
``go_cam_rig.yaml`` under ``target_info.additional_notes`` and
``target_info.edge_type_info``.

Values were derived from the Relation Ontology and biolink-model rather than from a human
reading RO labels; see the RIG for that derivation. Revisit the table after an RO or
biolink-model release.
"""

from typing import NamedTuple, Optional


class CausalPredicateMapping(NamedTuple):
    """One RO causal predicate resolved against RO and biolink-model."""

    #: Biolink predicate CURIE, or None when the edge should be dropped.
    predicate: Optional[str]
    #: "biolink:causes" when aspect and direction are both present, else None.
    qualified_predicate: Optional[str]
    #: GeneOrGeneProductOrChemicalEntityAspectEnum value ("activity"), or None.
    object_aspect: Optional[str]
    #: DirectionQualifierEnum value ("increased"/"decreased"), or None.
    direction: Optional[str]
    #: RO's own label for the predicate, carried for log messages and review.
    ro_label: str
    #: How this row was derived.
    provenance: str


#: RO/BFO causal predicate -> Biolink edge shape.
RO_PREDICATE_MAP: dict[str, CausalPredicateMapping] = {
    # directly positively regulates  (3,266 edges in the release measured)
    #   gene-product projection RO:0002450 (directly positively regulates activity of)
    "RO:0002629": CausalPredicateMapping(
        predicate="biolink:regulates",
        qualified_predicate="biolink:causes",
        object_aspect="activity",
        direction="increased",
        ro_label="directly positively regulates",
        provenance="gene-product projection RO:0002450 (directly positively regulates activity of)",
    ),
    # provides input for  (2,597 edges in the release measured)
    #   direct via RO:0002412 (immediately causally upstream of)
    "RO:0002413": CausalPredicateMapping(
        predicate="biolink:precedes",
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="provides input for",
        provenance="direct via RO:0002412 (immediately causally upstream of)",
    ),
    # causally upstream of  (1,425 edges in the release measured)
    #   direct via RO:0002411 (causally upstream of)
    "RO:0002411": CausalPredicateMapping(
        predicate="biolink:precedes",
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="causally upstream of",
        provenance="direct via RO:0002411 (causally upstream of)",
    ),
    # directly negatively regulates  (825 edges in the release measured)
    #   gene-product projection RO:0002449 (directly negatively regulates activity of)
    "RO:0002630": CausalPredicateMapping(
        predicate="biolink:regulates",
        qualified_predicate="biolink:causes",
        object_aspect="activity",
        direction="decreased",
        ro_label="directly negatively regulates",
        provenance="gene-product projection RO:0002449 (directly negatively regulates activity of)",
    ),
    # indirectly positively regulates  (204 edges in the release measured)
    #   gene-product projection RO:0002448 (directly regulates activity of)
    "RO:0002407": CausalPredicateMapping(
        predicate="biolink:regulates",
        qualified_predicate="biolink:causes",
        object_aspect="activity",
        direction="increased",
        ro_label="indirectly positively regulates",
        provenance="gene-product projection RO:0002448 (directly regulates activity of)",
    ),
    # causally upstream of, positive effect  (170 edges in the release measured)
    #   direct via RO:0002411 (causally upstream of)
    "RO:0002304": CausalPredicateMapping(
        predicate="biolink:precedes",
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="causally upstream of, positive effect",
        provenance="direct via RO:0002411 (causally upstream of)",
    ),
    # removes input for  (72 edges in the release measured)
    #   direct via RO:0002411 (causally upstream of)
    "RO:0012010": CausalPredicateMapping(
        predicate="biolink:precedes",
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="removes input for",
        provenance="direct via RO:0002411 (causally upstream of)",
    ),
    # indirectly negatively regulates  (62 edges in the release measured)
    #   gene-product projection RO:0002448 (directly regulates activity of)
    "RO:0002409": CausalPredicateMapping(
        predicate="biolink:regulates",
        qualified_predicate="biolink:causes",
        object_aspect="activity",
        direction="decreased",
        ro_label="indirectly negatively regulates",
        provenance="gene-product projection RO:0002448 (directly regulates activity of)",
    ),
    # part of  (37 edges in the release measured)
    #   direct via BFO:0000050 (part of)
    "BFO:0000050": CausalPredicateMapping(
        predicate="biolink:part_of",
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="part of",
        provenance="direct via BFO:0000050 (part of)",
    ),
    # causally upstream of, negative effect  (35 edges in the release measured)
    #   direct via RO:0002411 (causally upstream of)
    "RO:0002305": CausalPredicateMapping(
        predicate="biolink:precedes",
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="causally upstream of, negative effect",
        provenance="direct via RO:0002411 (causally upstream of)",
    ),
    # has part  (27 edges in the release measured)
    #   direct via BFO:0000051 (has part)
    "BFO:0000051": CausalPredicateMapping(
        predicate="biolink:has_part",
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="has part",
        provenance="direct via BFO:0000051 (has part)",
    ),
    # immediately causally upstream of  (14 edges in the release measured)
    #   direct via RO:0002412 (immediately causally upstream of)
    "RO:0002412": CausalPredicateMapping(
        predicate="biolink:precedes",
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="immediately causally upstream of",
        provenance="direct via RO:0002412 (immediately causally upstream of)",
    ),
    # constitutively upstream of  (13 edges in the release measured)
    #   direct via RO:0002411 (causally upstream of)
    "RO:0012009": CausalPredicateMapping(
        predicate="biolink:precedes",
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="constitutively upstream of",
        provenance="direct via RO:0002411 (causally upstream of)",
    ),
    # directly regulates  (6 edges in the release measured)
    #   gene-product projection RO:0002448 (directly regulates activity of)
    "RO:0002578": CausalPredicateMapping(
        predicate="biolink:regulates",
        qualified_predicate=None,
        object_aspect="activity",
        direction=None,
        ro_label="directly regulates",
        provenance="gene-product projection RO:0002448 (directly regulates activity of)",
    ),
    # causally upstream of or within  (6 edges in the release measured)
    #   no sound biolink mapping
    "RO:0002418": CausalPredicateMapping(
        predicate=None,
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="causally upstream of or within",
        provenance="no sound biolink mapping",
    ),
    # transports or maintains localization of  (4 edges in the release measured)
    #   direct via RO:0000057 (has participant)
    "RO:0002313": CausalPredicateMapping(
        predicate="biolink:has_participant",
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="transports or maintains localization of",
        provenance="direct via RO:0000057 (has participant)",
    ),
    # obsolete directly inhibits  (4 edges in the release measured)
    #   obsolete in RO
    "RO:0002408": CausalPredicateMapping(
        predicate=None,
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="obsolete directly inhibits",
        provenance="obsolete in RO",
    ),
    # has input  (4 edges in the release measured)
    #   direct via RO:0002233 (has input)
    "RO:0002233": CausalPredicateMapping(
        predicate="biolink:has_input",
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="has input",
        provenance="direct via RO:0002233 (has input)",
    ),
    # positively regulates  (3 edges in the release measured)
    #   gene-product projection RO:0002448 (directly regulates activity of)
    "RO:0002213": CausalPredicateMapping(
        predicate="biolink:regulates",
        qualified_predicate="biolink:causes",
        object_aspect="activity",
        direction="increased",
        ro_label="positively regulates",
        provenance="gene-product projection RO:0002448 (directly regulates activity of)",
    ),
    # enabled by  (2 edges in the release measured)
    #   direct via RO:0002333 (enabled by)
    "RO:0002333": CausalPredicateMapping(
        predicate="biolink:enabled_by",
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="enabled by",
        provenance="direct via RO:0002333 (enabled by)",
    ),
    # regulates levels of  (2 edges in the release measured)
    #   no sound biolink mapping
    "RO:0002332": CausalPredicateMapping(
        predicate=None,
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="regulates levels of",
        provenance="no sound biolink mapping",
    ),
    # causally upstream of or within, positive effect  (2 edges in the release measured)
    #   no sound biolink mapping
    "RO:0004047": CausalPredicateMapping(
        predicate=None,
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="causally upstream of or within, positive effect",
        provenance="no sound biolink mapping",
    ),
    # causally upstream of or within, negative effect  (2 edges in the release measured)
    #   no sound biolink mapping
    "RO:0004046": CausalPredicateMapping(
        predicate=None,
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="causally upstream of or within, negative effect",
        provenance="no sound biolink mapping",
    ),
    # is evidence with support from  (1 edges in the release measured)
    #   no sound biolink mapping
    "RO:0002614": CausalPredicateMapping(
        predicate=None,
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="is evidence with support from",
        provenance="no sound biolink mapping",
    ),
    # occurs in  (1 edge in the release measured)
    #   direct via BFO:0000066 (occurs in)
    "BFO:0000066": CausalPredicateMapping(
        predicate="biolink:occurs_in",
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="occurs in",
        provenance="direct via BFO:0000066 (occurs in)",
    ),
    # capable of  (1 edges in the release measured)
    #   direct via RO:0002215 (capable of)
    "RO:0002215": CausalPredicateMapping(
        predicate="biolink:capable_of",
        qualified_predicate=None,
        object_aspect=None,
        direction=None,
        ro_label="capable of",
        provenance="direct via RO:0002215 (capable of)",
    ),
}


def normalize_ro_curie(causal_predicate: str) -> str:
    """
    Normalize the several spellings GO-CAM uses for a relation identifier to a CURIE.

    >>> normalize_ro_curie("RO:0002629")
    'RO:0002629'
    >>> normalize_ro_curie("obo:RO#RO_0002629")
    'RO:0002629'
    >>> normalize_ro_curie("http://purl.obolibrary.org/obo/RO_0002629")
    'RO:0002629'
    >>> normalize_ro_curie("http://purl.obolibrary.org/obo/BFO_0000050")
    'BFO:0000050'
    """
    token = causal_predicate.strip()
    if token.startswith("http://purl.obolibrary.org/obo/"):
        token = token.rsplit("/", 1)[-1]
    elif token.startswith("obo:") and "#" in token:
        token = token.split("#", 1)[1]
    if "_" in token and ":" not in token:
        prefix, _, local = token.partition("_")
        return f"{prefix}:{local}"
    return token


def map_causal_predicate(causal_predicate: Optional[str]) -> Optional[CausalPredicateMapping]:
    """
    Resolve a GO-CAM causal predicate to its Biolink mapping.

    Returns None when the predicate is absent or absent from the generated table, and a
    mapping whose ``predicate`` is None when RO/biolink-model offer no sound target. Both
    mean "drop this edge"; the caller distinguishes them for reporting.

    A regulatory predicate resolves through its gene-product projection and carries a
    direction:

    >>> mapping = map_causal_predicate("RO:0002629")
    >>> mapping.predicate, mapping.direction
    ('biolink:regulates', 'increased')
    >>> map_causal_predicate("RO:0002630").direction
    'decreased'

    URI spellings resolve the same way as CURIEs:

    >>> map_causal_predicate("http://purl.obolibrary.org/obo/RO_0002630").predicate
    'biolink:regulates'

    The three qualified-statement parts travel together. A predicate resolved without
    the gene-product projection has no object activity for a direction to attach to, so
    it carries none, even where RO annotates one:

    >>> plain = map_causal_predicate("RO:0012010")
    >>> plain.predicate, plain.ro_label
    ('biolink:precedes', 'removes input for')
    >>> plain.qualified_predicate, plain.object_aspect, plain.direction
    (None, None, None)

    An RO-obsolete predicate resolves to a mapping that drops the edge, with the reason
    preserved for the log:

    >>> obsolete = map_causal_predicate("RO:0002408")
    >>> obsolete.predicate is None, obsolete.provenance
    (True, 'obsolete in RO')

    A predicate absent from the table is unknown rather than known-unmappable:

    >>> map_causal_predicate("RO:9999999") is None
    True
    >>> map_causal_predicate(None) is None
    True
    """
    if not causal_predicate:
        return None
    return RO_PREDICATE_MAP.get(normalize_ro_curie(causal_predicate))
