import doctest
import logging
from pathlib import Path

import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    GeneToGeneAssociation,
    Gene,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)
from translator_ingest.ingests.go_cam.go_cam import (
    extract_curator_orcids,
    extract_evidence_codes,
    extract_references,
    normalize_id,
    normalize_reference,
    transform_go_cam_models,
    unambiguous_value,
)

from tests.unit.ingests import MockKozaWriter

logger = logging.getLogger(__name__)


@pytest.fixture
def gocam_output():
    writer = MockKozaWriter()

    # Mock the koza transform object
    from koza.transform import KozaTransform

    mock_koza = KozaTransform(writer=writer, extra_fields={}, mappings={})

    # Create test data - this will be passed as an iterable to the transform function
    data = [
        {
            "graph": {
                "model_info": {
                    "id": "gomodel:0000000300000001",
                    "taxon": "NCBITaxon:9606",
                    "title": "Test GO-CAM Model",
                    "state": "production",
                    "date": "2023-01-01",
                    "contributor": "http://orcid.org/0000-0000-0000-0000",
                }
            },
            "nodes": [
                {"id": "UniProtKB:P12345", "label": "Test Gene 1"},
                {"id": "UniProtKB:Q67890", "label": "Test Gene 2"},
            ],
            "edges": [
                {
                    "source": "UniProtKB:P12345",
                    "target": "UniProtKB:Q67890",
                    "source_gene": "UniProtKB:P12345",
                    "target_gene": "UniProtKB:Q67890",
                    "model_id": "gomodel:GO:0001234",
                    "causal_predicate": "RO:0002629",
                    "causal_predicate_has_reference": ["PMID:12345678"],
                    "source_gene_molecular_function": "GO:0005515",
                    "source_gene_biological_process": "GO:0003700",
                    "source_gene_occurs_in": "GO:0005634",
                    "source_gene_product": "UniProtKB:P12345",
                    "target_gene_molecular_function": "GO:0043565",
                    "target_gene_biological_process": "GO:0003700",
                    "target_gene_occurs_in": "GO:0005634",
                    "target_gene_product": "UniProtKB:Q67890",
                }
            ],
            "_file_path": "test_model.json",
        }
    ]

    # Call the transform function directly
    results = transform_go_cam_models(mock_koza, data)
    for result in results:
        writer.write([result])

    return writer.items


def test_gocam_entities(gocam_output):
    entities = gocam_output
    assert entities
    assert len(entities) == 1

    # Extract nodes and edges from KnowledgeGraph
    kg = entities[0]
    from koza.model.graphs import KnowledgeGraph

    assert isinstance(kg, KnowledgeGraph)

    all_entities = list()
    all_entities.extend(kg.nodes)
    all_entities.extend(kg.edges)

    genes = [e for e in all_entities if isinstance(e, Gene)]
    assert len(genes) == 2

    gene1 = [g for g in genes if g.id == "UniProtKB:P12345"][0]
    assert gene1.name == "Test Gene 1"
    assert gene1.category == ["biolink:Gene"]
    assert gene1.in_taxon == ["NCBITaxon:9606"]

    gene2 = [g for g in genes if g.id == "UniProtKB:Q67890"][0]
    assert gene2.name == "Test Gene 2"
    assert gene2.category == ["biolink:Gene"]
    assert gene2.in_taxon == ["NCBITaxon:9606"]

    associations = [e for e in all_entities if isinstance(e, GeneToGeneAssociation)]
    assert len(associations) == 1

    association = associations[0]
    assert association.subject == "UniProtKB:P12345"
    assert association.subject_activity_qualifier == "GO:0005515"
    assert association.subject_process_qualifier == "GO:0003700"
    assert association.subject_context_qualifier == "GO:0005634"
    assert association.predicate == "biolink:regulates"
    assert association.object == "UniProtKB:Q67890"
    assert association.object_activity_qualifier == "GO:0043565"
    assert association.object_process_qualifier == "GO:0003700"
    assert association.object_context_qualifier == "GO:0005634"
    assert association.original_predicate == "RO:0002629"
    assert association.publications == ["PMID:12345678"]
    # Check that sources are properly set
    assert association.sources is not None
    assert len(association.sources) >= 1
    primary_source = [s for s in association.sources if s.resource_role == "primary_knowledge_source"][0]
    assert primary_source.resource_id == "infores:go-cam"


# -- Pydantic round-trip fixtures & test --

_GO_CAM_SOURCES = [
    RetrievalSource(
        id="infores:go-cam",
        resource_id="infores:go-cam",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

EDGE_FIXTURES = [
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "00007103-672d-5079-8d8c-6a4b1dc880f6",
            "subject": "NCBIGene:16171",
            "predicate": "biolink:acts_upstream_of",
            "object": "NCBIGene:19695",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "a06e6f4a-e1fd-48e2-8a20-211210adcde9",
            "subject": "NCBIGene:10919",
            "predicate": "biolink:related_to",
            "object": "NCBIGene:23468",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "79badbdf-e594-4d5d-a4d6-3d8fd7e69f84",
            "subject": "NCBIGene:14526",
            "predicate": "biolink:regulates",
            "object": "NCBIGene:14527",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "f3a0ad16-fb39-4dd6-9223-79c01edef663",
            "subject": "NCBIGene:81545",
            "predicate": "biolink:is_input_of",
            "object": "NCBIGene:5595",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "006d851a-04d7-50c9-84f4-ebd652ed5a22",
            "subject": "NCBIGene:652968",
            "predicate": "biolink:causes",
            "object": "NCBIGene:84219",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "70b69544-7ac5-47fe-aa76-c5c9d8690136",
            "subject": "NCBIGene:65960",
            "predicate": "biolink:acts_upstream_of_or_within",
            "object": "NCBIGene:65960",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "3b6e144c-f380-4292-97bc-57d0f6b69733",
            "subject": "NCBIGene:7421",
            "predicate": "biolink:acts_upstream_of_negative_effect",
            "object": "NCBIGene:1365",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "cbdb38b8-4594-40aa-ad81-9a66f0e0376a",
            "subject": "NCBIGene:619665",
            "predicate": "biolink:has_part",
            "object": "NCBIGene:619665",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "709f5611-515c-418f-8c21-a4ecbe9c761b",
            "subject": "NCBIGene:29110",
            "predicate": "biolink:has_input",
            "object": "UniProtKB:M0R3E9",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "2e10150d-f59d-48fa-a4a4-49cb2d9e041b",
            "subject": "NCBIGene:11684",
            "predicate": "biolink:precedes",
            "object": "NCBIGene:15446",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "aeaf7444-16ef-4951-8cd1-cc6beccb259a",
            "subject": "NCBIGene:60391",
            "predicate": "biolink:part_of",
            "object": "NCBIGene:60391",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "bdc24992-5eb9-4df4-aa84-775c6326a977",
            "subject": "NCBIGene:12578",
            "predicate": "biolink:acts_upstream_of_or_within_negative_effect",
            "object": "NCBIGene:104394",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "750b3403-193b-4c8d-bbe8-3b5a6e9e9750",
            "subject": "NCBIGene:51548",
            "predicate": "biolink:directly_physically_interacts_with",
            "object": "NCBIGene:22800",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
        },
    },
    {
        "association_class": GeneToGeneAssociation,
        "params": {
            "id": "9ec3fd8d-8e85-4c74-8d2b-c22bf7d2e723",
            "subject": "UniProtKB:Q6ZSJ9-1",
            "predicate": "biolink:enabled_by",
            "object": "NCBIGene:1742",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GO_CAM_SOURCES,
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


# ---------------------------------------------------------------------------
# Reference, contributor and evidence parsing
#
# Every malformed input below was observed in a real
# go-cam-networkx.tar.gz release; they are not hypotheticals.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        # Already well-formed
        ("PMID:12345678", "PMID:12345678"),
        ("GO_REF:0000024", "GO_REF:0000024"),
        # Whitespace, tabs and case
        (" PMID:33331896", "PMID:33331896"),
        ("PMID:7796420 ", "PMID:7796420"),
        ("PMID:18224415\t", "PMID:18224415"),
        ("PMID: 17974916", "PMID:17974916"),
        ("PMID:     24755855", "PMID:24755855"),
        ("pmid:26030875", "PMID:26030875"),
        (" \tPMID:11466413", "PMID:11466413"),
        # Prefix lost entirely
        ("34782749", "PMID:34782749"),
        (" 1323144", "PMID:1323144"),
        # GO_REF misspellings
        ("GOREF:0000033", "GO_REF:0000033"),
        ("GO:REF:0000008", "GO_REF:0000008"),
        (" GO_REF:0000024", "GO_REF:0000024"),
        # A doubled colon
        ("PMID::29523808", "PMID:29523808"),
        # Other reference namespaces the source uses. MGI arrives with its prefix
        # doubled; PAINT_REF keeps its own prefix rather than folding into GO_REF.
        ("MGI:MGI:4417868", "MGI:4417868"),
        ("PAINT_REF:12107", "PAINT_REF:12107"),
        ("ISBN:0-87901-047-9", "ISBN:0-87901-047-9"),
        # Not publications: Reactome ids are pathway records, carried as
        # source_record_urls instead; a GO term in a reference field is a leak.
        ("Reactome:R-HSA-201451", None),
        ("GO:0005515", None),
        ("", None),
    ],
)
def test_normalize_reference(raw, expected):
    """Reference strings are repaired rather than matched against a literal prefix."""
    assert normalize_reference(raw) == expected


def test_extract_references_splits_pipe_delimited_values():
    """A pipe-packed reference must not lose the identifier riding along with it."""
    publications, unrecognized = extract_references("MGI:MGI:5005039 | PMID:21459323")
    assert publications == ["MGI:5005039", "PMID:21459323"]
    assert unrecognized == []


def test_extract_references_reports_what_it_dropped():
    """Unusable references are returned for reporting, not silently discarded."""
    publications, unrecognized = extract_references(
        ["PMID:12345678", "Reactome:R-HSA-201451", "GO_REF:0000024"]
    )
    assert publications == ["PMID:12345678", "GO_REF:0000024"]
    assert unrecognized == ["Reactome:R-HSA-201451"]


def test_extract_references_deduplicates():
    """The source repeats a reference once per evidence instance; the edge wants it once."""
    publications, _ = extract_references(["PMID:12345678", "PMID:12345678"])
    assert publications == ["PMID:12345678"]


@pytest.mark.parametrize(
    "raw,expected",
    [
        (["https://orcid.org/0000-0001-6330-7526"], ["ORCID:0000-0001-6330-7526"]),
        ("http://orcid.org/0000-0002-1825-0097", ["ORCID:0000-0002-1825-0097"]),
        # Curator-group ids are not people and not publications
        (["GOC:reactome_curators"], []),
        (["GOC:pde", "GOC:als"], []),
        # Mixed: keep the ORCID, drop the group id
        (["GOC:reactome_curators", "https://orcid.org/0000-0001-6330-7526"],
         ["ORCID:0000-0001-6330-7526"]),
        (None, []),
    ],
)
def test_extract_curator_orcids(raw, expected):
    """Only real ORCIDs become publications; opaque curator-group ids are dropped."""
    assert extract_curator_orcids(raw) == expected


@pytest.mark.parametrize(
    "raw,eco,rejected",
    [
        (["ECO:0000314"], ["ECO:0000314"], []),
        (["ECO:0000318", "ECO:0000318"], ["ECO:0000318"], []),
        (["MGI:MGI:5490144"], [], ["MGI:MGI:5490144"]),
        (["ECO:0000314", "MGI:MGI:5490144"], ["ECO:0000314"], ["MGI:MGI:5490144"]),
        (None, [], []),
    ],
)
def test_extract_evidence_codes(raw, eco, rejected):
    """Non-ECO values in an evidence field are rejected rather than emitted."""
    assert extract_evidence_codes(raw) == (eco, rejected)


# ---------------------------------------------------------------------------
# Agent type resolved from ECO, and the absence of a status filter
# ---------------------------------------------------------------------------


def _model(model_id, taxon="NCBITaxon:9606", status="production", **edge_fields):
    """Build a minimal single-edge GO-CAM model for transform-level assertions."""
    edge = {
        "source": "UniProtKB:P12345",
        "target": "UniProtKB:Q67890",
        "causal_predicate": "RO:0002629",
        "source_gene_molecular_function": "GO:0005515",
        "target_gene_molecular_function": "GO:0043565",
    }
    edge.update(edge_fields)
    return {
        "graph": {"model_info": {"id": model_id, "taxon": taxon, "status": status}},
        "nodes": [
            {"id": "UniProtKB:P12345", "label": "Test Gene 1"},
            {"id": "UniProtKB:Q67890", "label": "Test Gene 2"},
        ],
        "edges": [edge],
    }


def _transform(models):
    """Run the transform and return the flattened list of emitted edges."""
    writer = MockKozaWriter()
    from koza.transform import KozaTransform

    mock_koza = KozaTransform(writer=writer, extra_fields={}, mappings={})
    return [edge for graph in transform_go_cam_models(mock_koza, models) for edge in graph.edges]


@pytest.mark.parametrize(
    "eco_terms,expected_agent_type",
    [
        # Manual-assertion branch of ECO
        (["ECO:0000314"], AgentTypeEnum.manual_agent),  # direct assay
        (["ECO:0000315"], AgentTypeEnum.manual_agent),  # mutant phenotype
        (["ECO:0000318"], AgentTypeEnum.manual_agent),  # biological aspect of ancestor
        # Automatic-assertion branch: the only signal separating imported content
        (["ECO:0000313"], AgentTypeEnum.automated_agent),  # imported information
        (["ECO:0000363"], AgentTypeEnum.automated_agent),  # computational inference
        (["ECO:0000313", "ECO:0000363"], AgentTypeEnum.automated_agent),
        (["ECO:0000501"], AgentTypeEnum.automated_agent),  # evidence used in automatic assertion
        # A curator in the loop wins any mixed set
        (["ECO:0000313", "ECO:0000314"], AgentTypeEnum.manual_agent),
        # ECO terms not qualified by assertion method, and no evidence at all
        (["ECO:0000002"], AgentTypeEnum.manual_agent),  # bare "direct assay evidence"
        (None, AgentTypeEnum.manual_agent),
    ],
)
def test_agent_type_follows_eco_assertion_branch(eco_terms, expected_agent_type):
    """agent_type is resolved from ECO rather than hardcoded to manual."""
    edges = _transform([_model("gomodel:1", causal_predicate_assessed_by=eco_terms)])
    assert len(edges) == 1
    assert edges[0].agent_type == expected_agent_type


def test_eco_terms_are_emitted_as_has_evidence_of_type():
    """ECO codes backing the causal statement reach the edge."""
    edges = _transform(
        [_model("gomodel:1", causal_predicate_assessed_by=["ECO:0000314", "ECO:0000314"])]
    )
    assert edges[0].has_evidence_of_type == ["ECO:0000314"]


def test_edge_without_evidence_omits_has_evidence_of_type():
    """An evidence-free edge carries no evidence slot rather than an empty list."""
    edges = _transform([_model("gomodel:1")])
    assert edges[0].has_evidence_of_type is None


def test_qualifier_evidence_fields_are_not_merged_into_edge_evidence():
    """
    Only causal_predicate_assessed_by evidences the causal claim.

    The sibling *_assessed_by fields evidence the MF/BP/CC qualifier annotations, and
    folding them in would overstate the evidence behind the edge.
    """
    edges = _transform(
        [
            _model(
                "gomodel:1",
                causal_predicate_assessed_by=["ECO:0000314"],
                source_gene_product_assessed_by=["ECO:0000318"],
                target_gene_biological_process_assessed_by=["ECO:0000266"],
            )
        ]
    )
    assert edges[0].has_evidence_of_type == ["ECO:0000314"]


@pytest.mark.parametrize("status", ["production", "development", "internal_test", "review", None])
def test_model_status_is_never_filtered(status):
    """
    Every model status is ingested.

    This is a regression guard, not an oversight: all Reactome-derived GO-CAM models
    carry status "development", so filtering to production would silently delete the
    entire infores:reactome knowledge source from the graph.
    """
    edges = _transform([_model("gomodel:1", status=status)])
    assert len(edges) == 1


def test_reactome_development_model_survives_with_automated_agent_type():
    """
    The Reactome case end to end.

    A Reactome-derived model is flagged development and carries automatic-assertion
    evidence: it must still be emitted, with infores:reactome as primary knowledge
    source and an automated agent type.
    """
    edges = _transform(
        [
            _model(
                "gomodel:R-HSA-201451",
                status="development",
                causal_predicate_assessed_by=["ECO:0000313"],
                causal_predicate_contributors=["GOC:reactome_curators"],
                causal_predicate_has_reference=["Reactome:R-HSA-201451"],
            )
        ]
    )
    assert len(edges) == 1
    edge = edges[0]
    assert edge.agent_type == AgentTypeEnum.automated_agent
    assert edge.has_evidence_of_type == ["ECO:0000313"]
    primary = [s.resource_id for s in edge.sources if s.resource_role == ResourceRoleEnum.primary_knowledge_source]
    assert primary == ["infores:reactome"]
    # Reactome pathway ids are not literature and the contributor is a group id,
    # so this edge carries no publications.
    assert edge.publications is None


def test_publications_combine_references_and_curator_orcids():
    """Literature references and curator ORCIDs share the publications slot."""
    edges = _transform(
        [
            _model(
                "gomodel:1",
                causal_predicate_has_reference=["pmid:26030875", "GOREF:0000033"],
                causal_predicate_contributors=[
                    "https://orcid.org/0000-0001-6330-7526",
                    "GOC:pde",
                ],
            )
        ]
    )
    assert edges[0].publications == [
        "PMID:26030875",
        "GO_REF:0000033",
        "ORCID:0000-0001-6330-7526",
    ]


def test_mismatched_evidence_and_reference_lengths_are_read_independently():
    """
    The parallel arrays are not co-indexed and must not be zipped.

    Real models carry differing numbers of evidence codes, references and contributors
    on the same edge; each is read as its own set.
    """
    edges = _transform(
        [
            _model(
                "gomodel:1",
                causal_predicate_assessed_by=["ECO:0000314", "ECO:0000315", "ECO:0000353"],
                causal_predicate_has_reference=["PMID:12345678"],
                causal_predicate_contributors=["https://orcid.org/0000-0001-6330-7526"],
            )
        ]
    )
    edge = edges[0]
    assert edge.has_evidence_of_type == ["ECO:0000314", "ECO:0000315", "ECO:0000353"]
    assert edge.publications == ["PMID:12345678", "ORCID:0000-0001-6330-7526"]


def test_doctests():
    """The parsing helpers document themselves with the malformations they repair."""
    from translator_ingest.ingests.go_cam import go_cam

    results = doctest.testmod(go_cam)
    assert results.failed == 0


# ---------------------------------------------------------------------------
# Causal predicate mapping at the transform level
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "causal_predicate,predicate,direction",
    [
        ("RO:0002629", "biolink:regulates", "increased"),
        ("RO:0002630", "biolink:regulates", "decreased"),
        ("RO:0002411", "biolink:precedes", None),
        ("RO:0012010", "biolink:precedes", None),
        ("BFO:0000050", "biolink:part_of", None),
        # URI spellings must resolve identically to CURIEs
        ("http://purl.obolibrary.org/obo/RO_0002630", "biolink:regulates", "decreased"),
        ("obo:RO#RO_0002629", "biolink:regulates", "increased"),
    ],
)
def test_causal_predicate_mapping(causal_predicate, predicate, direction):
    """Predicates resolve through the generated RO/biolink table, direction included."""
    edges = _transform([_model("gomodel:1", causal_predicate=causal_predicate)])
    assert len(edges) == 1
    assert edges[0].predicate == predicate
    assert edges[0].object_direction_qualifier == direction


def test_positive_and_negative_regulation_stay_distinguishable():
    """
    Both directions collapse onto biolink:regulates, so direction carries the difference.

    Without object_direction_qualifier this mapping would make positive and negative
    regulation indistinguishable, which would be worse than the predicate it replaced.
    """
    up, down = _transform(
        [
            _model("gomodel:1", causal_predicate="RO:0002629"),
            _model("gomodel:2", causal_predicate="RO:0002630"),
        ]
    )
    assert up.predicate == down.predicate == "biolink:regulates"
    assert up.object_direction_qualifier == "increased"
    assert down.object_direction_qualifier == "decreased"


@pytest.mark.parametrize(
    "causal_predicate",
    [
        "RO:0002408",  # obsolete in RO
        "RO:0002418",  # causally upstream of or within - no sound biolink mapping
        "RO:0002332",  # regulates levels of
        "RO:0002614",  # is evidence with support from - not a causal relation
        "RO:9999999",  # unknown to the table entirely
    ],
)
def test_unmappable_predicates_drop_the_edge(causal_predicate):
    """
    An edge we cannot map soundly is dropped, never emitted as biolink:related_to.

    A related_to edge carries no reasoning signal and is worse than an absent edge,
    because absence is measurable.
    """
    assert _transform([_model("gomodel:1", causal_predicate=causal_predicate)]) == []


def test_no_ontology_term_ever_reaches_the_predicate_slot():
    """
    Whatever the source says, predicate is a Biolink CURIE or the edge is dropped.

    The raw RO term is preserved on original_predicate instead.
    """
    raw_predicates = [
        "RO:0002629", "RO:0002630", "RO:0002413", "RO:0002411", "RO:0002304",
        "RO:0012010", "RO:0012009", "RO:0002578", "BFO:0000050", "BFO:0000051",
        "RO:0002233", "RO:0002215", "RO:0002333", "RO:0002313",
        "RO:0002408", "RO:0002418", "RO:9999999",
    ]
    edges = _transform(
        [_model(f"gomodel:{i}", causal_predicate=p) for i, p in enumerate(raw_predicates)]
    )
    assert edges, "expected at least some edges to survive"
    for edge in edges:
        assert edge.predicate.startswith("biolink:")
        assert not edge.predicate.startswith(("RO:", "BFO:"))
        # the source term is not lost - it is kept as provenance
        assert edge.original_predicate.startswith(("RO:", "BFO:"))


def test_original_predicate_preserves_the_source_term_verbatim():
    """original_predicate is provenance and keeps the RO CURIE, not the biolink one."""
    edges = _transform([_model("gomodel:1", causal_predicate="RO:0002629")])
    assert edges[0].original_predicate == "RO:0002629"
    assert edges[0].predicate == "biolink:regulates"


def test_every_qualifier_emitted_is_permitted_on_the_association_class():
    """
    Every qualifier slot this ingest sets must be one GeneToGeneAssociation permits.

    The pydantic classes are permissive about attribute names, so an unpermitted
    qualifier does not raise - it just produces an edge the model does not sanction.
    This checks the schema directly, via the installed biolink_model rather than bmt's
    default, which is not necessarily the same version.
    """
    from translator_ingest.util.biolink import get_biolink_model_toolkit

    edges = _transform(
        [
            _model(
                "gomodel:1",
                causal_predicate="RO:0002629",
                source_gene_biological_process="GO:0060070",
                source_gene_occurs_in="GO:0005829",
                target_gene_biological_process="GO:0060070",
                target_gene_occurs_in="GO:0005634",
            )
        ]
    )
    edge = edges[0]
    emitted = {
        slot
        for slot in type(edge).model_fields
        if (slot.endswith("_qualifier") or slot in ("qualified_predicate", "qualifier", "qualifiers"))
        and getattr(edge, slot, None)
    }
    assert emitted, "expected the fixture to populate qualifiers"

    permitted = {
        slot.name.replace(" ", "_")
        for slot in get_biolink_model_toolkit().view.class_induced_slots("gene to gene association")
    }
    unpermitted = emitted - permitted
    assert not unpermitted, f"GeneToGeneAssociation does not permit: {sorted(unpermitted)}"


def test_qualified_statement_is_emitted_as_a_complete_set():
    """
    A regulating edge reads as one sentence: "<subject> causes increased activity of <object>".

    The three slots are emitted together; any one alone is not a readable statement.
    """
    edges = _transform([_model("gomodel:1", causal_predicate="RO:0002629")])
    edge = edges[0]
    assert edge.predicate == "biolink:regulates"
    assert edge.qualified_predicate == "biolink:causes"
    assert edge.object_aspect_qualifier == "activity"
    assert edge.object_direction_qualifier == "increased"


def test_no_edge_carries_a_direction_without_an_aspect():
    """
    Across every predicate in the source, a direction never dangles.

    This is the invariant behind dropping direction from the causally-upstream family:
    their RO direction describes the execution of a downstream process, not an activity
    of the object gene, so there is no aspect for it to qualify.
    """
    raw_predicates = [
        "RO:0002629", "RO:0002630", "RO:0002407", "RO:0002409", "RO:0002213", "RO:0002578",
        "RO:0002413", "RO:0002411", "RO:0002304", "RO:0002305", "RO:0012009", "RO:0012010",
        "RO:0002412", "BFO:0000050", "BFO:0000051", "RO:0002233", "RO:0002215", "RO:0002333",
    ]
    edges = _transform(
        [_model(f"gomodel:{i}", causal_predicate=p) for i, p in enumerate(raw_predicates)]
    )
    assert edges
    for edge in edges:
        if edge.object_direction_qualifier is not None:
            assert edge.object_aspect_qualifier is not None, (
                f"{edge.original_predicate} emitted a direction with no aspect"
            )
        if edge.qualified_predicate is not None:
            assert edge.object_aspect_qualifier and edge.object_direction_qualifier


def test_precedes_edges_carry_no_direction_but_keep_the_ro_term():
    """
    The causally-upstream family loses its direction, not its provenance.

    RO:0002304 and RO:0002305 are still distinguishable downstream via original_predicate.
    """
    up, down = _transform(
        [
            _model("gomodel:1", causal_predicate="RO:0002304"),
            _model("gomodel:2", causal_predicate="RO:0002305"),
        ]
    )
    for edge in (up, down):
        assert edge.predicate == "biolink:precedes"
        assert edge.object_direction_qualifier is None
        assert edge.object_aspect_qualifier is None
        assert edge.qualified_predicate is None
    assert up.original_predicate == "RO:0002304"
    assert down.original_predicate == "RO:0002305"


# ---------------------------------------------------------------------------
# Retrieval sources carry the records an edge came from
# ---------------------------------------------------------------------------


def test_reactome_pathway_reference_becomes_a_source_record_url():
    """
    Reactome pathway ids are provenance, not citations.

    They are the only reference the Reactome-derived edges carry, so without this they
    were dropped entirely and those edges looked reference-less.
    """
    edges = _transform(
        [
            _model(
                "gomodel:R-HSA-201451",
                causal_predicate="RO:0002629",
                causal_predicate_has_reference=["Reactome:R-HSA-201451"],
                causal_predicate_contributors=["GOC:reactome_curators"],
            )
        ]
    )
    edge = edges[0]
    primary = next(
        s for s in edge.sources if s.resource_role == ResourceRoleEnum.primary_knowledge_source
    )
    assert primary.resource_id == "infores:reactome"
    assert primary.source_record_urls == ["https://reactome.org/content/detail/R-HSA-201451"]
    # still not a publication
    assert edge.publications is None


def test_gocam_model_record_url_is_recorded_on_the_source():
    """The GO-CAM model an edge came from is addressable from the edge."""
    edges = _transform([_model("gomodel:0000000300000001", causal_predicate="RO:0002629")])
    primary = edges[0].sources[0]
    assert primary.resource_id == "infores:go-cam"
    assert primary.source_record_urls == [
        "https://live-go-cam.geneontology.io/product/json/low-level/0000000300000001.json"
    ]


def test_sources_are_built_per_edge_not_per_model():
    """
    Two edges in one Reactome model can cite different pathway records.

    Building sources once per model would give both edges whichever record came first.
    """
    model = _model("gomodel:R-HSA-201451", causal_predicate="RO:0002629",
                   causal_predicate_has_reference=["Reactome:R-HSA-201451"])
    second = dict(model["edges"][0])
    second["causal_predicate_has_reference"] = ["Reactome:R-HSA-999999"]
    model["edges"].append(second)
    first_edge, second_edge = _transform([model])
    assert first_edge.sources[0].source_record_urls == [
        "https://reactome.org/content/detail/R-HSA-201451"
    ]
    assert second_edge.sources[0].source_record_urls == [
        "https://reactome.org/content/detail/R-HSA-999999"
    ]


# ---------------------------------------------------------------------------
# The RIG must describe what the code actually emits
#
# Guards NCATSTranslator/translator-ingests#490 and #339, which found the RIG
# declaring predicates that do not exist in biolink-model and do not match the
# graph the ingest actually produces.
# ---------------------------------------------------------------------------

RIG_PATH = (
    Path(__file__).resolve().parents[4]
    / "src/translator_ingest/ingests/go_cam/go_cam_rig.yaml"
)


def _rig():
    import yaml

    return yaml.safe_load(RIG_PATH.read_text())


def _biolink_name(curie: str) -> str:
    return curie.removeprefix("biolink:").replace("_", " ")


def test_rig_predicates_exist_in_biolink_model():
    """Every predicate the RIG declares must be a real Biolink predicate."""
    from translator_ingest.util.biolink import get_biolink_model_toolkit

    toolkit = get_biolink_model_toolkit()
    invalid = [
        predicate
        for edge_type in _rig()["target_info"]["edge_type_info"]
        for predicate in edge_type["predicates"]
        if not toolkit.is_predicate(_biolink_name(predicate))
    ]
    assert not invalid, f"RIG declares predicates absent from biolink-model: {invalid}"


def test_rig_edge_properties_and_qualifiers_exist_in_biolink_model():
    """Edge properties and qualifier slots in the RIG must be real Biolink slots."""
    from translator_ingest.util.biolink import get_biolink_model_toolkit

    toolkit = get_biolink_model_toolkit()
    invalid = []
    for edge_type in _rig()["target_info"]["edge_type_info"]:
        for prop in edge_type.get("edge_properties") or []:
            if toolkit.get_element(_biolink_name(prop)) is None:
                invalid.append(prop)
        for qualifier in edge_type.get("qualifiers") or []:
            if toolkit.get_element(_biolink_name(qualifier["property"])) is None:
                invalid.append(qualifier["property"])
    assert not invalid, f"RIG declares slots absent from biolink-model: {invalid}"


def test_rig_predicates_match_the_predicates_the_transform_emits():
    """
    The RIG's declared predicates must be exactly the set the code produces.

    Runs every RO predicate the source is known to use through the transform and
    compares the emitted predicates against the RIG.
    """
    source_predicates = [
        "RO:0002629", "RO:0002630", "RO:0002407", "RO:0002409", "RO:0002213", "RO:0002578",
        "RO:0002413", "RO:0002411", "RO:0002304", "RO:0002305", "RO:0012009", "RO:0012010",
        "RO:0002412", "BFO:0000050", "BFO:0000051", "RO:0002233", "RO:0002215", "RO:0002333",
        "RO:0002313", "RO:0002418", "RO:0002408", "RO:0002332", "RO:0004046", "RO:0004047",
        "RO:0002614", "BFO:0000066",
    ]
    edges = _transform(
        [_model(f"gomodel:{i}", causal_predicate=p) for i, p in enumerate(source_predicates)]
    )
    emitted = {edge.predicate for edge in edges}
    declared = {
        predicate
        for edge_type in _rig()["target_info"]["edge_type_info"]
        for predicate in edge_type["predicates"]
    }
    assert emitted == declared, (
        f"RIG and code disagree. Only in code: {sorted(emitted - declared)}. "
        f"Only in RIG: {sorted(declared - emitted)}"
    )


# ---------------------------------------------------------------------------
# Multi-valued source fields
#
# The networkx export collapses several activity-level edges between the same pair of
# gene products into one row, so a field can carry the set of values seen across all of
# them. The lists are not co-indexed, so they cannot be zipped back apart.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("GO:0140693", "GO:0140693"),
        (["GO:0140693"], "GO:0140693"),
        # the source repeats a value once per underlying assertion
        (["GO:0140693", "GO:0140693"], "GO:0140693"),
        # genuinely ambiguous
        (["GO:0140693", "GO:0140036"], None),
        (["GO:0140693", "GO:0140036", "GO:0030674"], None),
        (None, None),
        ([], None),
    ],
)
def test_unambiguous_value(raw, expected):
    assert unambiguous_value(raw) == expected


def test_multiple_causal_predicates_become_multiple_edges():
    """
    A row with several distinct causal predicates is several claims, not one.

    Taking the first would silently discard the others - including, in real data,
    negative regulation dropped in favour of a positive claim.
    """
    edges = _transform(
        [_model("gomodel:1", causal_predicate=["RO:0002629", "RO:0002630", "RO:0002411"])]
    )
    assert len(edges) == 3
    assert [e.predicate for e in edges] == [
        "biolink:regulates",
        "biolink:regulates",
        "biolink:precedes",
    ]
    assert [e.object_direction_qualifier for e in edges] == ["increased", "decreased", None]
    # each edge keeps the RO term it came from
    assert [e.original_predicate for e in edges] == ["RO:0002629", "RO:0002630", "RO:0002411"]


def test_repeated_causal_predicate_is_one_edge():
    """A predicate repeated once per evidence instance is still a single claim."""
    edges = _transform([_model("gomodel:1", causal_predicate=["RO:0002629", "RO:0002629"])])
    assert len(edges) == 1


def test_split_drops_only_the_unmappable_predicate():
    """An unmappable predicate in a multi-predicate row does not sink its siblings."""
    edges = _transform(
        [_model("gomodel:1", causal_predicate=["RO:0002629", "RO:0002408", "RO:0002411"])]
    )
    assert [e.predicate for e in edges] == ["biolink:regulates", "biolink:precedes"]


def test_ambiguous_go_qualifier_is_omitted_not_guessed():
    """
    An ambiguous GO term is left unset rather than narrowed to an arbitrary member.

    The destination qualifier slots are single-valued, so there is no way to carry both,
    and picking the first asserts a specific claim the source does not make.
    """
    edges = _transform(
        [
            _model(
                "gomodel:1",
                causal_predicate="RO:0002629",
                source_gene_molecular_function=["GO:0140693", "GO:0140036"],
                target_gene_occurs_in=["GO:0005776", "GO:0043232"],
            )
        ]
    )
    edge = edges[0]
    assert edge.subject_activity_qualifier is None
    assert edge.object_context_qualifier is None


def test_omission_is_per_field_not_per_edge():
    """One ambiguous field does not strip the unambiguous ones beside it."""
    edges = _transform(
        [
            _model(
                "gomodel:1",
                causal_predicate="RO:0002629",
                source_gene_molecular_function="GO:0140693",
                source_gene_occurs_in=["GO:0043232", "GO:0005829"],
                target_gene_molecular_function="GO:0008962",
            )
        ]
    )
    edge = edges[0]
    assert edge.subject_activity_qualifier == "GO:0140693"
    assert edge.object_activity_qualifier == "GO:0008962"
    assert edge.subject_context_qualifier is None


def test_split_edges_share_the_row_level_qualifiers_and_provenance():
    """Splitting on predicate does not duplicate or reshuffle the rest of the row."""
    edges = _transform(
        [
            _model(
                "gomodel:1",
                causal_predicate=["RO:0002629", "RO:0002411"],
                source_gene_molecular_function="GO:0140693",
                causal_predicate_has_reference=["PMID:12345678"],
                causal_predicate_assessed_by=["ECO:0000314"],
            )
        ]
    )
    assert len(edges) == 2
    for edge in edges:
        assert edge.subject_activity_qualifier == "GO:0140693"
        assert edge.publications == ["PMID:12345678"]
        assert edge.has_evidence_of_type == ["ECO:0000314"]
    # separate statements get separate identifiers
    assert edges[0].id != edges[1].id


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("MGI:MGI:102463", "MGI:102463"),
        ("MGI:MGI:1921700", "MGI:1921700"),
        # already single-prefixed, left alone
        ("MGI:102463", "MGI:102463"),
        ("UniProtKB:Q13501", "UniProtKB:Q13501"),
    ],
)
def test_gene_ids_drop_the_doubled_mgi_prefix(raw, expected):
    """MGI gene ids arrive with the prefix doubled; the graph should carry one."""
    assert normalize_id(raw) == expected


def test_no_doubled_prefix_reaches_the_emitted_graph():
    """
    Neither node ids nor edge endpoints may carry MGI:MGI:.

    The raw form is kept on original_subject / original_object as provenance.
    """
    model = _model("gomodel:1", causal_predicate="RO:0002629")
    model["nodes"] = [
        {"id": "MGI:MGI:102463", "label": "Nfatc2 Mmus"},
        {"id": "MGI:MGI:1921700", "label": "Test Gene"},
    ]
    model["edges"][0]["source"] = "MGI:MGI:102463"
    model["edges"][0]["target"] = "MGI:MGI:1921700"

    writer = MockKozaWriter()
    from koza.transform import KozaTransform

    graphs = list(transform_go_cam_models(KozaTransform(writer=writer, extra_fields={}, mappings={}), [model]))
    nodes = [n for g in graphs for n in g.nodes]
    edges = [e for g in graphs for e in g.edges]

    assert {n.id for n in nodes} == {"MGI:102463", "MGI:1921700"}
    assert edges[0].subject == "MGI:102463"
    assert edges[0].object == "MGI:1921700"
    # provenance keeps the source spelling
    assert edges[0].original_subject == "MGI:MGI:102463"
