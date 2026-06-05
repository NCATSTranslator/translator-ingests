import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    ChemicalOrDrugOrTreatmentAdverseEventAssociation,
    DiseaseToPhenotypicFeatureAssociation,
    GeneToDiseaseAssociation,
    GenotypeToVariantAssociation,
    VariantToDiseaseAssociation,
    FDAIDAAdverseEventEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
    Disease,
    PhenotypicFeature,
    ChemicalEntity,
    Gene,
    SequenceVariant,
)
from translator_ingest.ingests.cureid.cureid import (
    extract_cureid_source_version,
    get_condition_has_phenotype_edge,
    get_condition_has_phenotype_nodes,
    get_drug_applied_to_treat_condition_edge,
    get_drug_applied_to_treat_condition_nodes,
    get_drug_applied_to_treat_phenotype_edge,
    get_drug_applied_to_treat_phenotype_nodes,
    get_drug_has_adverse_event_edge,
    get_drug_has_adverse_event_nodes,
    get_gene_associated_with_condition_edge,
    get_gene_associated_with_condition_nodes,
    get_gene_has_sequence_variant_edge,
    get_gene_has_sequence_variant_nodes,
    get_sequence_variant_genetically_associated_with_condition_edge,
    get_sequence_variant_genetically_associated_with_condition_nodes,
    parse_cureid_version_records,
)


CUREID_SOURCES = [
    RetrievalSource(
        id="infores:cureid",
        resource_id="infores:cureid",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

# ── Fixtures: one per association type from cureid.py ───────────────────────
EDGE_FIXTURES = [
    {
        "association_class": ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
        "params": {
            "id": "uuid:cureid-chem-disease",
            "subject": "RXCUI:161",
            "predicate": "biolink:treats",
            "object": "MONDO:0100096",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CUREID_SOURCES,
            "publications": ["PMID:33332065"],
        },
    },
    {
        "association_class": ChemicalOrDrugOrTreatmentAdverseEventAssociation,
        "params": {
            "id": "uuid:cureid-adverse-event",
            "subject": "RXCUI:161",
            "predicate": "biolink:has_adverse_event",
            "object": "HP:0001945",
            "knowledge_level": KnowledgeLevelEnum.observation,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CUREID_SOURCES,
            "FDA_adverse_event_level": FDAIDAAdverseEventEnum.serious_adverse_event,
        },
    },
    {
        "association_class": DiseaseToPhenotypicFeatureAssociation,
        "params": {
            "id": "uuid:cureid-disease-pheno",
            "subject": "MONDO:0100096",
            "predicate": "biolink:has_phenotype",
            "object": "HP:0012735",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CUREID_SOURCES,
        },
    },
    {
        "association_class": GeneToDiseaseAssociation,
        "params": {
            "id": "uuid:cureid-gene-disease",
            "subject": "NCBIGene:7157",
            "predicate": "biolink:associated_with",
            "object": "MONDO:0100096",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CUREID_SOURCES,
        },
    },
    {
        "association_class": GenotypeToVariantAssociation,
        "params": {
            "id": "uuid:cureid-genotype-variant",
            "subject": "NCBIGene:7157",
            "predicate": "biolink:has_variant_part",
            "object": "HGVS:p.R175H",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CUREID_SOURCES,
        },
    },
    {
        "association_class": VariantToDiseaseAssociation,
        "params": {
            "id": "uuid:cureid-variant-disease",
            "subject": "HGVS:p.R175H",
            "predicate": "biolink:related_condition",
            "object": "MONDO:0100096",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": CUREID_SOURCES,
        },
    },
]


@pytest.mark.parametrize(
    "fixture",
    EDGE_FIXTURES,
    ids=lambda f: f["association_class"].__name__,
)
def test_pydantic_roundtrip(fixture):
    """Instantiate the association and round-trip through Pydantic serialization."""
    cls = fixture["association_class"]
    obj = cls(**fixture["params"])
    dumped = obj.model_dump()
    restored = cls.model_validate(dumped)
    assert restored == obj


def test_parse_cureid_version_records():
    """Parse non-empty JSONL metadata records."""
    metadata_text = """
{"id": "first", "source_versions": []}

{"id": "second", "source_versions": [{"name": "CURE-ID", "version": "reports_20260518T211409Z"}]}
"""

    records = list(parse_cureid_version_records(metadata_text))

    assert records == [
        {"id": "first", "source_versions": []},
        {
            "id": "second",
            "source_versions": [{"name": "CURE-ID", "version": "reports_20260518T211409Z"}],
        },
    ]


def test_extract_cureid_source_version():
    """Extract the CURE-ID source version from RASopathies Translator metadata."""
    records = [
        {
            "id": "cure_rasopathies_translator",
            "source_versions": [
                {
                    "name": "CURE-ID",
                    "version": "reports_20260518T211409Z",
                    "version_date": "2026-05-18",
                    "download_date": "2026-05-18",
                }
            ],
        }
    ]

    assert extract_cureid_source_version(records) == "reports_20260518T211409Z"


def test_condition_has_phenotype_nodes_trust_jsonl_roles():
    """Map condition and phenotype node types from JSONL field roles, not CURIE prefix."""
    record = {
        "condition": {"id": "MONDO:0007893", "name": "Noonan syndrome with multiple lentigines"},
        "phenotype": {"id": "MONDO:0003432", "name": "strabismus"},
    }

    nodes = get_condition_has_phenotype_nodes(record)

    assert nodes == [
        Disease(id="MONDO:0007893", name="Noonan syndrome with multiple lentigines"),
        PhenotypicFeature(id="MONDO:0003432", name="strabismus"),
    ]


def test_condition_has_phenotype_edge_payload():
    """Map aggregate CURE ID condition-phenotype payload onto edge provenance and support fields."""
    record = {
        "condition": {"id": "MONDO:0015280", "name": "cardiofaciocutaneous syndrome"},
        "predicate": {"id": "has_phenotype", "label": "has phenotype"},
        "phenotype": {"id": "HP:0001263", "name": "Global developmental delay"},
        "patient_count": 3,
        "case_report_count": 3,
        "evidence": [
            {
                "case_report": {"id": "case-1", "url": "https://cure.ncats.io/case-1"},
                "finding": {"source_value": "Developmental delays", "source_text": "Developmental delays"},
            },
            {
                "case_report": {"id": "case-2", "url": "https://cure.ncats.io/case-2"},
                "finding": {"source_value": "Developmental delays", "source_text": "Developmental delays"},
            },
            {
                "case_report": {"id": "case-2", "url": "https://cure.ncats.io/case-2"},
                "finding": {
                    "source_value": "Delayed psychomotor development",
                    "source_text": "Delayed psychomotor development",
                },
            },
        ],
    }

    edge = get_condition_has_phenotype_edge(record)

    assert edge.subject == "MONDO:0015280"
    assert edge.predicate == "biolink:has_phenotype"
    assert edge.object == "HP:0001263"
    assert edge.primary_knowledge_source == "infores:cureid"
    assert edge.knowledge_level == KnowledgeLevelEnum.observation
    assert edge.agent_type == AgentTypeEnum.manual_agent
    assert edge.evidence_count == 3
    assert edge.supporting_text == ["Developmental delays", "Delayed psychomotor development"]
    assert edge.sources[0].source_record_urls == [
        "https://cure.ncats.io/case-1",
        "https://cure.ncats.io/case-2",
    ]


def test_drug_applied_to_treat_phenotype_nodes_trust_jsonl_roles():
    """Map drug and phenotype node types from JSONL field roles."""
    record = {
        "drug": {"id": "CHEBI:75998", "name": "Trametinib"},
        "phenotype": {"id": "MONDO:0005045", "name": "hypertrophic cardiomyopathy"},
    }

    nodes = get_drug_applied_to_treat_phenotype_nodes(record)

    assert nodes == [
        ChemicalEntity(id="CHEBI:75998", name="Trametinib"),
        PhenotypicFeature(id="MONDO:0005045", name="hypertrophic cardiomyopathy"),
    ]


def test_drug_applied_to_treat_phenotype_edge_payload():
    """Map treatment outcomes to interim attributes until Biolink supports treatment_outcome."""
    record = {
        "drug": {"id": "CHEBI:75998", "name": "Trametinib"},
        "predicate": {"id": "applied_to_treat", "label": "applied to treat"},
        "phenotype": {"id": "MONDO:0005045", "name": "hypertrophic cardiomyopathy"},
        "patient_count": 3,
        "case_report_count": 3,
        "outcomes": ["Patient improved", "Patient improved", "Patient fully recovered"],
        "evidence": [
            {
                "case_report": {"id": "case-1", "url": "https://cure.ncats.io/case-1"},
                "finding": {"source_value": "Hypertrophic cardiomyopathy (with symptoms)"},
                "treatment_response": {"outcome": "Patient improved"},
            },
            {
                "case_report": {"id": "case-2", "url": "https://cure.ncats.io/case-2"},
                "finding": {"source_value": "Hypertrophic cardiomyopathy (with symptoms)"},
                "treatment_response": {"outcome": "Patient fully recovered"},
            },
        ],
    }

    edge = get_drug_applied_to_treat_phenotype_edge(record)

    assert edge.subject == "CHEBI:75998"
    assert edge.predicate == "biolink:applied_to_treat"
    assert edge.object == "MONDO:0005045"
    assert edge.primary_knowledge_source == "infores:cureid"
    assert edge.knowledge_level == KnowledgeLevelEnum.observation
    assert edge.agent_type == AgentTypeEnum.manual_agent
    assert edge.evidence_count == 3
    assert edge.has_attribute == [
        "CUREID:treatment_outcome_patient_improved",
        "CUREID:treatment_outcome_patient_fully_recovered",
    ]
    assert edge.supporting_text == [
        "Hypertrophic cardiomyopathy (with symptoms)",
        "treatment_outcome: Patient improved",
        "treatment_outcome: Patient fully recovered",
    ]
    assert edge.sources[0].source_record_urls == [
        "https://cure.ncats.io/case-1",
        "https://cure.ncats.io/case-2",
    ]


def test_drug_applied_to_treat_condition_nodes_trust_jsonl_roles():
    """Map drug and condition node types from JSONL field roles."""
    record = {
        "drug": {"id": "CHEBI:75998", "name": "Trametinib"},
        "condition": {"id": "MONDO:0018997", "name": "Noonan syndrome"},
    }

    nodes = get_drug_applied_to_treat_condition_nodes(record)

    assert nodes == [
        ChemicalEntity(id="CHEBI:75998", name="Trametinib"),
        Disease(id="MONDO:0018997", name="Noonan syndrome"),
    ]


def test_drug_applied_to_treat_condition_edge_payload():
    """Map aggregate treatment condition payload onto edge provenance and support count fields."""
    record = {
        "drug": {"id": "CHEBI:75998", "name": "Trametinib"},
        "predicate": {"id": "applied_to_treat", "label": "applied to treat"},
        "condition": {"id": "MONDO:0018997", "name": "Noonan syndrome"},
        "patient_count": 4,
        "case_report_count": 4,
        "evidence": [
            {"case_report": {"id": "case-1", "url": "https://cure.ncats.io/case-1"}},
            {"case_report": {"id": "case-2", "url": "https://cure.ncats.io/case-2"}},
            {"case_report": {"id": "case-2", "url": "https://cure.ncats.io/case-2"}},
        ],
    }

    edge = get_drug_applied_to_treat_condition_edge(record)

    assert edge.subject == "CHEBI:75998"
    assert edge.predicate == "biolink:applied_to_treat"
    assert edge.object == "MONDO:0018997"
    assert edge.primary_knowledge_source == "infores:cureid"
    assert edge.knowledge_level == KnowledgeLevelEnum.observation
    assert edge.agent_type == AgentTypeEnum.manual_agent
    assert edge.evidence_count == 4
    assert edge.supporting_text is None
    assert edge.has_attribute is None
    assert edge.sources[0].source_record_urls == [
        "https://cure.ncats.io/case-1",
        "https://cure.ncats.io/case-2",
    ]


def test_drug_has_adverse_event_nodes_trust_jsonl_roles():
    """Map drug and adverse-event phenotype node types from JSONL field roles."""
    record = {
        "drug": {"id": "CHEBI:3892", "name": "Corticotropin"},
        "phenotype": {"id": "SNOMED:722919003", "name": "Left ventricular ejection fraction decreased"},
    }

    nodes = get_drug_has_adverse_event_nodes(record)

    assert nodes == [
        ChemicalEntity(id="CHEBI:3892", name="Corticotropin"),
        PhenotypicFeature(id="SNOMED:722919003", name="Left ventricular ejection fraction decreased"),
    ]


def test_drug_has_adverse_event_edge_payload_with_outcome():
    """Map aggregate adverse-event payload with FDA severity when outcomes are present."""
    record = {
        "drug": {"id": "CHEBI:3892", "name": "Corticotropin"},
        "predicate": {"id": "has_adverse_event", "label": "has adverse event"},
        "phenotype": {"id": "HP:0001639", "name": "Hypertrophic cardiomyopathy"},
        "patient_count": 1,
        "case_report_count": 1,
        "outcomes": ["Life-threatening"],
        "evidence": [
            {
                "case_report": {"id": "case-1", "url": "https://cure.ncats.io/case-1"},
                "adverse_event": {
                    "source_label": "Hypertrophic cardiomyopathy and decreased stroke volume",
                    "have_adverse_events": "Yes",
                    "outcomes": ["Life-threatening"],
                },
            },
        ],
    }

    edge = get_drug_has_adverse_event_edge(record)

    assert edge.subject == "CHEBI:3892"
    assert edge.predicate == "biolink:has_adverse_event"
    assert edge.object == "HP:0001639"
    assert edge.primary_knowledge_source == "infores:cureid"
    assert edge.knowledge_level == KnowledgeLevelEnum.observation
    assert edge.agent_type == AgentTypeEnum.manual_agent
    assert edge.evidence_count == 1
    assert edge.FDA_adverse_event_level == FDAIDAAdverseEventEnum.life_threatening_adverse_event
    assert edge.supporting_text == ["Hypertrophic cardiomyopathy and decreased stroke volume"]
    assert edge.sources[0].source_record_urls == ["https://cure.ncats.io/case-1"]


def test_drug_has_adverse_event_edge_payload_without_outcome():
    """Omit FDA severity when an adverse-event record has no outcome value."""
    record = {
        "drug": {"id": "RXCUI:36437", "name": "Sertraline 25 Mg Oral Tablet [Zoloft]"},
        "predicate": {"id": "has_adverse_event", "label": "has adverse event"},
        "phenotype": {"id": "HP:0002014", "name": "Diarrhea"},
        "patient_count": 1,
        "case_report_count": 1,
        "outcomes": [],
        "evidence": [
            {
                "case_report": {"id": "case-1", "url": "https://cure.ncats.io/case-1"},
                "adverse_event": {
                    "source_label": "Diarrhea",
                    "have_adverse_events": "Yes",
                    "outcomes": None,
                },
            },
        ],
    }

    edge = get_drug_has_adverse_event_edge(record)

    assert edge.subject == "RXCUI:36437"
    assert edge.predicate == "biolink:has_adverse_event"
    assert edge.object == "HP:0002014"
    assert edge.FDA_adverse_event_level is None
    assert edge.supporting_text == ["Diarrhea"]
    assert edge.sources[0].source_record_urls == ["https://cure.ncats.io/case-1"]


def test_gene_associated_with_condition_nodes_trust_jsonl_roles():
    """Map gene and condition node types from JSONL field roles."""
    record = {
        "gene": {"id": "NCBIGene:673", "symbol": "BRAF"},
        "condition": {"id": "MONDO:0015280", "name": "cardiofaciocutaneous syndrome"},
    }

    nodes = get_gene_associated_with_condition_nodes(record)

    assert nodes == [
        Gene(id="NCBIGene:673", name="BRAF"),
        Disease(id="MONDO:0015280", name="cardiofaciocutaneous syndrome"),
    ]


def test_gene_associated_with_condition_edge_payload():
    """Map aggregate gene condition payload onto edge provenance and diagnosis support fields."""
    record = {
        "gene": {"id": "NCBIGene:673", "symbol": "BRAF"},
        "predicate": {"id": "gene_associated_with_condition", "label": "gene associated with condition"},
        "condition": {"id": "MONDO:0015280", "name": "cardiofaciocutaneous syndrome"},
        "patient_count": 4,
        "case_report_count": 4,
        "evidence": [
            {
                "case_report": {"id": "case-1", "url": "https://cure.ncats.io/case-1"},
                "diagnosis": {"diagnosis_methods": ["Gene sequencing", "Imaging"]},
            },
            {
                "case_report": {"id": "case-2", "url": "https://cure.ncats.io/case-2"},
                "diagnosis": {"diagnosis_methods": ["Doctor suspected in utero", "Gene sequencing"]},
            },
        ],
    }

    edge = get_gene_associated_with_condition_edge(record)

    assert edge.subject == "NCBIGene:673"
    assert edge.predicate == "biolink:associated_with"
    assert edge.object == "MONDO:0015280"
    assert edge.primary_knowledge_source == "infores:cureid"
    assert edge.knowledge_level == KnowledgeLevelEnum.observation
    assert edge.agent_type == AgentTypeEnum.manual_agent
    assert edge.evidence_count == 4
    assert edge.supporting_text == ["Gene sequencing", "Imaging", "Doctor suspected in utero"]
    assert edge.sources[0].source_record_urls == [
        "https://cure.ncats.io/case-1",
        "https://cure.ncats.io/case-2",
    ]


def test_gene_has_sequence_variant_nodes_emit_local_variant():
    """Emit local CureID variant nodes even when no normalized variant xref exists."""
    record = {
        "gene": {"id": "NCBIGene:673", "symbol": "BRAF"},
        "gene_variant": {
            "id": "case-1:gene-variant:0",
            "xref": None,
            "source_gene_symbol": "BRAF",
            "variant_label": "Not reported",
        },
    }

    nodes = get_gene_has_sequence_variant_nodes(record)

    assert nodes == [
        Gene(id="NCBIGene:673", name="BRAF"),
        SequenceVariant(id="case-1:gene-variant:0", name="BRAF variant"),
    ]


def test_gene_has_sequence_variant_edge_payload():
    """Map aggregate gene local-variant payload onto edge provenance and support fields."""
    record = {
        "gene": {"id": "NCBIGene:673", "symbol": "BRAF"},
        "predicate": {"id": "has_sequence_variant", "label": "has sequence variant"},
        "gene_variant": {
            "id": "case-1:gene-variant:0",
            "xref": None,
            "source_gene_symbol": "BRAF",
            "nucleotide_change": "c.1914T>A",
            "protein_change": "p.D638E",
            "variant_label": "BRAF p.D638E",
        },
        "patient_count": 1,
        "case_report_count": 1,
        "evidence": [
            {
                "case_report": {"id": "case-1", "url": "https://cure.ncats.io/case-1"},
                "diagnosis": {"diagnosis_methods": ["Gene sequencing"]},
            },
        ],
    }

    edge = get_gene_has_sequence_variant_edge(record)

    assert edge.subject == "NCBIGene:673"
    assert edge.predicate == "biolink:has_sequence_variant"
    assert edge.object == "case-1:gene-variant:0"
    assert edge.primary_knowledge_source == "infores:cureid"
    assert edge.knowledge_level == KnowledgeLevelEnum.observation
    assert edge.agent_type == AgentTypeEnum.manual_agent
    assert edge.evidence_count == 1
    assert edge.supporting_text == [
        "source_gene_symbol: BRAF",
        "nucleotide_change: c.1914T>A",
        "protein_change: p.D638E",
        "variant_label: BRAF p.D638E",
        "Gene sequencing",
    ]
    assert edge.sources[0].source_record_urls == ["https://cure.ncats.io/case-1"]


def test_sequence_variant_genetically_associated_with_condition_nodes_emit_local_variant():
    """Map local variant and condition node types from JSONL field roles."""
    record = {
        "gene_variant": {
            "id": "case-1:gene-variant:0",
            "source_gene_symbol": "BRAF",
            "variant_label": "BRAF p.D638E",
        },
        "condition": {"id": "MONDO:0015280", "name": "cardiofaciocutaneous syndrome"},
    }

    nodes = get_sequence_variant_genetically_associated_with_condition_nodes(record)

    assert nodes == [
        SequenceVariant(id="case-1:gene-variant:0", name="BRAF p.D638E"),
        Disease(id="MONDO:0015280", name="cardiofaciocutaneous syndrome"),
    ]


def test_sequence_variant_genetically_associated_with_condition_edge_payload():
    """Map CureID's variant-condition predicate to Biolink related_condition."""
    record = {
        "gene_variant": {
            "id": "case-1:gene-variant:0",
            "xref": None,
            "source_gene_symbol": "BRAF",
            "nucleotide_change": "Not reported",
            "protein_change": "Not reported",
            "variant_label": "Not reported",
        },
        "predicate": {"id": "genetically_associated_with", "label": "genetically associated with"},
        "condition": {"id": "MONDO:0015280", "name": "cardiofaciocutaneous syndrome"},
        "patient_count": 1,
        "case_report_count": 1,
        "evidence": [
            {
                "case_report": {"id": "case-1", "url": "https://cure.ncats.io/case-1"},
                "diagnosis": {"diagnosis_methods": ["Gene sequencing"]},
            },
        ],
    }

    edge = get_sequence_variant_genetically_associated_with_condition_edge(record)

    assert edge.subject == "case-1:gene-variant:0"
    assert edge.predicate == "biolink:related_condition"
    assert edge.original_predicate is None
    assert edge.object == "MONDO:0015280"
    assert edge.primary_knowledge_source == "infores:cureid"
    assert edge.knowledge_level == KnowledgeLevelEnum.observation
    assert edge.agent_type == AgentTypeEnum.manual_agent
    assert edge.evidence_count == 1
    assert edge.supporting_text == ["source_gene_symbol: BRAF", "Gene sequencing"]
    assert edge.sources[0].source_record_urls == ["https://cure.ncats.io/case-1"]
