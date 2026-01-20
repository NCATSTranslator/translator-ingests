import pytest
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalAffectsGeneAssociation,
    ChemicalEntityToBiologicalProcessAssociation,
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    ChemicalEntityToPathwayAssociation,
    ChemicalEntity,
    DirectionQualifierEnum,
    Disease,
    Gene,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    Pathway,
    PhenotypicFeature,
    RetrievalSource,
)

from koza.runner import KozaRunner, KozaTransformHooks
from translator_ingest.ingests.ctd.ctd import (
    transform_chemical_to_disease as ctd_transform,
    transform_exposure_events,
    transform_chem_gene_ixns,
    transform_chem_go_enriched,
    transform_chem_pathways_enriched,
    transform_pheno_term_ixns,
    on_chem_gene_ixns_begin,
    on_pheno_ixns_begin,
    BIOLINK_AFFECTS,
    BIOLINK_ASSOCIATED_WITH,
    BIOLINK_CAUSES,
    BIOLINK_CORRELATED_WITH,
    BIOLINK_POSITIVELY_CORRELATED,
    BIOLINK_NEGATIVELY_CORRELATED,
    BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT,
)

from tests.unit.ingests import MockKozaWriter


@pytest.fixture
def therapeutic_output():
    writer = MockKozaWriter()
    record = {
        "ChemicalName": "10,11-dihydro-10-hydroxycarbamazepine",
        "ChemicalID": "C039775",
        "CasRN": "",
        "DiseaseName": "Epilepsy",
        "DiseaseID": "MESH:D004827",
        "DirectEvidence": "therapeutic",
        "InferenceGeneSymbol": "",
        "InferenceScore": "",
        "OmimIDs": "",
        "PubMedIDs": "17516704|123",
    }
    runner = KozaRunner(data=[record], writer=writer, hooks=KozaTransformHooks(transform_record=[ctd_transform]))
    runner.run()
    return writer.items


def test_therapeutic_entities(therapeutic_output):
    entities = therapeutic_output
    assert len(entities) == 3
    association = [e for e in entities if isinstance(e, ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation)][0]
    assert association.predicate == BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT
    assert "PMID:17516704" in association.publications
    assert "PMID:123" in association.publications

    assert (
        association.sources
        and isinstance(association.sources[0], RetrievalSource)
        and association.sources[0].resource_id == "infores:ctd"
    )

    disease = [e for e in entities if isinstance(e, Disease)][0]
    assert disease.id == "MESH:D004827"
    assert disease.name == "Epilepsy"

    chemical = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chemical.id == "MESH:C039775"
    assert chemical.name == "10,11-dihydro-10-hydroxycarbamazepine"


@pytest.fixture
def marker_mechanism_output():
    writer = MockKozaWriter()
    record = {
        "ChemicalName": "10,10-bis(4-pyridinylmethyl)-9(10H)-anthracenone",
        "ChemicalID": "C112297",
        "CasRN": "",
        "DiseaseName": "Hyperkinesis",
        "DiseaseID": "MESH:D006948",
        "DirectEvidence": "marker/mechanism",
        "InferenceGeneSymbol": "",
        "InferenceScore": "",
        "OmimIDs": "",
        "PubMedIDs": "19098162",
    }
    runner = KozaRunner(data=[record], writer=writer, hooks=KozaTransformHooks(transform_record=[ctd_transform]))
    runner.run()
    return writer.items


def test_marker_mechanism(marker_mechanism_output):
    entities = marker_mechanism_output
    assert len(entities) == 3
    association = [e for e in entities if isinstance(e, ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation)][0]
    assert association.predicate == BIOLINK_CORRELATED_WITH
    assert "PMID:19098162" in association.publications

    assert (
        association.sources
        and isinstance(association.sources[0], RetrievalSource)
        and association.sources[0].resource_id == "infores:ctd"
    )

    disease = [e for e in entities if isinstance(e, Disease)][0]
    assert disease.id == "MESH:D006948"
    assert disease.name == "Hyperkinesis"

    chemical = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chemical.id == "MESH:C112297"
    assert chemical.name == "10,10-bis(4-pyridinylmethyl)-9(10H)-anthracenone"


@pytest.fixture
def genetic_inference_output():
    writer = MockKozaWriter()
    record = {
        "ChemicalName": "10-(2-pyrazolylethoxy)camptothecin",
        "ChemicalID": "C534422",
        "CasRN": "",
        "DiseaseName": "Melanoma",
        "DiseaseID": "MESH:D008545",
        "DirectEvidence": "",
        "InferenceGeneSymbol": "CASP8",
        "InferenceScore": "4.23",
        "OmimIDs": "",
        "PubMedIDs": "18563783|21983787",
    }
    runner = KozaRunner(data=[record], writer=writer, hooks=KozaTransformHooks(transform_record=[ctd_transform]))
    runner.run()
    return writer.items


def test_genetic_inference(genetic_inference_output):
    entities = genetic_inference_output
    assert len(entities) == 3
    association = [e for e in entities if isinstance(e, ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation)][0]
    assert association.predicate == BIOLINK_ASSOCIATED_WITH
    assert "PMID:21983787" in association.publications

    assert (
        association.sources
        and isinstance(association.sources[0], RetrievalSource)
        and association.sources[0].resource_id == "infores:ctd"
    )

    assert association.has_confidence_score == 4.23

    disease = [e for e in entities if isinstance(e, Disease)][0]
    assert disease.id == "MESH:D008545"
    assert disease.name == "Melanoma"

    chemical = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chemical.id == "MESH:C534422"
    assert chemical.name == "10-(2-pyrazolylethoxy)camptothecin"


# ---- Tests for transform_exposure_events ----

@pytest.fixture
def exposure_events_positive_correlation_output():
    writer = MockKozaWriter()
    record = {
        "exposurestressorid": "D000082",
        "exposurestressorname": "Acetaminophen",
        "outcomerelationship": "positive correlation",
        "diseaseid": "D006505",
        "diseasename": "Hepatitis",
        "phenotypeid": "",
        "phenotypename": "",
        "reference": "12345678",
    }
    runner = KozaRunner(
        data=[record],
        writer=writer,
        hooks=KozaTransformHooks(transform_record=[transform_exposure_events])
    )
    runner.run()
    return writer.items


def test_exposure_events_positive_correlation(exposure_events_positive_correlation_output):
    entities = exposure_events_positive_correlation_output
    assert len(entities) == 3  # chemical, disease, association
    association = [e for e in entities if isinstance(e, ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation)][0]
    assert association.predicate == BIOLINK_POSITIVELY_CORRELATED
    assert association.subject == "MESH:D000082"
    assert association.object == "MESH:D006505"
    assert "PMID:12345678" in association.publications

    assert (
        association.sources
        and isinstance(association.sources[0], RetrievalSource)
        and association.sources[0].resource_id == "infores:ctd"
    )

    disease = [e for e in entities if isinstance(e, Disease)][0]
    assert disease.id == "MESH:D006505"

    chemical = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chemical.id == "MESH:D000082"


@pytest.fixture
def exposure_events_with_phenotype_output():
    writer = MockKozaWriter()
    record = {
        "exposurestressorid": "D000082",
        "exposurestressorname": "Acetaminophen",
        "outcomerelationship": "negative correlation",
        "diseaseid": "",
        "diseasename": "",
        "phenotypeid": "GO:0006915",
        "phenotypename": "apoptotic process",
        "reference": "87654321",
    }
    runner = KozaRunner(
        data=[record],
        writer=writer,
        hooks=KozaTransformHooks(transform_record=[transform_exposure_events])
    )
    runner.run()
    return writer.items


def test_exposure_events_with_phenotype(exposure_events_with_phenotype_output):
    entities = exposure_events_with_phenotype_output
    assert len(entities) == 3  # chemical, phenotype, association
    association = [e for e in entities if isinstance(e, ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation)][0]
    assert association.predicate == BIOLINK_NEGATIVELY_CORRELATED
    assert association.subject == "MESH:D000082"
    assert association.object == "GO:0006915"

    phenotype = [e for e in entities if isinstance(e, PhenotypicFeature)][0]
    assert phenotype.id == "GO:0006915"


@pytest.fixture
def exposure_events_no_output():
    """Test that records with unsupported outcome relationships return None."""
    writer = MockKozaWriter()
    record = {
        "exposurestressorid": "D000082",
        "exposurestressorname": "Acetaminophen",
        "outcomerelationship": "some unsupported relationship",
        "diseaseid": "D006505",
        "diseasename": "Hepatitis",
        "phenotypeid": "",
        "phenotypename": "",
        "reference": "12345678",
    }
    runner = KozaRunner(
        data=[record],
        writer=writer,
        hooks=KozaTransformHooks(transform_record=[transform_exposure_events])
    )
    runner.run()
    return writer.items


def test_exposure_events_unsupported_relationship(exposure_events_no_output):
    assert len(exposure_events_no_output) == 0


# ---- Tests for transform_chem_gene_ixns ----

@pytest.fixture
def chem_gene_ixns_increases_expression_output():
    writer = MockKozaWriter()
    record = {
        "ChemicalName": "Acetaminophen",
        "ChemicalID": "D000082",
        "CasRN": "103-90-2",
        "GeneSymbol": "CYP1A2",
        "GeneID": "1544",
        "GeneForms": "protein",
        "Organism": "Homo sapiens",
        "OrganismID": "9606",
        "Interaction": "Acetaminophen results in increased expression of CYP1A2 protein",
        "InteractionActions": "increases^expression",
        "PubMedIDs": "11111111|22222222",
    }
    runner = KozaRunner(
        data=[record],
        writer=writer,
        hooks=KozaTransformHooks(
            on_data_begin=[on_chem_gene_ixns_begin],
            transform_record=[transform_chem_gene_ixns]
        )
    )
    runner.run()
    return writer.items


def test_chem_gene_ixns_increases_expression(chem_gene_ixns_increases_expression_output):
    entities = chem_gene_ixns_increases_expression_output
    assert len(entities) == 3  # chemical, gene, association
    association = [e for e in entities if isinstance(e, ChemicalAffectsGeneAssociation)][0]
    assert association.predicate == BIOLINK_AFFECTS
    assert association.qualified_predicate == BIOLINK_CAUSES
    assert association.subject == "MESH:D000082"
    assert association.object == "NCBIGene:1544"
    assert association.object_direction_qualifier == DirectionQualifierEnum.increased
    assert association.object_aspect_qualifier == GeneOrGeneProductOrChemicalEntityAspectEnum.expression
    assert association.species_context_qualifier == "NCBITaxon:9606"
    assert "PMID:11111111" in association.publications
    assert "PMID:22222222" in association.publications

    assert (
        association.sources
        and isinstance(association.sources[0], RetrievalSource)
        and association.sources[0].resource_id == "infores:ctd"
    )

    gene = [e for e in entities if isinstance(e, Gene)][0]
    assert gene.id == "NCBIGene:1544"

    chemical = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chemical.id == "MESH:D000082"


@pytest.fixture
def chem_gene_ixns_decreases_activity_output():
    writer = MockKozaWriter()
    record = {
        "ChemicalName": "Aspirin",
        "ChemicalID": "D001241",
        "CasRN": "50-78-2",
        "GeneSymbol": "PTGS2",
        "GeneID": "5743",
        "GeneForms": "protein",
        "Organism": "Homo sapiens",
        "OrganismID": "9606",
        "Interaction": "Aspirin results in decreased activity of PTGS2",
        "InteractionActions": "decreases^activity",
        "PubMedIDs": "33333333",
    }
    runner = KozaRunner(
        data=[record],
        writer=writer,
        hooks=KozaTransformHooks(
            on_data_begin=[on_chem_gene_ixns_begin],
            transform_record=[transform_chem_gene_ixns]
        )
    )
    runner.run()
    return writer.items


def test_chem_gene_ixns_decreases_activity(chem_gene_ixns_decreases_activity_output):
    entities = chem_gene_ixns_decreases_activity_output
    assert len(entities) == 3
    association = [e for e in entities if isinstance(e, ChemicalAffectsGeneAssociation)][0]
    assert association.object_direction_qualifier == DirectionQualifierEnum.decreased
    assert association.object_aspect_qualifier == GeneOrGeneProductOrChemicalEntityAspectEnum.activity


@pytest.fixture
def chem_gene_ixns_affects_phosphorylation_output():
    writer = MockKozaWriter()
    record = {
        "ChemicalName": "Caffeine",
        "ChemicalID": "D002110",
        "CasRN": "58-08-2",
        "GeneSymbol": "AKT1",
        "GeneID": "207",
        "GeneForms": "protein",
        "Organism": "Homo sapiens",
        "OrganismID": "9606",
        "Interaction": "Caffeine affects phosphorylation of AKT1",
        "InteractionActions": "affects^phosphorylation",
        "PubMedIDs": "44444444",
    }
    runner = KozaRunner(
        data=[record],
        writer=writer,
        hooks=KozaTransformHooks(
            on_data_begin=[on_chem_gene_ixns_begin],
            transform_record=[transform_chem_gene_ixns]
        )
    )
    runner.run()
    return writer.items


def test_chem_gene_ixns_affects_phosphorylation(chem_gene_ixns_affects_phosphorylation_output):
    entities = chem_gene_ixns_affects_phosphorylation_output
    assert len(entities) == 3
    association = [e for e in entities if isinstance(e, ChemicalAffectsGeneAssociation)][0]
    assert association.object_direction_qualifier is None  # 'affects' doesn't set direction
    assert association.object_aspect_qualifier == GeneOrGeneProductOrChemicalEntityAspectEnum.phosphorylation


@pytest.fixture
def chem_gene_ixns_multiple_interactions_output():
    """Test that records with multiple interactions return None."""
    writer = MockKozaWriter()
    record = {
        "ChemicalName": "Complex Chemical",
        "ChemicalID": "D999999",
        "CasRN": "",
        "GeneSymbol": "GENE1",
        "GeneID": "1234",
        "GeneForms": "protein",
        "Organism": "Homo sapiens",
        "OrganismID": "9606",
        "Interaction": "Complex interaction",
        "InteractionActions": "increases^expression|decreases^activity",
        "PubMedIDs": "55555555",
    }
    runner = KozaRunner(
        data=[record],
        writer=writer,
        hooks=KozaTransformHooks(
            on_data_begin=[on_chem_gene_ixns_begin],
            transform_record=[transform_chem_gene_ixns]
        )
    )
    runner.run()
    return writer.items


def test_chem_gene_ixns_multiple_interactions_skipped(chem_gene_ixns_multiple_interactions_output):
    assert len(chem_gene_ixns_multiple_interactions_output) == 0


# ---- Tests for transform_chem_go_enriched ----

@pytest.fixture
def chem_go_enriched_output():
    writer = MockKozaWriter()
    record = {
        "ChemicalName": "Acetaminophen",
        "ChemicalID": "D000082",
        "Ontology": "Biological Process",
        "GOTermName": "response to drug",
        "GOTermID": "GO:0042493",
        "HighestGOLevel": "5",
        "PValue": "0.001",
        "CorrectedPValue": "0.005",
        "TargetMatchQty": "10",
        "TargetTotalQty": "100",
        "BackgroundMatchQty": "50",
        "BackgroundTotalQty": "5000",
    }
    runner = KozaRunner(
        data=[record],
        writer=writer,
        hooks=KozaTransformHooks(transform_record=[transform_chem_go_enriched])
    )
    runner.run()
    return writer.items


def test_chem_go_enriched(chem_go_enriched_output):
    entities = chem_go_enriched_output
    assert len(entities) == 3  # chemical, pathway, association
    association = [e for e in entities if isinstance(e, ChemicalEntityToBiologicalProcessAssociation)][0]
    assert association.predicate == BIOLINK_AFFECTS
    assert association.subject == "MESH:D000082"
    assert association.object == "GO:0042493"
    assert association.p_value == 0.001
    assert association.adjusted_p_value == 0.005

    assert (
        association.sources
        and isinstance(association.sources[0], RetrievalSource)
        and association.sources[0].resource_id == "infores:ctd"
    )

    pathway = [e for e in entities if isinstance(e, Pathway)][0]
    assert pathway.id == "GO:0042493"

    chemical = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chemical.id == "MESH:D000082"


# ---- Tests for transform_chem_pathways_enriched ----

@pytest.fixture
def chem_pathways_enriched_kegg_output():
    writer = MockKozaWriter()
    record = {
        "ChemicalName": "Acetaminophen",
        "ChemicalID": "D000082",
        "PathwayName": "Drug metabolism",
        "PathwayID": "KEGG:hsa00982",
        "PValue": "0.0001",
        "CorrectedPValue": "0.0005",
        "TargetMatchQty": "15",
        "TargetTotalQty": "200",
        "BackgroundMatchQty": "100",
        "BackgroundTotalQty": "10000",
    }
    runner = KozaRunner(
        data=[record],
        writer=writer,
        hooks=KozaTransformHooks(transform_record=[transform_chem_pathways_enriched])
    )
    runner.run()
    return writer.items


def test_chem_pathways_enriched_kegg(chem_pathways_enriched_kegg_output):
    entities = chem_pathways_enriched_kegg_output
    assert len(entities) == 3  # chemical, pathway, association
    association = [e for e in entities if isinstance(e, ChemicalEntityToPathwayAssociation)][0]
    assert association.predicate == BIOLINK_ASSOCIATED_WITH
    assert association.subject == "MESH:D000082"
    assert association.object == "KEGG.PATHWAY:hsa00982"  # KEGG should be replaced with KEGG.PATHWAY
    assert association.p_value == 0.0001
    assert association.adjusted_p_value == 0.0005

    assert (
        association.sources
        and isinstance(association.sources[0], RetrievalSource)
        and association.sources[0].resource_id == "infores:ctd"
    )

    pathway = [e for e in entities if isinstance(e, Pathway)][0]
    assert pathway.id == "KEGG.PATHWAY:hsa00982"

    chemical = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chemical.id == "MESH:D000082"


@pytest.fixture
def chem_pathways_enriched_react_output():
    writer = MockKozaWriter()
    record = {
        "ChemicalName": "Aspirin",
        "ChemicalID": "D001241",
        "PathwayName": "Arachidonic acid metabolism",
        "PathwayID": "REACT:R-HSA-2142753",
        "PValue": "0.00001",
        "CorrectedPValue": "0.00005",
        "TargetMatchQty": "20",
        "TargetTotalQty": "300",
        "BackgroundMatchQty": "150",
        "BackgroundTotalQty": "15000",
    }
    runner = KozaRunner(
        data=[record],
        writer=writer,
        hooks=KozaTransformHooks(transform_record=[transform_chem_pathways_enriched])
    )
    runner.run()
    return writer.items


def test_chem_pathways_enriched_react(chem_pathways_enriched_react_output):
    entities = chem_pathways_enriched_react_output
    assert len(entities) == 3
    association = [e for e in entities if isinstance(e, ChemicalEntityToPathwayAssociation)][0]
    assert association.object == "REACT:R-HSA-2142753"  # REACT should remain unchanged

    pathway = [e for e in entities if isinstance(e, Pathway)][0]
    assert pathway.id == "REACT:R-HSA-2142753"


# ---- Tests for transform_pheno_term_ixns ----

@pytest.fixture
def pheno_term_ixns_increases_output():
    writer = MockKozaWriter()
    record = {
        "chemicalname": "Acetaminophen",
        "chemicalid": "D000082",
        "casrn": "103-90-2",
        "phenotypename": "apoptotic process",
        "phenotypeid": "GO:0006915",
        "comentionedterms": "",
        "organism": "Homo sapiens",
        "organismid": "9606",
        "interaction": "Acetaminophen results in increased apoptotic process",
        "interactionactions": "increases^phenotype",
        "anatomyterms": "1^Lung^D008168",
        "pubmedids": "66666666|77777777",
    }
    runner = KozaRunner(
        data=[record],
        writer=writer,
        hooks=KozaTransformHooks(
            on_data_begin=[on_pheno_ixns_begin],
            transform_record=[transform_pheno_term_ixns]
        )
    )
    runner.run()
    return writer.items


def test_pheno_term_ixns_increases(pheno_term_ixns_increases_output):
    entities = pheno_term_ixns_increases_output
    assert len(entities) == 3  # chemical, phenotype, association
    association = [e for e in entities if isinstance(e, ChemicalEntityToBiologicalProcessAssociation)][0]
    assert association.predicate == BIOLINK_AFFECTS
    assert association.object_direction_qualifier == "increased"
    # The ctd.py code sets qualified_predicate when direction is specified
    assert association.qualified_predicate == BIOLINK_CAUSES
    assert association.subject == "MESH:D000082"
    assert association.object == "GO:0006915"
    assert association.species_context_qualifier == "NCBITaxon:9606"
    assert "MESH:D008168" in association.anatomical_context_qualifier
    assert "PMID:66666666" in association.publications
    assert "PMID:77777777" in association.publications

    assert (
        association.sources
        and isinstance(association.sources[0], RetrievalSource)
        and association.sources[0].resource_id == "infores:ctd"
    )

    phenotype = [e for e in entities if isinstance(e, PhenotypicFeature)][0]
    assert phenotype.id == "GO:0006915"

    chemical = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chemical.id == "MESH:D000082"


@pytest.fixture
def pheno_term_ixns_decreases_output():
    writer = MockKozaWriter()
    record = {
        "chemicalname": "Aspirin",
        "chemicalid": "D001241",
        "casrn": "50-78-2",
        "phenotypename": "inflammatory response",
        "phenotypeid": "GO:0006954",
        "comentionedterms": "",
        "organism": "Homo sapiens",
        "organismid": "9606",
        "interaction": "Aspirin results in decreased inflammatory response",
        "interactionactions": "decreases^phenotype",
        "anatomyterms": "1^Blood^D001769|2^Liver^D008099",
        "pubmedids": "88888888",
    }
    runner = KozaRunner(
        data=[record],
        writer=writer,
        hooks=KozaTransformHooks(
            on_data_begin=[on_pheno_ixns_begin],
            transform_record=[transform_pheno_term_ixns]
        )
    )
    runner.run()
    return writer.items


def test_pheno_term_ixns_decreases(pheno_term_ixns_decreases_output):
    entities = pheno_term_ixns_decreases_output
    assert len(entities) == 3
    association = [e for e in entities if isinstance(e, ChemicalEntityToBiologicalProcessAssociation)][0]
    assert association.object_direction_qualifier == "decreased"
    # qualified_predicate is set when direction is specified
    assert association.qualified_predicate == BIOLINK_CAUSES
    assert "MESH:D001769" in association.anatomical_context_qualifier
    assert "MESH:D008099" in association.anatomical_context_qualifier


@pytest.fixture
def pheno_term_ixns_affects_output():
    writer = MockKozaWriter()
    record = {
        "chemicalname": "Caffeine",
        "chemicalid": "D002110",
        "casrn": "58-08-2",
        "phenotypename": "cell proliferation",
        "phenotypeid": "GO:0008283",
        "comentionedterms": "",
        "organism": "Homo sapiens",
        "organismid": "9606",
        "interaction": "Caffeine affects cell proliferation",
        "interactionactions": "affects^phenotype",
        "anatomyterms": "1^Brain^D001921",
        "pubmedids": "99999999",
    }
    runner = KozaRunner(
        data=[record],
        writer=writer,
        hooks=KozaTransformHooks(
            on_data_begin=[on_pheno_ixns_begin],
            transform_record=[transform_pheno_term_ixns]
        )
    )
    runner.run()
    return writer.items


def test_pheno_term_ixns_affects(pheno_term_ixns_affects_output):
    entities = pheno_term_ixns_affects_output
    assert len(entities) == 3
    association = [e for e in entities if isinstance(e, ChemicalEntityToBiologicalProcessAssociation)][0]
    assert association.predicate == BIOLINK_AFFECTS
    # 'affects' doesn't set qualified_predicate
    assert association.qualified_predicate is None
    assert association.object_direction_qualifier is None
