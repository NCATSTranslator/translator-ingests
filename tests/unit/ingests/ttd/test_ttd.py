import pytest
from koza.runner import KozaRunner, KozaTransformHooks
from tests.unit.ingests import MockKozaWriter

## ADJUST based on what I am actually using
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    DiseaseOrPhenotypicFeature,
    RetrievalSource,
    ChemicalAffectsGeneAssociation,    ## ONLY for affects
    ChemicalGeneInteractionAssociation,    ## ONLY for interacts_with
)
from translator_ingest.ingests.ttd.ttd import (
    p1_05_transform, p1_07_transform
)
from translator_ingest.ingests.ttd.mappings import MOA_MAPPING
import pandas as pd


## P1-05
@pytest.fixture
def p1_05_output():
    writer = MockKozaWriter()
    ## example of df row after parsing with P1_05_prepare
    record = {
        "subject_pubchem": "PUBCHEM.COMPOUND:136033680",
        "biolink_predicate": "biolink:in_clinical_trials_for",
        "clinical_approval_status": pd.NA,
        "max_research_phase": "clinical_trial_phase_2",
        "object_nameres_id": "MONDO:0018177",
        "subject_ttd_drug": {"D0V8AG"},
        "object_indication_name": {"Glioblastoma multiforme", "Recurrent glioblastoma"},
        "clinical_status": {"Phase 2"},
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[p1_05_transform])
    )
    runner.run()
    return writer.items


def test_p1_05_output(p1_05_output):
    ## check basic output
    entities = p1_05_output
    assert entities
    ## 1 edge/association, 2 nodes
    assert len(entities) == 3

    ## check association contents
    ## Doing because entities includes Nodes as well
    association = [e for e in entities if isinstance(e, ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation)][0]
    assert association
    ## placeholder: go through contents of association, test stuff that isn't hard-coded or isn't subject/object

    ## sources stuff
    assert association.sources
    assert len(association.sources) == 1
    p1_05_source = association.sources[0]
    assert isinstance(p1_05_source, RetrievalSource)
    assert p1_05_source.source_record_urls == [
        "https://ttd.idrblab.cn/data/drug/details/d0v8ag"
    ]

    chem = [e for e in entities if isinstance(e, ChemicalEntity)][0]
    assert chem.id == "PUBCHEM.COMPOUND:136033680"

    dop = [e for e in entities if isinstance(e, DiseaseOrPhenotypicFeature)][0]
    assert dop.id == "MONDO:0018177"


## P1-07

## PLAIN INTERACTS_WITH EDGE
@pytest.fixture
def p1_07_plain_interacts():
    writer = MockKozaWriter()
    ## example of df row after parsing with p1_07_prepare
    ## from notebook, row with multiple original TTD drug IDs
    record = {
        "subject_pubchem": "PUBCHEM.COMPOUND:71587700",
        "object_id": "NCBIGene:5578",
        "mod_moa": "NO_VALUE",
        "TargetID": {"T06413"},
        "DrugID": {"D0K4GI", "D02MFK"},
        "MOA": {"NO_VALUE"},
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[p1_07_transform])
    )
    runner.run()
    return writer.items

def test_07_plain_interacts(p1_07_plain_interacts):
    ## check basic output
    entities = p1_07_plain_interacts
    assert entities
    ## 1 edge/association, 2 nodes
    assert len(entities) == 3

    ## check association contents
    ## Doing because entities includes Nodes as well
    association = [e for e in entities if isinstance(e, ChemicalGeneInteractionAssociation)][0]
    assert association
    ## placeholder: go through contents of association, test stuff that isn't hard-coded or isn't subject/object

    ## predicate/qualifier stuff
    assert association.predicate == MOA_MAPPING["NO_VALUE"]["predicate"]
    ## shouldn't have any qualifier values
    assert association.causal_mechanism_qualifier is None

    ## sources stuff
    assert association.sources
    assert len(association.sources) == 1
    p1_07_source = association.sources[0]
    assert isinstance(p1_07_source, RetrievalSource)
    assert len(p1_07_source.source_record_urls) == 2


## BIOLINK_DP_INTERACTS main edge
@pytest.fixture
def p1_07_dp_interacts():
    writer = MockKozaWriter()
    ## example of df row after parsing with p1_07_prepare
    ## current mod_moa value that maps to "predicate": BIOLINK_DP_INTERACTS
    record = {
        "subject_pubchem": "PUBCHEM.COMPOUND:10045805",
        "object_id": "NCBIGene:706",
        "mod_moa": "BINDING",
        "TargetID": {"T75440"},
        "DrugID": {"D0GN5R"},
        "MOA": {"ligand"},
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[p1_07_transform])
    )
    runner.run()
    return writer.items

def test_07_dp_interacts(p1_07_dp_interacts):
    ## check basic output
    entities = p1_07_dp_interacts
    assert entities
    ## 1 edge/association, 2 nodes
    assert len(entities) == 3

    ## check association contents
    ## Doing because entities includes Nodes as well
    association = [e for e in entities if isinstance(e, ChemicalGeneInteractionAssociation)][0]
    assert association
    ## placeholder: go through contents of association, test stuff that isn't hard-coded or isn't subject/object

    ## predicate/qualifier stuff
    assert association.predicate == MOA_MAPPING["BINDING"]["predicate"]
    assert association.causal_mechanism_qualifier == MOA_MAPPING["BINDING"]["qualifiers"]["causal_mechanism_qualifier"]

    ## sources stuff
    assert association.sources
    assert len(association.sources) == 1
    p1_07_source = association.sources[0]
    assert isinstance(p1_07_source, RetrievalSource)
    assert len(p1_07_source.source_record_urls) == 1


## affects edge WITHOUT extra edge made
@pytest.fixture
def p1_07_affects_only():
    writer = MockKozaWriter()
    ## example of df row after parsing with p1_07_prepare
    ## current mod_moa value that maps to "predicate": BIOLINK_AFFECTS
    record = {
        "subject_pubchem": "PUBCHEM.COMPOUND:10016012",
        "object_id": "NCBIGene:4137",
        "mod_moa": "modulator",
        "TargetID": {"T45593"},
        "DrugID": {"D00GXL"},
        "MOA": {"modulator"},
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[p1_07_transform])
    )
    runner.run()
    return writer.items

def test_07_affects_only(p1_07_affects_only):
    ## check basic output
    entities = p1_07_affects_only
    assert entities
    ## 1 edge/association, 2 nodes
    assert len(entities) == 3

    ## check association contents
    ## Doing because entities includes Nodes as well
    association = [e for e in entities if isinstance(e, ChemicalAffectsGeneAssociation)][0]
    assert association
    ## placeholder: go through contents of association, test stuff that isn't hard-coded or isn't subject/object

    ## predicate/qualifier stuff
    assert association.predicate == MOA_MAPPING["modulator"]["predicate"]
    assert association.object_aspect_qualifier == MOA_MAPPING["modulator"]["qualifiers"]["object_aspect_qualifier"]
    assert association.causal_mechanism_qualifier == MOA_MAPPING["modulator"]["qualifiers"]["causal_mechanism_qualifier"]

    ## sources stuff
    assert association.sources
    assert len(association.sources) == 1
    p1_07_source = association.sources[0]
    assert isinstance(p1_07_source, RetrievalSource)
    assert len(p1_07_source.source_record_urls) == 1


## affects edge WITH extra edge
@pytest.fixture
def p1_07_affects_extra():
    writer = MockKozaWriter()
    ## example of df row after parsing with p1_07_prepare
    ## current mod_moa value that maps to BIOLINK_AFFECTS and has extra_edge_pred field
    record = {
        "subject_pubchem": "PUBCHEM.COMPOUND:11110698",
        "object_id": "NCBIGene:4907",
        "mod_moa": "inhibitor",
        "TargetID": {"T65116"},
        "DrugID": {"D0MA9U", "D0Y2QO"},
        "MOA": {"inhibitor"},
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[p1_07_transform])
    )
    runner.run()
    return writer.items

def test_07_affects_extra(p1_07_affects_extra):
    ## check basic output
    entities = p1_07_affects_extra
    assert entities
    ## 2 edge/association, 2 nodes
    assert len(entities) == 4

    ## check contents of main affects edge
    ## Doing because entities includes Nodes as well
    association_affects = [e for e in entities if isinstance(e, ChemicalAffectsGeneAssociation)][0]
    assert association_affects
    ## placeholder: go through contents of association, test stuff that isn't hard-coded or isn't subject/object
    ## predicate/qualifier stuff
    assert association_affects.predicate == MOA_MAPPING["inhibitor"]["predicate"]
    assert association_affects.qualified_predicate == MOA_MAPPING["inhibitor"]["qualifiers"]["qualified_predicate"]
    assert association_affects.object_direction_qualifier == MOA_MAPPING["inhibitor"]["qualifiers"]["object_direction_qualifier"]
    assert association_affects.object_aspect_qualifier == MOA_MAPPING["inhibitor"]["qualifiers"]["object_aspect_qualifier"]
    assert association_affects.causal_mechanism_qualifier == MOA_MAPPING["inhibitor"]["qualifiers"]["causal_mechanism_qualifier"]
    ## sources stuff
    assert association_affects.sources
    assert len(association_affects.sources) == 1
    p1_07_source = association_affects.sources[0]
    assert isinstance(p1_07_source, RetrievalSource)
    assert len(p1_07_source.source_record_urls) == 2

    ## check contents of extra direct-interacts edge
    ## Doing because entities includes Nodes as well
    association_extra = [e for e in entities if isinstance(e, ChemicalGeneInteractionAssociation)][0]
    assert association_extra
    ## placeholder: go through contents of association, test stuff that isn't hard-coded or isn't subject/object
    ## predicate/qualifier stuff
    assert association_extra.predicate == MOA_MAPPING["inhibitor"]["extra_edge_pred"]
    ## shouldn't have any qualifier values
    assert association_extra.causal_mechanism_qualifier is None
    ## sources stuff
    assert association_extra.sources
    assert len(association_extra.sources) == 1
    p1_07_source = association_extra.sources[0]
    assert isinstance(p1_07_source, RetrievalSource)
    assert len(p1_07_source.source_record_urls) == 2
