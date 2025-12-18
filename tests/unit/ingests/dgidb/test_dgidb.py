import pytest
from koza.runner import KozaRunner, KozaTransformHooks
from tests.unit.ingests import MockKozaWriter
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalAffectsGeneAssociation,    ## ONLY for affects
    ChemicalGeneInteractionAssociation,    ## ONLY for interacts_with
)
import translator_ingest.util.biolink as util

## import what I'm testing
from translator_ingest.ingests.dgidb.dgidb import (
    transform_row,
)
## import from mapping file
from translator_ingest.ingests.dgidb.mappings import (
    supporting_data_sources,
    publications,
    int_type_mapping,
)


## PLAIN INTERACTS_WITH EDGE
@pytest.fixture
def plain_interacts():
    writer = MockKozaWriter()
    ## example of df row after parsing with prepare, from notebook
    record = {
        "drug_concept_id": "RXCUI:9997",
        "gene_concept_id": "HGNC:7979",
        "mod_type": "~PLAIN_INTERACTS",
        "interaction_types": {"~NULL"},
        ## remember the parser overwrites sources column during sources special logic (vs notebook creates new column)
        "interaction_source_db_name": {"TdgClinicalTrial", "GuideToPharmacology", "ChEMBL", "TTD", "TEND"},
        "interaction_score": 0.429308,
        "evidence_score": 5.0,
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[transform_row])
    )
    runner.run()
    return writer.items

def test_plain_output(plain_interacts):
    ## check basic output
    entities = plain_interacts
    assert entities
    ## 1 edge/association, 2 nodes
    assert len(entities) == 3

    ## Doing because entities includes Nodes as well
    association = [e for e in entities if isinstance(e, ChemicalGeneInteractionAssociation)][0]
    assert association
    ## go through contents of association, test stuff that isn't hard-coded or isn't subject/object

    ## predicate/qualifier stuff
    assert association.predicate == int_type_mapping["~PLAIN_INTERACTS"]["predicate"]
    ## shouldn't have any qualifier values
    assert association.causal_mechanism_qualifier is None

    ## sources/publications
    assert association.sources
    ## primary dgidb + 3 of the interaction_source_db_name are keys in supporting_data_sources
    assert len(association.sources) == 4
    ## primary
    primary_source = [i for i in association.sources if i.resource_role == "primary_knowledge_source"][0]
    assert primary_source.resource_id == util.INFORES_DGIDB
    ## supporting
    supporting_sources = [i for i in association.sources if i.resource_role == "supporting_data_source"]
    assert len(supporting_sources) == 3
    supporting_infores = [i.resource_id for i in supporting_sources]

    assert set(supporting_infores) == {
        supporting_data_sources["GuideToPharmacology"],
        supporting_data_sources["ChEMBL"],
        supporting_data_sources["TTD"],
    }
    ## has 2 publications from other interaction_source_db_name elements
    assert association.publications
    assert len(association.publications) == 2
    assert set(association.publications) == {
        publications["TdgClinicalTrial"],
        publications["TEND"],
    }

    ## scores
    assert association.dgidb_interaction_score == 0.429308
    assert association.dgidb_evidence_score == 5


## BIOLINK_DP_INTERACTS main edge
@pytest.fixture
def dp_interacts():
    writer = MockKozaWriter()
    ## example of df row after parsing with prepare, from notebook
    record = {
        "drug_concept_id": "CHEMBL.COMPOUND:CHEMBL2109293",
        "gene_concept_id": "HGNC:4445",
        "mod_type": "binder",
        "interaction_types": {"binder"},
        ## remember the parser overwrites sources column (vs notebook creates new column)
        "interaction_source_db_name": {"ChEMBL"},
        ## in this test, I remove the scores fields to mimic the lack of scores (from scores special logic)
        ## in practice these scores seem to come in as nan
        ##     regular pipeline running seems to handle these without errors (added conversion to None)
        ##     but pytest's pydantic seems to error on them and I don't know why
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[transform_row])
    )
    runner.run()
    return writer.items

def test_dp_interacts(dp_interacts):
    ## check basic output
    entities = dp_interacts
    assert entities
    ## 1 edge/association, 2 nodes
    assert len(entities) == 3

    ## Doing because entities includes Nodes as well
    association = [e for e in entities if isinstance(e, ChemicalGeneInteractionAssociation)][0]
    assert association

    ## predicate/qualifier stuff
    assert association.predicate == int_type_mapping["binder"]["predicate"]
    assert association.causal_mechanism_qualifier == int_type_mapping["binder"]["qualifiers"]["causal_mechanism_qualifier"]

    ## sources/publications
    assert association.sources
    ## primary dgidb + supporting chembl
    assert len(association.sources) == 2
    ## primary
    primary_source = [i for i in association.sources if i.resource_role == "primary_knowledge_source"][0]
    assert primary_source.resource_id == util.INFORES_DGIDB
    ## supporting
    supporting_sources = [i for i in association.sources if i.resource_role == "supporting_data_source"]
    assert len(supporting_sources) == 1
    assert supporting_sources[0].resource_id == supporting_data_sources["ChEMBL"]
    ## no publications
    assert association.publications is None

    ## scores
    assert association.dgidb_interaction_score is None
    assert association.dgidb_evidence_score is None


## affects edge WITHOUT extra edge made
@pytest.fixture
def affects_only():
    writer = MockKozaWriter()
    ## example of df row after parsing with prepare, from notebook
    record = {
        "drug_concept_id": "RXCUI:8351",
        "gene_concept_id": "HGNC:4573",
        "mod_type": "modulator",
        "interaction_types": {"modulator"},
        ## remember the parser overwrites sources column during sources special logic (vs notebook creates new column)
        "interaction_source_db_name": {"GuideToPharmacology"},
        "interaction_score": 0.248590,
        "evidence_score": 1.0,
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[transform_row])
    )
    runner.run()
    return writer.items

def test_affects_only(affects_only):
    ## check basic output
    entities = affects_only
    assert entities
    ## 1 edge/association, 2 nodes
    assert len(entities) == 3

    ## Doing because entities includes Nodes as well
    association = [e for e in entities if isinstance(e, ChemicalAffectsGeneAssociation)][0]
    assert association

    ## predicate/qualifier stuff
    assert association.predicate == int_type_mapping["modulator"]["predicate"]
    assert association.object_aspect_qualifier == int_type_mapping["modulator"]["qualifiers"]["object_aspect_qualifier"]
    assert association.causal_mechanism_qualifier == int_type_mapping["modulator"]["qualifiers"]["causal_mechanism_qualifier"]

    ## sources/publications
    assert association.sources
    ## primary dgidb + supporting GuideToPharmacology == gtopdb
    assert len(association.sources) == 2
    ## primary
    primary_source = [i for i in association.sources if i.resource_role == "primary_knowledge_source"][0]
    assert primary_source.resource_id == util.INFORES_DGIDB
    ## supporting
    supporting_sources = [i for i in association.sources if i.resource_role == "supporting_data_source"]
    assert len(supporting_sources) == 1
    assert supporting_sources[0].resource_id == supporting_data_sources["GuideToPharmacology"]
    ## no publications
    assert association.publications is None

    ## scores
    assert association.dgidb_interaction_score == 0.248590
    assert association.dgidb_evidence_score == 1


## affects edge WITH extra edge
@pytest.fixture
def affects_extra():
    writer = MockKozaWriter()
    ## example of df row after parsing with prepare, from notebook
    record = {
        "drug_concept_id": "DRUGBANK:DB05169",
        "gene_concept_id": "HGNC:6192",
        "mod_type": "inhibitor",
        "interaction_types": {"inhibitor"},
        ## remember the parser overwrites sources column during sources special logic (vs notebook creates new column)
        "interaction_source_db_name": {"TALC", "MyCancerGenome"},
        "interaction_score": 0.096853,
        "evidence_score": 2.0,
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[transform_row])
    )
    runner.run()
    return writer.items

def test_affects_extra(affects_extra):
    ## check basic output
    entities = affects_extra
    assert entities
    ## 2 edge/association, 2 nodes
    assert len(entities) == 4


    ## check contents of main affects edge
    ## Doing because entities includes Nodes as well
    association_affects = [e for e in entities if isinstance(e, ChemicalAffectsGeneAssociation)][0]
    assert association_affects

    ## predicate/qualifier stuff
    assert association_affects.predicate == int_type_mapping["inhibitor"]["predicate"]
    assert association_affects.qualified_predicate == int_type_mapping["inhibitor"]["qualifiers"]["qualified_predicate"]
    assert association_affects.object_direction_qualifier == int_type_mapping["inhibitor"]["qualifiers"]["object_direction_qualifier"]
    assert association_affects.object_aspect_qualifier == int_type_mapping["inhibitor"]["qualifiers"]["object_aspect_qualifier"]
    assert association_affects.causal_mechanism_qualifier == int_type_mapping["inhibitor"]["qualifiers"]["causal_mechanism_qualifier"]

    ## sources/publications
    assert association_affects.sources
    ## primary dgidb + supporting MyCancerGenome
    assert len(association_affects.sources) == 2
    ## primary
    primary_source = [i for i in association_affects.sources if i.resource_role == "primary_knowledge_source"][0]
    assert primary_source.resource_id == util.INFORES_DGIDB
    ## supporting
    supporting_sources = [i for i in association_affects.sources if i.resource_role == "supporting_data_source"]
    assert len(supporting_sources) == 1
    assert supporting_sources[0].resource_id == supporting_data_sources["MyCancerGenome"]
    ## has 1 publication from other interaction_source_db_name element
    assert association_affects.publications
    assert len(association_affects.publications) == 1
    assert association_affects.publications[0] == publications["TALC"]

    ## scores
    assert association_affects.dgidb_interaction_score == 0.096853
    assert association_affects.dgidb_evidence_score == 2


    ## check contents of extra direct-interacts edge
    ## Doing because entities includes Nodes as well
    association_extra = [e for e in entities if isinstance(e, ChemicalGeneInteractionAssociation)][0]
    assert association_extra

    ## predicate/qualifier stuff
    assert association_extra.predicate == int_type_mapping["inhibitor"]["extra_edge_pred"]
    ## shouldn't have any qualifier values
    assert association_extra.causal_mechanism_qualifier is None

    ## sources/publications: special logic - same as original edge
    assert association_extra.sources
    ## primary dgidb + supporting MyCancerGenome
    assert len(association_extra.sources) == 2
    ## primary
    primary_source = [i for i in association_extra.sources if i.resource_role == "primary_knowledge_source"][0]
    assert primary_source.resource_id == util.INFORES_DGIDB
    ## supporting
    supporting_sources = [i for i in association_extra.sources if i.resource_role == "supporting_data_source"]
    assert len(supporting_sources) == 1
    assert supporting_sources[0].resource_id == supporting_data_sources["MyCancerGenome"]
    ## has 1 publication from other interaction_source_db_name element
    assert association_extra.publications
    assert len(association_extra.publications) == 1
    assert association_extra.publications[0] == publications["TALC"]

    ## no scores: CX special logic decision
    assert association_extra.dgidb_interaction_score is None
    assert association_extra.dgidb_evidence_score is None
