import pytest
from koza.runner import KozaRunner, KozaTransformHooks
from tests.unit.ingests import MockKozaWriter
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    ChemicalAffectsGeneAssociation,    ## ONLY for affects
    ChemicalGeneInteractionAssociation,    ## ONLY for interacts_with
    MacromolecularMachineHasSubstrateAssociation,    ## ONLY for has_substrate
)
import translator_ingest.util.biolink as util

## import what I'm testing
from translator_ingest.ingests.drugcentral.drugcentral import (
    omop_transform,
    bioactivity_transform,
)
## import from mapping file
from translator_ingest.ingests.drugcentral.mappings import (
    OMOP_RELATION_MAPPING,
    INFORES_MAPPING,
    ACTION_TYPE_MAPPING
) 


## omop_relationship
## testing record for "off-label use": has the extra edge-attribute
@pytest.fixture
def omop_off_label():
    writer = MockKozaWriter()
    ## example of df row after parsing with prepare, from notebook
    record = {
        "struct_id": 144,
        "relationship_name": "off-label use",
        "umls_cui": "C0022336",
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[omop_transform])
    )
    runner.run()
    return writer.items

def test_omop_output(omop_off_label):
    ## check basic output
    entities = omop_off_label
    assert entities
    ## 1 edge/association, 2 nodes
    assert len(entities) == 3

    ## Doing because entities includes Nodes as well
    association = [e for e in entities if isinstance(e, ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation)][0]
    assert association
    ## go through contents of association, test stuff that isn't hard-coded or isn't subject/object

    ## predicate + edge-attribute
    assert association.predicate == OMOP_RELATION_MAPPING["off-label use"]["predicate"]
    ## should have edge-attribute for off-label
    assert association.clinical_approval_status == "off_label_use"

    ## sources
    assert association.sources
    assert len(association.sources) == 1
    ## check source_record_url
    assert len(association.sources[0].source_record_urls) == 1


## act_table_full
## BIOLINK_DP_INTERACTS main edge (uses same code-block as plain interacts_with)
@pytest.fixture
def dp_interacts():
    writer = MockKozaWriter()
    ## example of df row after parsing with prepare, from notebook
    record = {
        "struct_id": 4192,
        "accession": "P02766",
        "action_type": "BINDING AGENT",
        "act_source": "DRUG LABEL",
        "act_source_url": "http://www.ema.europa.eu/docs/en_GB/document_library/EPAR_-_Product_Information/human/002294/WC500117862.pdf",
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[bioactivity_transform])
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
    assert association.predicate == ACTION_TYPE_MAPPING["BINDING AGENT"]["predicate"]
    assert association.causal_mechanism_qualifier == ACTION_TYPE_MAPPING["BINDING AGENT"]["qualifiers"]["causal_mechanism_qualifier"]
    ## sources/publications
    assert association.sources
    assert len(association.sources) == 1
    assert association.publications
    assert len(association.publications) == 1

## act_table_full
## BIOLINK_SUBSTRATE main edge
@pytest.fixture
def substrate():
    writer = MockKozaWriter()
    ## example of df row after parsing with prepare, from notebook
    record = {
        "struct_id": 618,
        "accession": "P31645",
        "action_type": "SUBSTRATE",
        "act_source": "WOMBAT-PK",
        "act_source_url": None,
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[bioactivity_transform])
    )
    runner.run()
    return writer.items

def test_substrate(substrate):
    ## check basic output
    entities = substrate
    assert entities
    ## 1 edge/association, 2 nodes
    assert len(entities) == 3

    ## Doing because entities includes Nodes as well
    association = [e for e in entities if isinstance(e, MacromolecularMachineHasSubstrateAssociation)][0]
    assert association
    ## check that subject is protein, object is chem
    assert association.subject == "UNIPROTKB:P31645"
    assert association.object == "DrugCentral:618"
    ## predicate/qualifier stuff
    assert association.predicate == ACTION_TYPE_MAPPING["SUBSTRATE"]["predicate"]
    ## shouldn't have any qualifiers - association currently doesn't have these slots
    ## sources/publications
    assert association.sources
    ## primary wombat + aggregator drugcentral
    assert len(association.sources) == 2
    ## primary
    primary_source = [i for i in association.sources if i.resource_role == "primary_knowledge_source"][0]
    assert primary_source.resource_id == INFORES_MAPPING["WOMBAT-PK"]
    ## aggregator
    aggregator = [i for i in association.sources if i.resource_role == "aggregator_knowledge_source"][0]
    assert aggregator.resource_id == util.INFORES_DRUGCENTRAL
    ## no publications
    assert association.publications is None


## act_table_full
## affects edge WITH extra edge
@pytest.fixture
def affects_extra():
    writer = MockKozaWriter()
    ## example of df row after parsing with prepare, from notebook
    record = {
        "struct_id": 296,
        "accession": "P11387",
        "action_type": "INHIBITOR",
        "act_source": "SCIENTIFIC LITERATURE",
        "act_source_url": "PMID:9875499",
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[bioactivity_transform])
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
    assert association_affects.predicate == ACTION_TYPE_MAPPING["INHIBITOR"]["predicate"]
    assert association_affects.qualified_predicate == ACTION_TYPE_MAPPING["INHIBITOR"]["qualifiers"]["qualified_predicate"]
    assert association_affects.object_direction_qualifier == ACTION_TYPE_MAPPING["INHIBITOR"]["qualifiers"]["object_direction_qualifier"]
    assert association_affects.object_aspect_qualifier == ACTION_TYPE_MAPPING["INHIBITOR"]["qualifiers"]["object_aspect_qualifier"]
    assert association_affects.causal_mechanism_qualifier == ACTION_TYPE_MAPPING["INHIBITOR"]["qualifiers"]["causal_mechanism_qualifier"]
    ## sources/publications
    assert association_affects.sources
    assert len(association_affects.sources) == 1
    assert association_affects.publications
    assert len(association_affects.publications) == 1

    ## check contents of extra direct-interacts edge
    ## Doing because entities includes Nodes as well
    association_extra = [e for e in entities if isinstance(e, ChemicalGeneInteractionAssociation)][0]
    assert association_extra
    ## predicate/qualifier stuff
    assert association_extra.predicate == ACTION_TYPE_MAPPING["INHIBITOR"]["extra_edge_pred"]
    ## shouldn't have any qualifier values
    assert association_extra.causal_mechanism_qualifier is None
    ## sources/publications
    assert association_extra.sources
    assert len(association_extra.sources) == 1
    assert association_extra.publications
    assert len(association_extra.publications) == 1
