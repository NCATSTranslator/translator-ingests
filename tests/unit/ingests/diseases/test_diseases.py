import pytest
from koza.runner import KozaRunner, KozaTransformHooks
from tests.unit.ingests import MockKozaWriter

## ADJUST based on what I am actually using
from biolink_model.datamodel.pydanticmodel_v2 import (
    Protein,
    Disease,
    CorrelatedGeneToDiseaseAssociation,
    GeneToDiseaseAssociation,
    RetrievalSource,
    ResourceRoleEnum,
)
from translator_ingest.ingests.diseases.diseases import (
    remove_duplicates,
    keep_rows_with_IDs,
    textmining_transform,
    knowledge_transform,
)
import pandas as pd


## ADJUST based on what I am actually using
INFORES_DISEASES = "infores:diseases"
INFORES_MEDLINEPLUS = "infores:medlineplus"
INFORES_AMYCO = "infores:amyco"


## NON-KOZA FUNCTIONS
@pytest.fixture
def starting_data():
    df = pd.DataFrame.from_records(
        data=[
            ## real duplicate data, KNOWLEDGE
            (
                "ENSP00000269703",
                "CYP4F22",
                "DOID:0060655",
                "Autosomal recessive congenital ichthyosis",
                "MedlinePlus",
                "CURATED",
                5.0,
            ),
            (
                "ENSP00000269703",
                "CYP4F22",
                "DOID:0060655",
                "Autosomal recessive congenital ichthyosis",
                "MedlinePlus",
                "CURATED",
                5.0,
            ),
            ## real data without IDs, KNOWLEDGE
            ("ABHD11-AS1", "ABHD11-AS1", "DOID:1928", "Williams-Beuren syndrome", "MedlinePlus", "CURATED", 5.0),
            (
                "ENSP00000227667",
                "APOC3",
                "AmyCo:26",
                "Apolipoprotein C-III associated Amyloidosis",
                "AmyCo",
                "CURATED",
                4.0,
            ),
        ],
        columns=[
            "protein_id",
            "protein_name",
            "disease_id",
            "disease_name",
            "source_db",
            "evidence_type",
            "confidence_score",
        ],
    )

    ## copied from py file
    ID_start_strings = {
        "protein_id": "ENSP",
        "disease_id": "DOID",
    }

    return df, ID_start_strings


def test_functions(starting_data):
    ## run remove_duplicates, see if output is as-expected
    cleaned_df, count_duplicates = remove_duplicates(starting_data[0])
    assert cleaned_df.shape[0] == 3
    assert count_duplicates == 1

    ## then run keep_rows_with_IDs, see if output is as-expected
    cleaned_df, dict_no_IDs = keep_rows_with_IDs(dataframe=starting_data[0], starting_strings=starting_data[1])
    assert cleaned_df.shape[0] == 1
    assert dict_no_IDs["ENSP"] == 1
    assert dict_no_IDs["DOID"] == 1


## TEXT-MINING
@pytest.fixture
def textmining_output():
    writer = MockKozaWriter()
    ## From searching resource file: grep -m 1 "ENSP"
    record = {
        "protein_id": "ENSP00000000233",
        "protein_name": "ARF5",
        "disease_id": "DOID:0111266",
        "disease_name": "Geroderma osteodysplasticum",
        "z_score": 4.774,
        "confidence_score": 2.387,
        "url": "https://diseases.jensenlab.org/Entity?documents=10&type1=9606&id1=ENSP00000000233&type2=-26&id2=DOID:0111266",
    }
    runner = KozaRunner(
        data=iter([record]), writer=writer, hooks=KozaTransformHooks(transform_record=[textmining_transform])
    )
    runner.run()
    return writer.items


def test_textmining_output(textmining_output):
    ## check basic output
    entities = textmining_output
    assert entities
    ## 1 edge/association, 2 nodes
    assert len(entities) == 3

    ## check association contents
    ## Doing because entities includes Nodes as well
    association = [e for e in entities if isinstance(e, CorrelatedGeneToDiseaseAssociation)][0]
    assert association
    ## go through contents of association, test stuff that isn't hard-coded or isn't subject/object
    assert association.z_score == 4.774
    assert association.has_confidence_score == 2.387

    ## sources stuff
    assert association.sources
    assert len(association.sources) == 1
    textmining_source = association.sources[0]
    assert isinstance(textmining_source, RetrievalSource)
    assert textmining_source.source_record_urls == [
        "https://diseases.jensenlab.org/Entity?documents=10&type1=9606&id1=ENSP00000000233&type2=-26&id2=DOID:0111266"
    ]

    protein = [e for e in entities if isinstance(e, Protein)][0]
    assert protein.id == "ENSEMBL:ENSP00000000233"

    disease = [e for e in entities if isinstance(e, Disease)][0]
    assert disease.id == "DOID:0111266"


## KNOWLEDGE
@pytest.fixture
def knowledge_output():
    writer = MockKozaWriter()
    ## From searching resource file: grep -m 10 "MedlinePlus" and "AmyCo"
    records = [
        {
            "protein_id": "ENSP00000016171",
            "protein_name": "COX15",
            "disease_id": "DOID:3762",
            "disease_name": "cytochrome-c oxidase deficiency disease",
            "source_db": "MedlinePlus",
            "evidence_type": "CURATED",
            "confidence_score": 5.0,
        },
        {
            "protein_id": "ENSP00000167586",
            "protein_name": "KRT14",
            "disease_id": "DOID:0050639",
            "disease_name": "Primary cutaneous amyloidosis",
            "source_db": "AmyCo",
            "evidence_type": "CURATED",
            "confidence_score": 4.0,
        },
    ]
    runner = KozaRunner(data=records, writer=writer, hooks=KozaTransformHooks(transform_record=[knowledge_transform]))
    runner.run()
    return writer.items


def test_knowledge_output(knowledge_output):
    ## check basic output
    entities = knowledge_output
    assert entities
    ## 2 edges/associations, 4 nodes
    assert len(entities) == 6

    ## check first record's transform (MedlinePlus)
    ## Doing because entities includes Nodes as well
    association1 = [e for e in entities if isinstance(e, GeneToDiseaseAssociation)][0]
    assert association1
    ## go through contents of association, test stuff that isn't hard-coded or isn't subject/object
    assert association1.has_confidence_score == 5.0
    ## sources stuff
    assert association1.sources
    assert len(association1.sources) == 2
    ## medlineplus primary
    source1 = association1.sources[0]
    assert isinstance(source1, RetrievalSource)
    assert source1.resource_id == INFORES_MEDLINEPLUS
    assert source1.resource_role == ResourceRoleEnum.primary_knowledge_source
    ## diseases aggregator
    source2 = association1.sources[1]
    assert isinstance(source2, RetrievalSource)
    assert source2.resource_id == INFORES_DISEASES
    assert source2.resource_role == ResourceRoleEnum.aggregator_knowledge_source
    ## nodes
    protein = [e for e in entities if isinstance(e, Protein)][0]
    assert protein.id == "ENSEMBL:ENSP00000016171"
    disease = [e for e in entities if isinstance(e, Disease)][0]
    assert disease.id == "DOID:3762"

    ## check second record's transform (AmyCo)
    ## Doing because entities includes Nodes as well
    association2 = [e for e in entities if isinstance(e, GeneToDiseaseAssociation)][1]
    assert association2
    ## go through contents of association, test stuff that isn't hard-coded or isn't subject/object
    assert association2.has_confidence_score == 4.0
    ## sources stuff
    assert association2.sources
    assert len(association2.sources) == 2
    ## amyco primary
    source1 = association2.sources[0]
    assert isinstance(source1, RetrievalSource)
    assert source1.resource_id == INFORES_AMYCO
    assert source1.resource_role == ResourceRoleEnum.primary_knowledge_source
    ## diseases aggregator
    source2 = association2.sources[1]
    assert isinstance(source2, RetrievalSource)
    assert source2.resource_id == INFORES_DISEASES
    assert source2.resource_role == ResourceRoleEnum.aggregator_knowledge_source
    ## nodes
    protein = [e for e in entities if isinstance(e, Protein)][1]
    assert protein.id == "ENSEMBL:ENSP00000167586"
    disease = [e for e in entities if isinstance(e, Disease)][1]
    assert disease.id == "DOID:0050639"
