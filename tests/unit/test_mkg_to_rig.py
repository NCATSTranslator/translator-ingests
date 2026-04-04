"""
Unit testing for bits of the mkg_to_rig.py code
"""
import pytest

from docs.scripts.mkg_to_rig import EdgeData


def test_current_id_management():
    edge_data = EdgeData(knowledge_level='knowledge_assertion',agent_type='manual_agent')
    assert edge_data.get_current_id() == 'global'  # default
    edge_data.set_current_id('test')
    assert edge_data.get_current_id() == 'test'
    edge_data.set_current_id('')
    assert not edge_data.get_current_id() # empty string
    edge_data.add_identifier('testing')
    edge_data.add_identifier('1')
    edge_data.add_identifier('2')
    edge_data.add_identifier('3')
    assert edge_data.get_current_id() == 'testing,1,2,3'

def test_edge_data_management():
    edge_data = EdgeData(knowledge_level='knowledge_assertion', agent_type='manual_agent')
    assert not edge_data.get_current_edge_data()  # empty dict

    data = edge_data.get_current_edge_data()  # retrieve the whole dictionary
    edge_data.add_value(key='key2', value='value1')
    edge_data.add_value(key='key2', value='value2')
    edge_data.add_value(key='key2', value='value3')
    assert len(data['key2']) == 3
    assert 'value2' in data['key2']

def test_process_qualifiers():
    edge_data = EdgeData(knowledge_level='knowledge_assertion', agent_type='manual_agent')

    # empty qualifier list
    edge_data.set_current_id('test')
    edge_data.process_qualifiers({})

    # default identity of edges lacking qualifiers & discriminating attributes
    assert edge_data.get_current_id() == 'global'

    edge_data.process_qualifiers(
        {
            "qualifiers": [
                {
                    "qualifier_type_id": "object_aspect_qualifier",
                    "applicable_values": [
                        "transport"
                    ]
                },
                {
                    "qualifier_type_id": "object_direction_qualifier",
                    "applicable_values": [
                        "increased"
                    ]
                },
                {
                    "qualifier_type_id": "qualified_predicate",
                    "applicable_values": [
                        "biolink:causes"
                    ]
                }
            ],
            "subject": "biolink:ChemicalEntity",
            "predicate": "biolink:correlated_with",
            "object": "biolink:Gene",
            "attributes": [
                {
                    "attribute_type_id": "biolink:knowledge_level",
                    "attribute_source": None,
                    "original_attribute_names": [
                        "knowledge_level"
                    ],
                    "constraint_use": False,
                    "constraint_name": None
                },
                {
                    "attribute_type_id": "biolink:agent_type",
                    "attribute_source": None,
                    "original_attribute_names": [
                        "agent_type"
                    ],
                    "constraint_use": False,
                    "constraint_name": None
                },
                {
                    "attribute_type_id": "biolink:publications",
                    "attribute_source": None,
                    "original_attribute_names": [
                        "publications"
                    ],
                    "constraint_use": False,
                    "constraint_name": None
                }
            ]
        }
    )
    assert edge_data.get_current_id() == \
            "object_aspect_qualifier=['transport']," \
            "object_direction_qualifier=['increased']," \
            "qualified_predicate=['biolink:causes'],biolink:publications"

    # assert edge_data.get_current_edge_data()['qualifier1'] == 'value1'
    # assert edge_data.get_current_edge_data()['qualifier2'] == 'value2'