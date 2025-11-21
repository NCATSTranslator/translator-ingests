"""
Unit tests of the annotate_rig.py code
"""
import pytest
import copy
from deepdiff import DeepDiff
from src.docs.scripts.annotate_rig import rewrite_property

@pytest.mark.parametrize(
    "given,properties,values,expected",
    [
        (   # Query 0 - insert a value (a list) for an empty property in the data structure
            {
                "source_info":
                {
                    "citations": None,
                    "terms_of_use_info":
                        {"terms_of_use_url": "https://hpo.jax.org/license"}
                }
            },
            ["source_info", "citations"],
            ["https://doi.org/10.1093/nar/gkaa1043"],
            {
                "source_info":
                {
                    "citations": ["https://doi.org/10.1093/nar/gkaa1043"],
                    "terms_of_use_info":
                        {"terms_of_use_url": "https://hpo.jax.org/license"}
                }
            }
        ),
        (   # Query 1 - insert a property plus value (a list) for
            #           a property missing in the data structure
            {
                "source_info":
                {
                    "terms_of_use_info":
                        {"terms_of_use_url": "https://hpo.jax.org/license"}
                }
            },
            ["source_info", "citations"],
            ["https://doi.org/10.1093/nar/gkaa1043"],
            {
                "source_info":
                {
                    "citations": ["https://doi.org/10.1093/nar/gkaa1043"],
                    "terms_of_use_info":
                        {"terms_of_use_url": "https://hpo.jax.org/license"}
                }
            }
        ),
        (  # Query 2 - delete the value of the given property in the data structure
                {
                    "source_info":
                    {
                        "citations": ["https://doi.org/10.1093/nar/gkaa1043"],
                        "terms_of_use_info":
                            {"terms_of_use_url": "https://hpo.jax.org/license"}
                    }
                },
                ["source_info", "citations"],
                None,
                {
                    "source_info":
                        {
                            "terms_of_use_info":
                                {"terms_of_use_url": "https://hpo.jax.org/license"}
                        }
                }
        ),
        (   # Query 3 - insert a value (a list of dictionaries) at an empty property
            #           in a deeper and (list) repetitive data structure
            {
                "target_info":
                {
                    "edge_type_info":
                    [
                        {
                            "subject_categories": ["biolink:SmallMolecule"],
                            "predicates": ["biolink:positively_correlated_with"],
                            "object_categories": ["biolink:Disease"],
                            "qualifiers": None,
                            "edge_properties": ["biolink:equivalent_identifiers"]
                        },
                        {
                            "subject_categories": ["biolink:SmallMolecule"],
                            "predicates": ["biolink:regulates"],
                            "object_categories": ["biolink:Gene"],
                            "qualifiers": None,
                            "edge_properties": ["biolink:equivalent_identifiers"]
                        }
                    ]
                }
            },
            ["target_info","edge_type_info","qualifiers"],
            [
                {
                    "property": "biolink:subject_feature_name",
                    "value_range": "str"
                },
                {
                    "property": "biolink:object_feature_name",
                    "value_range": "str"
                }
            ],
            {
                "target_info":
                {
                    "edge_type_info":
                    [
                        {
                            "subject_categories": ["biolink:SmallMolecule"],
                            "predicates": ["biolink:positively_correlated_with"],
                            "object_categories": ["biolink:Disease"],
                            "qualifiers": [
                                {
                                    "property": "biolink:subject_feature_name",
                                    "value_range": "str"
                                },
                                {
                                    "property": "biolink:object_feature_name",
                                    "value_range": "str"
                                }
                            ],
                            "edge_properties": ["biolink:equivalent_identifiers"]
                        },
                        {
                            "subject_categories": ["biolink:SmallMolecule"],
                            "predicates": ["biolink:regulates"],
                            "object_categories": ["biolink:Gene"],
                            "qualifiers": [
                                {
                                    "property": "biolink:subject_feature_name",
                                    "value_range": "str"
                                },
                                {
                                    "property": "biolink:object_feature_name",
                                    "value_range": "str"
                                }
                            ],
                            "edge_properties": ["biolink:equivalent_identifiers"]
                        }
                    ]
                }
            }
        ),
        (   # Query 4 - insert a value (a list of dictionaries) at a missing property
            #           in a deeper and (list) repetitive data structure
            {
                "target_info":
                    {
                        "edge_type_info":
                        [
                            {
                                "subject_categories": ["biolink:SmallMolecule"],
                                "predicates": ["biolink:positively_correlated_with"],
                                "object_categories": ["biolink:Disease"],
                                "edge_properties": ["biolink:equivalent_identifiers"]
                            },
                            {
                                "subject_categories": ["biolink:SmallMolecule"],
                                "predicates": ["biolink:regulates"],
                                "object_categories": ["biolink:Gene"],
                                "edge_properties": ["biolink:equivalent_identifiers"]
                            }

                        ]
                    }
            },
            ["target_info", "edge_type_info", "qualifiers"],
            [
                {
                    "property": "biolink:subject_feature_name",
                    "value_range": "str"
                },
                {
                    "property": "biolink:object_feature_name",
                    "value_range": "str"
                }
            ],
            {
                "target_info":
                {
                    "edge_type_info":
                    [
                        {
                            "subject_categories": ["biolink:SmallMolecule"],
                            "predicates": ["biolink:positively_correlated_with"],
                            "object_categories": ["biolink:Disease"],
                            "qualifiers": [
                                {
                                    "property": "biolink:subject_feature_name",
                                    "value_range": "str"
                                },
                                {
                                    "property": "biolink:object_feature_name",
                                    "value_range": "str"
                                }
                            ],
                            "edge_properties": ["biolink:equivalent_identifiers"]
                        },
                        {
                            "subject_categories": ["biolink:SmallMolecule"],
                            "predicates": ["biolink:regulates"],
                            "object_categories": ["biolink:Gene"],
                            "qualifiers": [
                                {
                                    "property": "biolink:subject_feature_name",
                                    "value_range": "str"
                                },
                                {
                                    "property": "biolink:object_feature_name",
                                    "value_range": "str"
                                }
                            ],
                            "edge_properties": ["biolink:equivalent_identifiers"]
                        }
                    ]
                }
            }
        ),
        (   # Query 5 - delete a property and its value (a list) for all equivalent
            #           path endpoints in a deeper, (list) repetitive data structure
            {
                "target_info":
                {
                    "edge_type_info":
                    [
                        {
                            "subject_categories": ["biolink:SmallMolecule"],
                            "predicates": ["biolink:positively_correlated_with"],
                            "object_categories": ["biolink:Disease"],
                            "qualifiers": [
                                {
                                    "property": "biolink:subject_feature_name",
                                    "value_range": "str"
                                },
                                {
                                    "property": "biolink:object_feature_name",
                                    "value_range": "str"
                                }
                            ],
                            "edge_properties": ["biolink:equivalent_identifiers"]
                        },
                        {
                            "subject_categories": ["biolink:SmallMolecule"],
                            "predicates": ["biolink:regulates"],
                            "object_categories": ["biolink:Gene"],
                            "qualifiers": [
                                {
                                    "property": "biolink:subject_feature_name",
                                    "value_range": "str"
                                },
                                {
                                    "property": "biolink:object_feature_name",
                                    "value_range": "str"
                                }
                            ],
                            "edge_properties": ["biolink:equivalent_identifiers"]
                        }
                    ]
                }
            },
            ["target_info","edge_type_info","qualifiers"],
            None,
            {
                "target_info":
                {
                    "edge_type_info":
                    [
                        {
                            "subject_categories": ["biolink:SmallMolecule"],
                            "predicates": ["biolink:positively_correlated_with"],
                            "object_categories": ["biolink:Disease"],
                            "edge_properties": ["biolink:equivalent_identifiers"]
                        },
                        {
                            "subject_categories": ["biolink:SmallMolecule"],
                            "predicates": ["biolink:regulates"],
                            "object_categories": ["biolink:Gene"],
                            "edge_properties": ["biolink:equivalent_identifiers"]
                        }

                    ]

                }
            }
        )
    ]
)
def test_rewrite_rewrite_property(given, properties, values,expected):
    # just work on a copy of the given data
    rig_data = copy.deepcopy(given)
    rewrite_property(rig_data=rig_data, properties=properties, values=values)
    diff = DeepDiff(rig_data, expected)
    assert diff == {}, f"Unexpected result discrepancy:\n,\t{diff}"
