# Wrapper module for Translator Node Normalization operations within
# the Translator Ingest pipeline.
#
# We'd like to code normalizer functions for nodes and edges that call
# the Translator Node Normalization tool.
#
# We assume that such functions will work upon Pydantic-encapsulated instances
# of the nodes and edges of interest: functions can take
# Pydantic objects as input and return Pydantic objects as outputs.
#
# Specifically:
#
# - A node normalization method will take an "original" (Primary Knowledge Source-published)
# node and record its canonical identifier and associated node annotation from the node normalizer.
#
# - An edge normalization method would take an edge and return the edge
# normalized with subject and object identifiers normalized to their
# canonical identifiers identified by the node normalizer.
#
# Although edge normalization could trigger normalization of the dereferenced nodes
# from the edge subject and object. However, some thought will need to be given for
# how the normalization - of the two distinct but interrelated collections
# containing KGX-coded Biolink entities - is best coordinated.
#
from typing import Any
from .http import post_query

from biolink_model.datamodel.pydanticmodel_v2 import NamedThing, Association


NODE_NORMALIZER_SERVER = "https://nodenormalization-sri.renci.org/get_normalized_nodes"


def convert_to_preferred(curie, allowed_list):
    """
    :param curie
    :param allowed_list
    """
    query = {'curies': [curie]}
    result = post_query(url=NODE_NORMALIZER_SERVER, query=query, server="Node Normalizer")
    if not (result and curie in result and result[curie] and 'equivalent_identifiers' in result[curie]):
        return None
    new_ids = [v['identifier'] for v in result[curie]['equivalent_identifiers']]
    for nid in new_ids:
        if nid.split(':')[0] in allowed_list:
            return nid
    return None

def normalize_node(node: NamedThing) -> NamedThing:
    return NamedThing()

def normalize_edge(edge: Association) -> Association:
    return Association()
