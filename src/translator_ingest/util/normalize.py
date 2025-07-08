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
from typing import Optional, Dict
from .http import post_query

from biolink_model.datamodel.pydanticmodel_v2 import (
    KnowledgeLevelEnum,
    Literal, NamedThing, Association, Gene
)


NODE_NORMALIZER_SERVER = "https://nodenormalization-sri.renci.org/get_normalized_nodes"

def node_normalizer_query(query: Dict) -> Optional[Dict]:
    """
    Wrapper for Node Normalizer http POST query request.
    :param query: JSON query input, as a Python dictionary
    :return: Optional[Dict] JSON-like result as multi-level Python dictionary
    """
    return post_query(url=NODE_NORMALIZER_SERVER, query=query, server="Node Normalizer")

# I'm not sure that this method is needed... copied from the
# reasoner-validator library, just to help thinking about NN
def convert_to_preferred(curie, allowed_list, nn_query=node_normalizer_query):
    """
    Given an input CURIE, consults the Node Normalizer
     to identify an acceptable CURIE match with prefix
      as requested in the input 'allowed_list' of prefixes.
    :param curie: str CURIE identifier of interest to convert to a preferred namespace
    :param allowed_list: List[str] of one or more acceptable CURIE
           namespaces from which to find at least one equivalent identifier
    :param nn_query: Node Normalizer accessor query wrapper
    :param nn_query: Node Normalizer query accessor method
    """
    query = {'curies': [curie]}
    result = nn_query(query=query)
    if not (result and curie in result and result[curie] and 'equivalent_identifiers' in result[curie]):
        return None
    new_ids = [v['identifier'] for v in result[curie]['equivalent_identifiers']]
    for nid in new_ids:
        if nid.split(':')[0] in allowed_list:
            return nid
    return None


def normalize_node(node: NamedThing, nn_query=node_normalizer_query) -> Optional[NamedThing]:
    """
    Calls the Node Normalizer ("NN") with the identifier of the given node
    and updates the node contents with NN resolved values for canonical identifier,
    node categories, and cross-references.

    Known limitation: this method does NOT reset the node.category field values at this time.

    :param node: target instance of a class object in the NameThing hierarchy
    :param nn_query: Node Normalizer accessor query wrapper

    :return: rewritten node entry; None, if a node cannot be resolved in NN
    """

    assert node.id, "normalize_node(node): empty node identifier?"

    query = {'curies': [node.id]}
    result = nn_query(query=query)

    # Sanity check about regular NN operation for all queries
    # that returns a result with key equal to the input node identifier
    assert node.id in result

    # Maybe nothing returned if the node identifier is unknown?
    if not result[node.id]:
        return None

    # retrieve the NN dictionary result
    # associated with the input node identifier
    node_identity = result[node.id]

    # Update the contents of the node with the NN result
    # The original identifier of the input node may be
    # demoted to xref, while the canonical identifier is reset

    # (sanity check for required fields... We assume that they ought to always be present?)
    assert "id" in node_identity and "identifier" in node_identity["id"]
    canonical_identifier = node_identity["id"]["identifier"]
    canonical_name = node_identity["id"]["label"]

    # Sanity check... in case the
    # original node xref and synonym field are empty (or not...)
    if node.xref is None:
        node.xref = list()

    if node.synonym is None:
        node.synonym = list()

    for entry in node_identity["equivalent_identifiers"]:

        # Don't include the canonical entry as xref
        if entry["identifier"] == canonical_identifier:
            continue

        # Otherwise, capture equivalent identifier as xref...
        if entry["identifier"] not in node.xref:
            node.xref.append(entry["identifier"])
        # ... and label as a synonym (except for the canonical name)
        if "label" in entry and \
                entry["label"] != canonical_name and \
                entry["label"] not in node.synonym:
            node.synonym.append(entry["label"])

    if node.id != canonical_identifier:

        # Reset the node.id to the canonical id...
        node.id = canonical_identifier

        # Add input.name moved to the list of synonyms
        # if it is identical to the canonical name
        if node.name != canonical_name and \
                node.name not in node.synonym:
            node.synonym.append(node.name)

    # Overwrite the node name with the canonical name...
    node.name = canonical_name

    # ... TODO: (Re-)set the node categories
    #           (Pydantic doesn't allow the following statement ... yet?)
    # node.category = node_identity["type"]

    # return the normalized node
    return node

def normalize_edge(edge: Association, nn_query=node_normalizer_query) -> Optional[Association]:
    """
    Rewrites the Association subject and object identifiers with their Node Normalizer canonical identifiers.
    :param edge: target instance of a class object in the Association hierarchy
    :param nn_query: Node Normalizer accessor query wrapper

    rewritten node entry; None, if an association subject or object cannot be resolved in NN
    """
    edge_subject = normalize_node(NamedThing(id=edge.subject, **{}), nn_query=nn_query)
    edge_object = normalize_node(NamedThing(id=edge.object, **{}), nn_query=nn_query)
    if not (edge_subject and edge_object):
        return None
    edge.subject = edge_subject.id
    edge.object = edge_object.id
    return edge
