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
from typing import Optional
from .http import post_query

from biolink_model.datamodel.pydanticmodel_v2 import NamedThing, Association


NODE_NORMALIZER_SERVER = "https://nodenormalization-sri.renci.org/get_normalized_nodes"

# I'm not sure that this method is needed... copied from the
# reasoner-validator library, just to help thinking about NN
def convert_to_preferred(curie, allowed_list):
    """
    Given an input CURIE, consults the Node Normalizer
     to identify an acceptable CURIE match with prefix
      as requested in the input 'allowed_list' of prefixes.
    :param curie: str CURIE identifier of interest to convert to a preferred namespace
    :param allowed_list: List[str] of one or more acceptable CURIE
           namespaces from which to find at least one equivalent identifier
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


def normalize_node(node: NamedThing) -> Optional[NamedThing]:
    """
    Calls the Node Normalizer ("NN") with the identifier of the given node
    and updates the node contents with NN resolved values for canonical identifier,
    categories and cross-references.
    :param node: target instance of a class object in the NameThing hierarchy
    :return: rewritten node entry; None, if a node cannot be resolved in NN
    """
    assert node.id, "normalize_node(node): empty node identifier?"
    query = {'curies': [node.id]}
    result = post_query(url=NODE_NORMALIZER_SERVER, query=query, server="Node Normalizer")

    # Sanity check about regular NN operation for all queries
    assert node.id in result

    # Maybe nothing returned if node identifier is unknown?
    if not result[node.id]:
        return None

    # retrieve the NN dictionary result
    # associated with the input node identifier
    node_identity = result[node.id]

    # Update the contents of the node with the NN result
    # The original identifier of the input node may be
    # demoted to xref, while the canonical identifier is reset
    canonical_identifier = node_identity.id.identifier

    if node.id != canonical_identifier:
        # input node.id moved to cross-references ('xref'),
        # and input.name moved to the synonym slot list,...

        # Sanity check... in case the
        # original node xref and synonym field are empty (or not...)
        if node.xref is None:
            node.xref = list()

        if node.synonym is None:
            node.synonym = list()

        for entry in node_identity.equivalent_identifiers:

            # Don't include the canonical entry as xref
            if entry.identifier == canonical_identifier:
                continue

            # Otherwise, capture equivalent identifier as xref...
            if entry.identifier not in node.xref:
                node.xref.append(entry.identifier)
            # ... and label as a synonym
            if entry.label not in node.synonym:
                node.synonym.append(entry.label)

        # Reset the node.id to the canonical id...
        node.id = canonical_identifier

    else:
        # The original node identifier is already the canonical identifier;
        # however, the cross-reference and synonym slot values still need to be set.
        raise NotImplementedError("Implement me!")

    # For both cases (above), we:
    # - retain the input node name as a synonym
    if node.name not in node.synonym:
        node.synonym.append(node.name)
    # - overwrite the node name with the canonical identifier name...
    node.name = node_identity.id.label

    # ... and (Re-)set the node categories
    node.category = node_identity.type

    # return the normalized node
    return node

def normalize_edge(edge: Association) -> Optional[Association]:
    return None
