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
# - The convert_to_preferred() method can convert a single curie into a preferred namespace
#
# - The normalize_identifiers() method can convert a list of input curies into their canonical identifiers
#
# - A node normalization method - normalize_node() - will take an "original" (Primary Knowledge Source-published)
#   (Pydantic NamedThing structured) node and record its canonical identifier and
#   associated node annotation from the node normalizer.
#
# - An edge normalization method - normalize_edge() - would take an edge and return
#   the edge normalized with subject and object identifiers normalized to their
#   canonical identifiers identified by the node normalizer.
#
# All of the above methods can use gene-to-protein and drug-to-chemical conflation to
# modify their results. The code defaults are for gene-to-protein conflation to be applied,
# but drug-to-chemical conflation to be avoided.
from typing import Optional, Union, List, Dict, Callable
import logging

from .http import post_query

from biolink_model.datamodel.pydanticmodel_v2 import (
    KnowledgeLevelEnum,
    Literal, NamedThing, Association, Gene
)

logger = logging.getLogger(__name__)

class Normalizer:
    """
    Wrapper for Node Normalizer operations.
    """

    NODE_NORMALIZER_SERVER = "https://nodenormalization-sri.renci.org/get_normalized_nodes"

    @staticmethod
    def _query(
        curies: List[str],
        description: bool = False,
        gp_conflate: bool = True,
        dc_conflate: bool = False
    ) -> Dict[str, Union[str, List[str], bool]]:
        """
        Compose a well formed input query dictionary for the Node Normalization "get_normalized_nodes" endpoint.
        :param curies: List[str] of curies to be matched for normalization.
        :param description: bool, if True, return description associated with identifier (default False)
        :param gp_conflate: bool, apply Gene-Protein conflation (default: True)
        :param dc_conflate:  bool, apply Drug-Chemical conflation (default: False)
        :return: Dict[str, Union[str, List[str], bool]] well-formed "get_normalized_nodes" query parameter
        """
        query = {'curies': curies, 'description': description}
        if gp_conflate:
            query["conflate"] = True
        else:
            query["conflate"] = False

        if dc_conflate:
            query["drug_chemical_conflate"] = True
        else:
            query["drug_chemical_conflate"] = False
        return query


    @classmethod
    def get_normalized_nodes(cls, query: Dict) -> Optional[Dict]:
        """
        Wrapper for Node Normalizer http POST query request,
        implementing a remote web server implementation of the Node Normalizer.
        :param query: JSON query input, as a Python dictionary
        :return: Optional[Dict] JSON-like result as multi-level Python dictionary
        """
        return post_query(url=cls.NODE_NORMALIZER_SERVER, query=query, server="Node Normalizer")

    def __init__(self, endpoint: Callable = get_normalized_nodes):
        """
        Constructs a Normalizer instance. See https://nodenormalization-sri.renci.org/docs for parameters of query.
        :param endpoint: Node Normalizer access method with protocol f(query: Dict) -> Optional[Dict]
        """
        self.node_normalizer = endpoint

    # I'm not sure that this method is needed... copied from the
    # reasoner-validator library, just to help thinking about NN
    def convert_to_preferred(
            self,
            curie: str,
            allowed_list: List[str],
            gp_conflate: bool = True,
            dc_conflate: bool = False
    ) -> Optional[str]:
        """
        Given an input CURIE, consults the Node Normalizer
        to identify an acceptable CURIE match with prefix
        as requested in the input 'allowed_list' of prefixes.

        :param curie: str CURIE identifier of interest to convert to a preferred namespace
        :param gp_conflate: bool, apply Gene-Protein conflation (default: True)
        :param dc_conflate:  bool, apply Drug-Chemical conflation (default: False)
        :param allowed_list: List[str] of one or more acceptable CURIE
               namespaces from which to find at least one equivalent identifier
        :return: Optional[str] curie if found, within the allowable list of prefix namespaces
        """
        assert curie, "curie must be non-empty string"

        result = self.node_normalizer(
            query=self._query(
                curies=[curie],
                gp_conflate=gp_conflate,
                dc_conflate=dc_conflate
            )
        )

        if not (result and curie in result and result[curie] and 'equivalent_identifiers' in result[curie]):
            return None
        new_ids = [v['identifier'] for v in result[curie]['equivalent_identifiers']]
        for nid in new_ids:
            if nid.split(':')[0] in allowed_list:
                return nid
        return None

    def normalize_identifiers(
            self,
            curies: List[str],
            gp_conflate: bool = True,
            dc_conflate: bool = False
    ) -> Dict[str, Optional[str]]:
        """
        Calls the Node Normalizer ("NN") with a list of curies,
        returning a dictionary of canonical identifier mappings.

        :param curies: non-empty list of CURIE identifiers to be normalized
        :param gp_conflate: bool, apply Gene-Protein conflation (default: True)
        :param dc_conflate:  bool, apply Drug-Chemical conflation (default: False)
        :return: dictionary mappings of input identifiers to canonical identifiers (None if unknown)
        """
        assert curies, "normalize_identifiers(curies): empty list of CURIE identifiers?"

        result = self.node_normalizer(
            query=self._query(
                curies=curies,
                gp_conflate=gp_conflate,
                dc_conflate=dc_conflate
            )
        )

        assert result, "normalize_identifiers(curies): no result returned from the Node Normalizer?"

        mappings: Dict[str, Optional[str]] = {}
        for identifier in curies:
            # Sanity check: was a result for every input CURIE returned?
            if identifier not in result:
                # Sanity check: was a result for every input CURIE returned? Is this really needed?
                logger.warning(f"normalize_identifiers(curies): input curie '{identifier}' not in result")
                continue

            # Maybe nothing returned if the node identifier is unknown?
            if not result[identifier]:
                mappings[identifier] = None
            else:
                # Non-trivial result returned
                # retrieve the NN dictionary result
                # associated with the input node identifier
                entry = result[identifier]

                # (sanity check for required fields... We assume that they ought to always be present?)
                assert "id" in entry and "identifier" in entry["id"]
                mappings[identifier] = entry["id"]["identifier"]

        return mappings


    def normalize_node(
            self,
            node: NamedThing,
            gp_conflate: bool = True,
            dc_conflate: bool = False
    ) -> Optional[NamedThing]:
        """
        Calls the Node Normalizer ("NN") with the identifier of the given node
        and updates the node contents with NN resolved values for canonical identifier,
        node categories, and cross-references. Optional flags may be set to True to force
        either gene-protein or drug-chemical conflation. The node returned also can have
        a fully updated node description as returned by the Node Normalizer.567890-

        Known limitation: this method does NOT reset the node.category field values at this time.

        :param node: target instance of a class object in the NameThing hierarchy
        :param gp_conflate: bool, apply Gene-Protein conflation (default: True)
        :param dc_conflate:  bool, apply Drug-Chemical conflation (default: False)
        :return: rewritten node entry; None, if a node cannot be resolved in NN
        """

        assert node.id, "normalize_node(node): empty node identifier?"

        result = self.node_normalizer(
            query=self._query(
                curies=[node.id],
                description=True,
                gp_conflate=gp_conflate,
                dc_conflate=dc_conflate
            )
        )

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

        # (sanity check for required fields...
        #  We assume that they ought to always be present?)
        assert "id" in node_identity and "identifier" in node_identity["id"]
        canonical_identifier = node_identity["id"]["identifier"]
        canonical_name = node_identity["id"]["label"]
        canonical_description = node_identity["id"]["description"] if "description" in node_identity["id"] else ""

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
        node.description = canonical_description

        # ... TODO: (Re-)set the node categories
        #           (Pydantic doesn't allow the following statement ... yet?)
        # node.category = node_identity["type"]

        # return the normalized node
        return node

    def normalize_edge(
            self,
            edge: Association,
            gp_conflate: bool = True,
            dc_conflate: bool = False
    ) -> Optional[Association]:
        """
        Rewrites the Association subject and object identifiers with their Node Normalizer canonical identifiers.

        :param edge: target instance of a class object in the Association hierarchy
        :param gp_conflate: bool, apply Gene-Protein conflation (default: True)
        :param dc_conflate:  bool, apply Drug-Chemical conflation (default: False)

        :return: rewritten edge Association entry; None, if edge subject or object identifier
                 cannot be resolved by the Node Normalizer.
        """
        edge_subject = self.normalize_node(
            NamedThing(id=edge.subject, **{}),
            gp_conflate=gp_conflate,
            dc_conflate=dc_conflate
        )
        edge_object = self.normalize_node(
            NamedThing(id=edge.object, **{}),
            gp_conflate=gp_conflate,
            dc_conflate=dc_conflate
        )
        if not (edge_subject and edge_object):
            return None
        edge.subject = edge_subject.id
        edge.object = edge_object.id
        return edge
