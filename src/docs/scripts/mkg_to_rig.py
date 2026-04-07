"""
This script loads details from a Biolink Model-compliant
(TRAPI) Meta Knowledge Graph JSON file into
 a project knowledge graph description.

The script now provides two complementary output formats:
1. A human-readable spreadsheet table (i.e. "Translator Phase 2 Ingest Inventory" style).
2. Population of a Reference Ingest Guide ("RIG") YAML file 'target_info' section.
"""
from typing import Optional, Any
from os import path, rename
from pathlib import Path
import sys
import yaml
import json
import csv
import click

from biolink_model.datamodel.pydanticmodel_v2 import KnowledgeLevelEnum, AgentTypeEnum

from translator_ingest import INGESTS_PARSER_PATH

def read_mkg_nodes(nodes, node_info):
    """
    Convert the input MKG node data into an output list of nodes.

    :param nodes: Nodes from the given Meta Knowledge Graph JSON file.
    :param node_info: Parsed out node information for the output.
    :return: Modified node_info list.
    """
    for category, details in nodes.items():
        node_data = dict()

        #   - node_category: "biolink:Disease"
        node_data['node_category'] = category

        #     source_identifier_types:
        #       - "OMIM" etc.
        id_prefixes: list[str] = details['id_prefixes']
        node_data['source_identifier_types'] = id_prefixes.copy()

        #     node_properties:
        #     - "biolink:inheritance"
        node_data['node_properties'] = []
        attributes = details.get('attributes',[])
        for attribute in attributes:
            attribute_type_id = attribute['attribute_type_id']
            node_data['node_properties'].append(attribute_type_id)

            # TODO: unsure if or how to really record this at the moment,
            #       let alone, other associated properties?
            # original_attribute_names = attribute['original_attribute_names']

        node_info.append(node_data)


class EdgeData:
    """
    This class is a container for processed RIG edge data, organized by 'edge type'.
    At any point in time, instances of this class will be in a mode to capture one 'edge type.'
    An 'edge type' is defined by a distinct combination of qualifiers and/or attributes,
    which are used to construct a unique index key string label.
    One default index key string label is 'global' which simply targets the edge type
    that does not have any discriminating qualifiers or attributes.
    """
    def __init__(self, knowledge_level: str, agent_type: str):
        """
        Initializes data structures to capture processed meta knowledge graph edge data.
        Initializes the initial edge type to 'global'.
        """
        self.current_edge_type_id = 'global'
        self.edge_data: dict[str, dict[str, Any]] = {'global': dict()}
        self.knowledge_level = knowledge_level
        self.agent_type = agent_type

    def set_current_id(self, identifier: str):
        self.current_edge_type_id = identifier

    def get_current_id(self) -> str:
        return self.current_edge_type_id

    def add_identifier(self, identifier: str):
        """
        Appends an identifier to the current edge identifier, then moves
        any current edge data under that revised current identifier. As a side effect,
        the edge_data dictionary is created for empty or current identifiers without data.
        """
        current_id = self.get_current_id()
        if current_id and current_id in self.edge_data:
            current_data = self.edge_data.pop(current_id)
        else:
            current_data = dict()
        # appends the new identifier to the current identifier string
        current_id = f"{current_id},{identifier}" if current_id else identifier
        # re-adds the current edge data under the new identifier string
        self.edge_data[current_id] = current_data
        self.set_current_id(current_id)

    def get_current_edge_data(self) -> dict[str, Any]:
        current_id = self.get_current_id()
        if current_id not in self.edge_data:
            self.edge_data[current_id] = dict()
        return self.edge_data[current_id]

    def add_value(self, key: str, value: str):
        edge_data = self.get_current_edge_data()
        if key not in edge_data:
            edge_data[key] = set()
        else:
            assert isinstance(edge_data[key], set)
        edge_data[key].add(value)

    def process_qualifiers(self, edge: dict[str, Any]):
        """
        Capture any qualifiers and set the current edge type identifier.
        If no qualifiers are present, the edge type is set to 'global'.
        """
        #
        # qualifiers: # (optional, multivalued, range = Qualifier)
        #   - property:  # (required, range = URIorCURIE)
        #   # Choose one (or more) of the 'value' slots to describe the type of value hold by the qualifier
        #     value_range: # (optional, multivalued, range = URIorCURIE)
        #       -
        #     value_enumeration: # (optional, multivalued, range = string)
        #       -
        #     value_id_prefixes: # (optional, multivalued, range = string)
        #       -
        #     value_description:  # (optional, range = string)
        #
        qualifiers = edge.get('qualifiers', [])
        if not qualifiers:
            # No qualifiers are observed on this edge,
            # thus no further processing is needed,
            # plus the current id is set to 'global'
            self.set_current_id('global')
            return

        # Otherwise, any encountered qualifiers and discriminating attributes are used
        # to construct and reset the EdgeData instance to a unique current identifier

        # recreating the current identifier here, thus starting with an empty string
        self.set_current_id("")

        # collect the list of qualifiers for this edge type
        qualifier_list: list[dict[str, Any]] = []
        for qualifier in qualifiers:
            #
            # Sample meta_knowledge_graph.json edge qualifier data:

            # "qualifiers":{
            #   "qualifier_type_id": "object_aspect_qualifier",
            #   "applicable_values": [
            #     "transport"
            #   ]
            # }
            qualifier_type_id = qualifier["qualifier_type_id"]
            applicable_values = qualifier["applicable_values"]

            self.add_identifier(f"{str(qualifier_type_id)}={str(applicable_values)}")

            rig_qualifier = dict()
            rig_qualifier["property"] = qualifier_type_id
            rig_qualifier["value_enumeration"] = applicable_values
            qualifier_list.append(rig_qualifier)

        # Capture the qualifiers for this edge type, setting up
        # edge_data dictionary, if not already created
        current_id = self.get_current_id()

        # Sanity check: create the qualifier-specified
        # edge_data dictionary if not already created
        if current_id not in self.edge_data:
            self.edge_data[current_id] = dict()

        if 'qualifiers' not in self.edge_data[current_id]:
            # Record the qualifiers for this edge type
            self.edge_data[current_id]['qualifiers'] = qualifier_list

    def sets_to_lists(self) -> list[dict[str, Any]]:
        # Each edge_data dictionary entry is an indexed collection
        # of SPOQ values plus edge properties for one edge type.
        converted: list[dict[str, Any]] = []
        for entry in self.edge_data.values():
            # Converts all dictionary set() values to list() values
            for key, value in entry.items():
                if isinstance(value, set):
                    entry[key] = list(value)
                else:
                    entry[key] = value

            # enforce expected ordering of the entry fields
            ordered_entry: dict[str,Any] = dict()
            for key in [
                'subject', 'predicates', 'object', 'qualifiers',
                'knowledge_level', 'agent_type', 'edge_properties'
            ]:
                if key in entry:
                    ordered_entry[key] = entry[key]

            converted.append(ordered_entry)

        return converted

    def add_edge(self, edge: dict[str, Any]):
        #       subject_categories:
        #       - "biolink:Disease"
        self.add_value('subject', edge['subject'])

        #       predicates:
        #         - "biolink:has_phenotype"
        self.add_value('predicates', edge['predicate'])

        #       object_categories:
        #       - "biolink:PhenotypicFeature"
        self.add_value('object', edge['object'])

        #       knowledge_level:
        #       - knowledge_assertion
        self.add_value('knowledge_level', self.knowledge_level)

        #       agent_type:
        #       - manual_agent
        self.add_value('agent_type', self.agent_type)

        # Collect the list of attributes (ignoring knowledge_level and agent type)
        # that discriminate for this edge type if any are encountered
        attributes = edge.get('attributes', [])
        for attribute in attributes:
            if attribute['attribute_type_id'] in ["biolink:knowledge_level","biolink:agent_type"]:
                continue  # these are now dedicated RIG fields, not 'edge_properties'
            else:
                attribute_type_id = attribute['attribute_type_id']
                # add the discriminating attribute type id to the current identifier string
                self.add_value(key='edge_properties', value=attribute_type_id)


def read_mkg_edges(
        edges,
        edge_info,
        knowledge_level: str,
        agent_type: str
):
    """
    Convert the input MKG edge data into an output list of edges.

    :param edges: Edges from the given Meta Knowledge Graph JSON file.
    :param edge_info: Parsed out edge information for the output.
    :param knowledge_level: Knowledge level for the edge.
    :param agent_type: Agent type for the edge.
    :return: Modified edge_info list.
    """
    # Edge data partitions edges by qualifiers and/or attributes;
    # 'global' if no qualifiers or discriminating attributes
    edge_data = EdgeData(knowledge_level=knowledge_level, agent_type=agent_type)

    for edge in edges:

        # Edge qualifiers and attributes discriminate an association type.
        # Along the way, a unique edge_type_info entry identifier is computed,
        # then set as current internally in the edge_data instance...
        edge_data.process_qualifiers(edge)

        # ...Edge data is now set in data structures
        # tagged by the edge_type_info identifier.
        edge_data.add_edge(edge)

    edge_info.extend(edge_data.sets_to_lists())


CSV_TABLE_HEADERS:list[str] = [
    "Columns / Fields Used",
    "MetaEdge Subject Category",
    "MetaEdge Predicate",
    "MetaEdge Object Category",
    "MetaEdge Qualifiers",
    "KL",
    "AT",
    "Other Edge Attributes",
    "Subject Identifier Prefixes",
    "Subject Node Properties",
    "Object Identifier Prefixes",
    "Object Node Properties"
]

COLUMNS_FIELDS_USED = "'- subject, \n- predicate, \n- object, \n- attributes"

def flatten(data:list[str], rewrite) -> str:
    if data:
        output = "'- "
        output += "\n- ".join(
            [rewrite(entry) for entry in data]
        )
    else:
        output = ""
    return output

def flatten_field(field_name: str, edge: dict, rewrite) -> str:
    field_value = edge.get(field_name, [])
    return flatten(field_value, rewrite)

def flatten_biolink(field_name: str, edge: dict) -> str:
    return flatten_field(field_name, edge, rewrite=lambda x: x.replace("biolink:", ""))

def flatten_qualifiers(edge: dict) -> str:
    return flatten_field('qualifiers', edge, rewrite=lambda x: x['property'])

def flatten_values(field_name: str, data: dict) -> str:
    return flatten_field(field_name, data, rewrite=lambda x: x)

def flatten_id_prefixes(node: dict) -> str:
    return flatten_field('id_prefixes', node, rewrite=lambda x: x)

def flatten_node_properties(node: dict) -> str:
    return flatten_field('node_properties', node, rewrite=lambda x: x)

def process_nodes(field_name: str, edge: dict, kg_nodes: dict) -> tuple[str, str]:
    # 'field_name' is one of 'subject' or 'object';
    # absent data will trigger a RuntimeError
    categories: Optional[list[str]] = edge.get(field_name)
    if not categories:
        raise RuntimeError(f"Edge Missing '{field_name}' categories")

    # else: categories is a list of node categories
    # First, remove the biolink: prefix from each category
    category_list: list[str] = [x.replace("biolink:", "") for x in categories]

    # using sets to remove duplicates, for flattening to a string
    id_prefixes: set = set()
    node_properties: set = set()
    for category in category_list:
        node_info = kg_nodes.get(category)
        if not node_info:
            raise RuntimeError(f"MKG missing node category '{category}' used in edge")
        id_prefixes.update(node_info['id_prefixes'])
        node_properties.update(node_info['node_properties'])

    return flatten(list(id_prefixes), rewrite=lambda x: x), flatten(list(node_properties), rewrite=lambda x: x)

def prepare_table_data(node_info, edge_info) -> list[dict]:
    """
    Prepare data for use in a Translator Phase 2 Ingest Inventory style spreadsheet.
    :param node_info: List of node information.
    :param edge_info: List of edge information.
    :return: A list[dict] of merged, flattened, and renamed
             node and edge information, one dictionary per edge, per list row.
    """
    kg_nodes: dict = dict()
    for node in node_info:
        node_category = node['node_category'].replace("biolink:","")
        kg_nodes[node_category] = {
            "id_prefixes": node.get('source_identifier_types', []),
            "node_properties": node.get('node_properties', [])
        }

    kg_data: list[dict] = list()
    for edge in edge_info:
        try:
            subject_categories = flatten_biolink('subject', edge)
            subject_id_prefixes, subject_node_properties = process_nodes('subject', edge, kg_nodes)

            predicates = flatten_biolink('predicates', edge)

            object_categories = flatten_biolink('object', edge)
            object_id_prefixes, object_node_properties = process_nodes('object', edge, kg_nodes)

            qualifiers = flatten_qualifiers(edge)

            # Note: edge properties keep the biolink: prefix
            edge_properties = flatten_values('edge_properties', edge)

            knowledge_level = flatten_values('knowledge_level', edge)
            agent_type = flatten_values('agent_type', edge)

        except (RuntimeError, KeyError):
            continue  # just ignore faulty or missing data

        kg_data.append(
            {
                "Columns / Fields Used": COLUMNS_FIELDS_USED,
                "MetaEdge Subject Category": subject_categories,
                "MetaEdge Predicate": predicates,
                "MetaEdge Object Category": object_categories,
                "MetaEdge Qualifiers": qualifiers,
                "KL": knowledge_level,
                "AT": agent_type,
                "Other Edge Attributes": edge_properties,
                "Subject Identifier Prefixes": subject_id_prefixes,
                "Subject Node Properties": subject_node_properties,
                "Object Identifier Prefixes": object_id_prefixes,
                "Object Node Properties": object_node_properties
            }
        )
    return kg_data


def prune_empty(x):
    if isinstance(x, dict):
        return {k: prune_empty(v) for k, v in x.items()
                if v not in (None, "", [], {}) and prune_empty(v) not in (None, "", [], {})}
    if isinstance(x, list):
        return [prune_empty(v) for v in x
                if v not in (None, "", [], {}) and prune_empty(v) not in (None, "", [], {})]
    return x


@click.command()
@click.option(
    '--ingest', '-i',
    required=True,
    help='Target ingest folder name of the target data source folder (e.g., icees)'
)
@click.option(
    '--rig', '-r',
    default=None,
    help='Target RIG file (default: <ingest folder name>_rig.yaml)'
)
@click.option(
    '--mkg', '-m',
    default='meta_knowledge_graph.json',
    help='Meta Knowledge Graph JSON file source of details to be loaded into the RIG ' +
         '(default: "meta_knowledge_graph.json",  assumed co-located with RIG in the ingest folder)'
)
@click.option(
    '--knowledge_level', '-k',
    default='not_provided',
    help='Biolink Edge Knowledge Level (default: "not_provided")'
)
@click.option(
    '--agent_type', '-a',
    default='not_provided',
    help='Biolink Edge Agent Type (default: "not_provided")'
)
@click.option(
    '--output', '-o',
    default='rig',
    help='Desired format of the output, i.e., "rig" or "csv" (default: "rig")'
)
def main(
        ingest: str,
        rig: str,
        mkg: str,
        knowledge_level: str,
        agent_type: str,
        output: str
):
    """
    Either populate the 'target_info' section of a given RIG YAML file or
    create a comparable CSV formatted edge inventory file, using node and
    edge information from a (TRAPI-generated) Meta Knowledge Graph JSON file.

    :param ingest: str, Target ingest folder name of the target data source folder (e.g., icees)
    :param rig: Reference-Ingest Guide ("RIG") file (default: <ingest folder name>_rig.yaml);
            This switch is ignored if the output format is "table".
    :param mkg: Meta Knowledge Graph JSON file name source of details to be
                loaded into the RIG (assumed co-located with RIG in the ingest task folder)
    :param knowledge_level: Biolink Model compliant edge knowledge level specification
    :param agent_type: Biolink edge agent type specification
    :param output: Desired format of the output, i.e., "rig" or "csv" (default: "rig")
    :return: side effect is either a revised RIG file or a new CSV formatted edge inventory file.

    Examples:

    \b
    The minimum usage is to simply provide the target ingest folder name.
    mk_to_rig.py --ingest icees

    # But default values for RIG creation can be overwritten
    mk_to_rig.py --ingest icees --mkg my_meta_graph.json --rig my_rig.yaml

    # If a table of edges is preferred, the --output switch option can be used.
    mk_to_rig.py --ingest icees --output csv
    """
    if output not in ['rig', 'csv']:
        click.echo(
            message=f"Error: Invalid output format: {output}",
            err=True
        )
        sys.exit(1)

    # Biolink data quality assurance sanity checks
    if knowledge_level not in KnowledgeLevelEnum:
        click.echo(
            message=f"Error: Invalid Biolink Knowledge Level: {knowledge_level}",
            err=True
        )
        sys.exit(1)

    if agent_type not in AgentTypeEnum:
        click.echo(
            message=f"Error: Invalid Biolink Agent Type: {agent_type}",
        )
        sys.exit(1)

    ingest_path = INGESTS_PARSER_PATH / ingest
    print(f"Ingest Data: {ingest_path}")

    mkg_path = ingest_path / mkg
    print(f"Metadata: {mkg_path}")

    try:
        kg_data_path: Optional[Path]
        if output == 'rig':

            # Default RIG file name, if not given
            if rig is None:
                rig = f"{ingest}_rig.yaml"

            kg_data_path = ingest_path / rig

            print(f"RIG: {kg_data_path}")

            # Check if the RIG file exists
            if not path.exists(kg_data_path):
                click.echo(
                    message=f"Error: RIG yaml file not found: {kg_data_path}",
                    err=True
                )
                sys.exit(1)

            with open(kg_data_path, 'r') as rig:
                kg_data = yaml.safe_load(rig)

            # conservative, in case target_info is already present
            target_info = kg_data.setdefault('target_info', {})

            # We accept that all pre-existing node and edge data will be overwritten.
            # We mitigate information loss by saving a copy of the original file later.
            node_info = target_info['node_type_info'] = []
            edge_info = target_info['edge_type_info'] = []
        else:
            # Assume a 'table' datafile but this will
            # only be used for writing the output
            kg_data_path = ingest_path / f"{ingest}_table.csv"
            node_info = []
            edge_info = []

        # Check if the meta-knowledge graph file exists
        if not path.exists(mkg_path):
            click.echo(
                message=f"Error: Meta Knowledge Graph json file not found: {mkg_path}",
                err=True
            )
            sys.exit(1)

        with open(mkg_path, 'r') as mkg:
            mkg_data = json.load(mkg)
            read_mkg_nodes(mkg_data['nodes'], node_info)
            read_mkg_edges(mkg_data['edges'], edge_info, knowledge_level, agent_type)

        if output == 'rig':
            rename(kg_data_path, str(kg_data_path)+".original")
            with open(kg_data_path, 'w') as rig:
                cleaned = prune_empty(kg_data)
                yaml.safe_dump(cleaned, rig, sort_keys=False)
        else:
            # Generate 'csv' output file
            kg_data = prepare_table_data(node_info, edge_info)
            with open(kg_data_path, mode="w", newline="", encoding="utf-8") as table_file:
                writer = csv.DictWriter(table_file, fieldnames=CSV_TABLE_HEADERS, quoting=0) # quoting=csv.QUOTE_ALL
                writer.writeheader()
                writer.writerows(kg_data)

    except Exception as e:
        click.echo(f"Error processing Meta Knowledge Graph JSON data: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
