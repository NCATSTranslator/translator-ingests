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


def process_qualifiers(edge: dict[str, Any], edge_data: dict[str, Any]) -> Optional[str]:
    #         qualifiers: # (optional, multivalued, range = Qualifier)
    #           - property:  # (required, range = URIorCURIE)
    #           # Choose one (or more) of the 'value' slots to describe the type of value hold by the qualifier
    #             value_range: # (optional, multivalued, range = URIorCURIE)
    #               -
    #             value_enumeration: # (optional, multivalued, range = string)
    #               -
    #             value_id_prefixes: # (optional, multivalued, range = string)
    #               -
    #             value_description:  # (optional, range = string)
    qualifiers = edge.get('qualifiers', None)
    if not qualifiers:
        return None

    if 'qualifiers' not in edge_data:
        edge_data['qualifiers'] = []

    for qualifier in qualifiers:
        # {
        #   "qualifier_type_id": "object_aspect_qualifier",
        #   "applicable_values": [
        #     "transport"
        #   ]
        # }
        rig_qualifier = dict()
        rig_qualifier["property"] = qualifier["qualifier_type_id"]

        # it is guess anything else here except a "value_enumeration"
        rig_qualifier["value_enumeration"] = qualifier["qualifier_type_id"]

        edge_data['qualifiers'].extend(qualifier)


def _sets_to_lists(edge_data: dict[str, Any]) -> dict[str, Any]:
    # Converts all dictionary set() values to list() values
    converted: dict[str, Any] = {}
    for key, value in edge_data.items():
        if isinstance(value, set):
            converted[key] = list(value)
        else:
            converted[key] = value
    return converted

def read_mkg_edges(
        edges,
        edge_info,
        knowledge_level: str,
        agent_type: str,
        merge_edges
):
    """
    Convert the input MKG edge data into an output list of edges.

    :param edges: Edges from the given Meta Knowledge Graph JSON file.
    :param edge_info: Parsed out edge information for the output.
    :param knowledge_level: Knowledge level for the edge.
    :param agent_type: Agent type for the edge.
    :param merge_edges: Merge edge_type_info specified edges metadata into one definition.
    :return: Modified edge_info list.
    """
    edge_data: Optional[dict[str, Any]] = None

    for edge in edges:

        if not edge_data:
            edge_data = dict()

        #       subject_categories:
        #       - "biolink:Disease"
        if 'subject' not in edge_data:
            edge_data['subject'] = set()
        edge_data['subject'].add(edge['subject'])

        #       predicates:
        #         - "biolink:has_phenotype"
        if 'predicates' not in edge_data:
            edge_data['predicates'] = set()
        edge_data['predicates'].add(edge['predicate'])

        #       object_categories:
        #       - "biolink:PhenotypicFeature"
        if 'object' not in edge_data:
            edge_data['object'] = set()
        edge_data['object'].add(edge['object'])

        # qualifiers are weirdly discriminating of edge type,
        # somewhat hard to resolve...
        # TODO: generate an indexing key using process qualifiers
        #       then use it to discriminate between edge types?
        process_qualifiers(edge, edge_data)

        #       knowledge_level:
        #       - knowledge_assertion
        if 'knowledge_level' not in edge_data:
            edge_data['knowledge_level'] = knowledge_level

        #       agent_type:
        #       - manual_agent
        if 'agent_type' not in edge_data:
            edge_data['agent_type'] = agent_type

        if 'edge_properties' not in edge_data:
            edge_data['edge_properties'] = set()

        attributes = edge.get('attributes', [])
        for attribute in attributes:
            if attribute['attribute_type_id'] in ["biolink:knowledge_level","biolink:agent_type"]:
                continue  # these are dedicated RIG fields, not edge_properties
            else:
                attribute_type_id = attribute['attribute_type_id']
                edge_data['edge_properties'].add(attribute_type_id)

                # TODO: unsure if or how to really record this at the moment,
                #       let alone, other associated properties?
                # original_attribute_names = attribute['original_attribute_names']

        if not merge_edges:
            # Publish each edge separately
            edge_info.append(_sets_to_lists(edge_data))
            edge_data = None

    if merge_edges and edge_data is not None:
        # merge the edge data once
        edge_info.append(_sets_to_lists(edge_data))

CSV_TABLE_HEADERS:list[str] = [
    "MetaEdge Subject Category",
    "MetaEdge Predicate",
    "MetaEdge Object Category",
    "MetaEdge Qualifiers",
    "KL",
    "AT",
    "Other Edge Attributes",
    "Subject Node Properties",
    "Subject Identifier Prefixes",
    "Object Node Properties",
    "Object Identifier Prefixes"
]
def prepare_table_data(node_info, edge_info) -> list[dict]:
    """
    Prepare data for use in a Translator Phase 2 Ingest Inventory style spreadsheet.
    :param node_info: List of node information.
    :param edge_info: List of edge information.
    :return: A list[dict] of merged, flattened and renamed
             node and edge information, one dictionary per edge, per list row.
    """
    kg_nodes: dict = dict()
    for node in node_info:
        node_category = node['node_category'].replace("biolink:","")
        id_prefixes = node.get('source_identifier_types', [])
        node_properties = node.get('node_properties', [])

        kg_nodes[node_category] = {
            "id_prefixes": ",".join(id_prefixes),
            "node_properties": ",".join(node_properties)
        }

    kg_data: list[dict] = list()
    for edge in edge_info:
        try:
            subject_category = edge['subject'][0]
            subject_category = subject_category.replace("biolink:", "")
            subject_metadata: Optional[dict] = kg_nodes.get(subject_category, None)
            object_category = edge['object'][0]
            object_category = object_category.replace("biolink:", "")
            object_metadata: Optional[dict] = kg_nodes.get(object_category, None)
            # Sanity check: skip edges with missing subject or object nodes
            if subject_metadata is None or object_metadata is None:
                continue
            predicate = edge['predicates'][0].replace("biolink:","")
            qualifiers = edge.get('qualifiers',[])
            qualifiers = ",".join(qualifiers) if qualifiers else ""
            edge_properties = edge.get('edge_properties',[])
            edge_properties = ",".join(edge_properties) if edge_properties else ""
        except Exception:
            continue  # just ignore faulty or missing data

        kg_data.append(
            {
                "MetaEdge Subject Category": subject_category,
                "MetaEdge Predicate": predicate,
                "MetaEdge Object Category": object_category,
                "MetaEdge Qualifiers": qualifiers,
                "KL": edge['knowledge_level'],
                "AT": edge['agent_type'],
                "Other Edge Attributes": edge_properties,
                "Subject Node Properties": subject_metadata['node_properties'],
                "Subject Identifier Prefixes": subject_metadata['id_prefixes'],
                "Object Node Properties": object_metadata['node_properties'],
                "Object Identifier Prefixes": object_metadata['id_prefixes']
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
    '--ingest',
    required=True,
    help='Target ingest folder name of the target data source folder (e.g., icees)'
)
@click.option(
    '--rig',
    default=None,
    help='Target RIG file (default: <ingest folder name>_rig.yaml)'
)
@click.option(
    '--mkg',
    default='meta_knowledge_graph.json',
    help='Meta Knowledge Graph JSON file source of details to be loaded into the RIG ' +
         '(default: "meta_knowledge_graph.json",  assumed co-located with RIG in the ingest folder)'
)
@click.option(
    '--knowledge_level',
    default='not_provided',
    help='Biolink Edge Knowledge Level (default: "not_provided")'
)
@click.option(
    '--agent_type',
    default='not_provided',
    help='Biolink Edge Agent Type (default: "not_provided")'
)
@click.option(
    '--merge_edges',
    default=True,
    help='Merge edge_type_info specified edges metadata into one definition (default: True)'
)
@click.option(
    '--output',
    default='rig',
    help='Desired format of the output, i.e., "rig" or "csv" (default: "rig")'
)
def main(
        ingest: str,
        rig: str,
        mkg: str,
        knowledge_level: str,
        agent_type: str,
        merge_edges: bool,
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
    :param merge_edges: boolean flag triggering merging "edge_type_info" specified edges metadata into one definition
                        (default: True)  This supports the RIG convention that allows for multiple predicates
                        and S/O categories as long as the provenance of all these edges is the same
                        (same source file, KL/AT, and general curation or generation method by the source)
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
            read_mkg_edges(mkg_data['edges'], edge_info, knowledge_level, agent_type, merge_edges)

        if output == 'rig':
            rename(kg_data_path, str(kg_data_path)+".original")
            with open(kg_data_path, 'w') as rig:
                cleaned = prune_empty(kg_data)
                yaml.safe_dump(cleaned, rig, sort_keys=False)
        else:
            # Generate 'csv' output file
            kg_data = prepare_table_data(node_info, edge_info)
            with open(kg_data_path, mode="w", newline="", encoding="utf-8") as table_file:
                writer = csv.DictWriter(table_file, fieldnames=CSV_TABLE_HEADERS)
                writer.writeheader()
                writer.writerows(kg_data)

    except Exception as e:
        click.echo(f"Error processing Meta Knowledge Graph JSON data: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
