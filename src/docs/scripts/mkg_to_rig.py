"""
This script loads details from a Biolink Model-compliant
(TRAPI) Meta Knowledge Graph JSON file into
 a project knowledge graph description.

The script now provides two complementary output formats:
1. A human-readable spreadsheet table (i.e. "Translator Phase 2 Ingest Inventory" style).
2. Population of a Reference Ingest Guide ("RIG") YAML file 'target_info' section.
"""
from typing import Optional
from os import path, rename
from pathlib import Path
import sys
import yaml
import json
import csv
import click
from translator_ingest import INGESTS_PARSER_PATH


def read_mkg_nodes(nodes,node_info):
    """Read the Meta Knowledge Graph nodes from the given JSON file handle."""
    for category, details in nodes.items():

        # 'target_info.node_type_info' is a list of rig_node entries
        rig_node = dict()

        #   - node_category: "biolink:Disease"
        rig_node['node_category'] = category

        #     source_identifier_types:
        #       - "OMIM" etc.
        id_prefixes: list[str] = details['id_prefixes']
        rig_node['source_identifier_types'] = id_prefixes.copy()

        #     node_properties:
        #     - "biolink:inheritance"
        rig_node['node_properties'] = []
        attributes = details['attributes']
        for attribute in attributes:
            attribute_type_id = attribute['attribute_type_id']
            rig_node['node_properties'].append(attribute_type_id)

            # TODO: unsure if or how to really record this at the moment,
            #       let alone, other associated properties?
            # original_attribute_names = attribute['original_attribute_names']

        node_info.append(rig_node)

def read_mkg_edges(
        edges,
        knowledge_level,
        agent_type,
        edge_info
):
    """Read the Meta Knowledge Graph edges from the given JSON file."""
    for edge in edges:
        rig_edge = dict()

        #       subject_categories:
        #       - "biolink:Disease"
        rig_edge['subject'] = [edge['subject']]

        #       predicates:
        #         - "biolink:has_phenotype"
        rig_edge['predicates'] = [edge['predicate']]

        #       object_categories:
        #       - "biolink:PhenotypicFeature"
        rig_edge['object'] = [edge['object']]

        # TODO: rig_edge['qualifiers']

        #       knowledge_level:
        #       - knowledge_assertion
        rig_edge['knowledge_level'] = knowledge_level

        #       agent_type:
        #       - manual_agent
        rig_edge['agent_type'] = agent_type

        rig_edge['edge_properties'] = []
        attributes = details['attributes']
        for attribute in attributes:
            attribute_type_id = attribute['attribute_type_id']
            rig_edge['edge_properties'].append(attribute_type_id)

            # TODO: unsure if or how to really record this at the moment,
            #       let alone, other associated properties?
            # original_attribute_names = attribute['original_attribute_names']

        edge_info.append(rig_edge)


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
    '--output',
    default='rig',
    help='Desired format of the output, i.e., rig or table (default: "rig")'
)
def main(ingest, mkg, rig, knowledge_level, agent_type, output):
    """
    Merge Meta Knowledge Graph node and edge information into RIG 'target_info'.

    :param ingest: Target ingest folder name of the target data source folder (e.g., icees)
    :param mkg: Meta Knowledge Graph JSON file source of details to be
                loaded into the RIG (assumed co-located with RIG in the ingest task folder
    :param rig: Target Reference Ingest Guide ("RIG") file (default: <ingest folder name>_rig.yaml)
    :param knowledge_level: Biolink Model compliant edge knowledge level specification
    :param agent_type: Biolink edge agent type specification
    :param output: Desired format of the output, i.e., rig or table (default: "rig")
    :return:

    Examples:

    \b
    The minimum usage is to simply provide the target ingest folder name.
    mk_to_rig.py --ingest icees

    # But default values for RIG creation can be overwritten
    mk_to_rig.py --ingest icees --mkg my_meta_graph.json --rig my_rig.yaml

    # If a table of edges is preferred, the --format option can be used.
    mk_to_rig.py --ingest icees --format table
    """
    if output not in ['rig', 'table']:
        click.echo(
            message=f"Error: Invalid output format: {output}",
            err=True
        )
        sys.exit(1)

    ingest_path = INGESTS_PARSER_PATH / ingest
    print(f"Ingest Data: {ingest_path}")

    mkg_path = ingest_path / mkg
    print(f"Metadata: {mkg_path}")

    try:
        kg_data_path: Optional[Path]
        kg_data: dict
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
                yaml.safe_dump(kg_data, rig, sort_keys=False)
        else:
            # Assume 'table' model output

            # data = [
            #     {"name": "Alice", "age": 30, "city": "Sooke"},
            #     {"name": "Bob", "age": 25, "city": "Victoria"},
            #     {"name": "Charlie", "age": 35, "city": "Vancouver"}
            # ]
            # If the data has inconsistent keys,
            # consider using
            #      set().union(*[d.keys() for d in data])
            # to build a complete header list.

            with open(kg_data_path, mode="w", newline="", encoding="utf-8") as table_file:
                writer = csv.DictWriter(table_file, fieldnames=kg_data[0].keys())
                writer.writeheader()
                writer.writerows(kg_data)

    except Exception as e:
        click.echo(f"Error processing Meta Knowledge Graph JSON data: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
