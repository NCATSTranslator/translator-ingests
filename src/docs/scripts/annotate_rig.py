"""
This script 'surgically' annotates specified RIG fields in a specified RIG YAML file.
"""
from os import path, rename
import sys
import yaml
import json
import click
from loguru import logger

from translator_ingest import INGESTS_PARSER_PATH


def rewrite_property(rig_data, properties, values):
    """
    Recurse to modify the specified hierarchically specified property in the RIG data,
    with the specified values (or delete the property if values are 'None').
    Note that the software is currently agnostic about the RIG schema data model,
    so the onus of correct value formats is placed on the user of the tool!

    :param rig_data: RIG data file as a hierarchical Python data structure.
    :param properties: list[str], components of the sequential path to the value to be rewritten
    :param values: Python data object representing the property value to be rewritten. Delete the property if 'None'.
    :return: None (side effect is an updated RIG file)
    """
    # logger.debug(f"rig_data: {rig_data}")
    # logger.debug(f"properties: {properties}")
    # logger.debug(f"values: {str(values)}")
    if len(properties) == 1:
        # terminal leaf of properties: perform the required surgery here
        if values is not None:
            rig_data[properties[0]] = values
        else:
            # delete the property from the rig_data
            rig_data.pop(properties[0])

    else:
        # not yet at the end of the path... recurse down the path
        if isinstance(rig_data[properties[0]], list):
            # ...but remember to iterate through all
            # parallel paths if you have a list
            for i in range(len(rig_data[properties[0]])):
                rewrite_property(rig_data[properties[0]][i], properties[1:], values)
        else:
            rewrite_property(rig_data[properties[0]], properties[1:], values)
        # logger.debug(f"Revised rig_data: {rig_data}")


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
    '--tag',
    required=True,
    help='Dot-delimited target yaml property path for modification (e.g. target_info.edge_type_info.qualifiers)'
)
@click.option(
    '--value',
    default=None,
    help='Values to which to set the specified property, '+
         'specified as a JSON object expressed as a valid quote-escaped string '+
         '(default: None, deletes the property and its values)'
)
def main(ingest, rig, tag, value):
    """
    Enables homogeneous global revisions to a RIG target based on suitable data mappings.
    The tag path is not necessarily unique within the RIG file, in which case, the tool
    iterates through all parallel paths and modifies the values of all
    the equivalent terminal leaf tags in each path

    :param ingest: Target ingest folder name string of the target data source folder (e.g., icees)
    :param rig: Optional[str] target RIG file (default: <ingest folder name>_rig.yaml)
    :param tag: str Dot-delimited path specification to the YAML property value to be written out
    :param value: Optional[str], Values to which to set the specified property, '+
                   'expressed as a valid JSON object written as a (quote-escaped) string
                   (default: None, deletes the property and its values)'
    :return: None (side effect is an updated RIG file)

    Examples:

    \b
    annotate_rig.py \
        --ingest icees \
        --rig my_rig.yaml \
        --property "target_info.edge_type_info.qualifiers" \
        --values "[{\"property\": \"biolink:subject_feature_name\", \"value_range\": \"str\"},{\"property\": \"biolink:object_feature_name\", \"value_range\": \"str\"}]"
    """

    ingest_path = INGESTS_PARSER_PATH / ingest

    print(f"Ingest Data: {ingest_path}")

    # Default RIG file name, if not given
    if rig is None:
        rig = f"{ingest}_rig.yaml"

    rig_path = ingest_path / rig

    print(f"RIG: {rig_path}")

    # Check if the RIG file exists
    if not path.exists(rig_path):
        click.echo(f"Error: RIG yaml file not found: {rig_path}", err=True)
        sys.exit(1)

    # Parse out dot-delimited YAML property path to the value(s) to be revised
    properties = tag.split('.')

    if value is not None:
        # parse the revised string encoded JSON value giving
        # a complex hierarchy of Python lists and dictionaries
        values = json.loads(value)
    else:
        # empty values will trigger property deletion!
        if not click.confirm(f"Are you sure that you wish to delete the property '{tag}'?", default=False):
            sys.exit(0)
        # User confirmed deletion of the property, signalled by null values
        values = None

    try:
        rig_data: dict
        with open(rig_path, 'r') as r:
            rig_data = yaml.safe_load(r)
            rewrite_property(rig_data, properties,values)

        rename(rig_path, str(rig_path)+".original")

        with open(rig_path, 'w') as r:
            yaml.safe_dump(rig_data, r, sort_keys=False)

    except Exception as e:
        click.echo(f"Error revising properties in '{rig_path}':\n\t{e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
