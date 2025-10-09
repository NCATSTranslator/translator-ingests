#!/usr/bin/env python3
"""
Script to create a new Reference Ingest Guide (RIG) from the template.
"""

from os import makedirs, path, sep
import sys
from pathlib import Path
import yaml
from datetime import datetime
import click

RIGS_DIRECTORY = Path(__file__).parent.parent / "docs" / "rigs"

def load_template(template_path):
    """Load the RIG template from the YAML file."""
    with open(template_path, 'r') as f:
        return yaml.safe_load(f)


def create_rig(infores_id, rig_name, output_path, template_path):
    """
    Create a new RIG from the RIG template with user-specified values.

    :param infores_id: Associated with the primary knowledge source
    :param rig_name: name of the RIG
    :param output_path: full path to the RIG file
    :param template_path: full path to the RIG template file
    :return: None
    """
    # Load template
    template = load_template(template_path)
    
    # Update template with user values
    template['name'] = rig_name
    template['source_info']['infores_id'] = infores_id
    
    # Set the target infores_id based on the source (optional but commonly done)
    if 'target_info' not in template:
        template['target_info'] = {}
    # TODO: should this now be something like
    #       "infores:translator-ctd-kgx" instead of just "'"infores:ctd"?
    template['target_info']['infores_id'] = infores_id
    
    # Add the creation timestamp in additional_notes if not present
    if 'additional_notes' not in template['source_info']:
        template['source_info']['additional_notes'] = f"RIG created on {datetime.now().strftime('%Y-%m-%d')}"
    
    # Write the new RIG file
    with open(output_path, 'w') as f:
        yaml.dump(template, f, default_flow_style=False, sort_keys=False, indent=2)
    
    click.echo(f"Created new RIG: {output_path}")
    click.echo(f"  Name: {rig_name}")
    click.echo(f"  InfoRes ID: {infores_id}")
    click.echo(f"\nNext steps:")
    click.echo(f"1. Edit {output_path} to fill in the template sections")
    click.echo(f"2. See src{sep}docs{sep}files{sep}example-rigs.md for detailed guidance")


@click.command()
@click.option(
    '--infores', 
    required=True,
    help='InfoRes identifier for the data source (e.g., infores:ctd)'
)
@click.option(
    '--name',
    required=True, 
    help='Human-readable name for the RIG (e.g., "CTD Chemical-Disease Associations")'
)
@click.option(
    '--output',
    help='Output filename for the new RIG (default: based on infores ID)'
)
@click.option(
    '--template',
    default=f"src{sep}docs{sep}files{sep}rig_template.yaml",
    help=f"Path to the RIG template file (default: src{sep}docs{sep}files{sep}rig_template.yaml)"
)
def main(infores, name, output, template):
    """Create a new Reference Ingest Guide from the template.
    
    Examples:
    
    \b
    create_rig.py --infores infores:ctd --name "CTD Chemical-Disease Associations"
    create_rig.py --infores infores:pharmgkb --name "PharmGKB Drug-Gene Interactions" --output pharmgkb_rig.yaml
    """
    
    # Validate infores format
    if not infores.startswith('infores:'):
        click.echo("Error: InfoRes ID must start with 'infores:'", err=True)
        sys.exit(1)

    # Generate output filename if not provided
    if not output:
        # Extract a source file name from infores ID and create filename
        source_name = infores.replace('infores:', '').replace(':', '_')
        output = f"{source_name}_rig.yaml"

    # Sanity check: ensure the rigs directory exists
    makedirs(path.abspath(RIGS_DIRECTORY), exist_ok=True)
    output_path = f"{RIGS_DIRECTORY}{sep}{output}"

    # Check if template exists
    if not path.exists(template):
        click.echo(f"Error: Template file not found: {template}", err=True)
        sys.exit(1)
    
    # Check if the output file already exists
    if path.exists(output_path):
        if not click.confirm(f"File {output_path} already exists. Overwrite?"):
            click.echo("Aborted.")
            sys.exit(0)
    
    try:
        create_rig(infores, name, output_path, template)
    except Exception as e:
        click.echo(f"Error creating RIG: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()