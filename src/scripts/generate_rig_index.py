#!/usr/bin/env python3
"""
Generate RIG index table from YAML files.
"""

import os
import sys
import yaml
import click
from pathlib import Path
from jinja2 import Environment, FileSystemLoader


@click.command()
@click.option('--rig-dir', default='src/docs/rigs', help='Directory containing RIG YAML files')
@click.option('--template-dir', default='src/docs/doc-templates', help='Directory containing Jinja templates')
@click.option('--input-file', default='src/docs/files/rig_index.md', help='Input markdown template file')
@click.option('--output-file', default='docs/rig_index.md', help='Output markdown file')
def main(rig_dir, template_dir, input_file, output_file):
    """Generate RIG index by injecting table into existing markdown file."""
    
    rig_path = Path(rig_dir)
    template_path = Path(template_dir)
    input_path = Path(input_file)
    output_path = Path(output_file)
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Load Jinja environment
    env = Environment(loader=FileSystemLoader(template_path))
    table_template = env.get_template('rig_table.md.jinja2')
    
    # Collect RIG data
    rigs = []
    yaml_files = list(rig_path.glob('*.yaml'))
    
    for yaml_file in yaml_files:
        if yaml_file.name == 'test_rig.yaml':
            continue  # Skip test file
            
        try:
            with open(yaml_file, 'r') as f:
                rig_data = yaml.safe_load(f)
            
            # Extract key information
            rig_info = {
                'filename': yaml_file.stem,
                'name': rig_data.get('name', yaml_file.stem),
                'infores_id': rig_data.get('source_info', {}).get('infores_id', 'Unknown')
            }
            
            rigs.append(rig_info)
            
        except Exception as e:
            click.echo(f"Error reading {yaml_file}: {e}", err=True)
            continue
    
    # Sort RIGs by InfoRes ID
    rigs.sort(key=lambda x: x['infores_id'])
    
    # Generate table content
    table_content = table_template.render(rigs=rigs)
    
    # Read the input markdown file
    with open(input_path, 'r') as f:
        base_content = f.read()
    
    # Replace the table placeholder with generated content
    output_content = base_content.replace(
        '<!-- RIG_TABLE_START -->\n<!-- This table is automatically generated during documentation build -->\n<!-- RIG_TABLE_END -->',
        f'<!-- RIG_TABLE_START -->\n<!-- This table is automatically generated during documentation build -->\n{table_content}\n<!-- RIG_TABLE_END -->'
    )
    
    # Write output file
    with open(output_path, 'w') as f:
        f.write(output_content)
    
    click.echo(f"Generated RIG index: {output_path}")
    click.echo(f"Found {len(rigs)} RIG files")


if __name__ == '__main__':
    main()