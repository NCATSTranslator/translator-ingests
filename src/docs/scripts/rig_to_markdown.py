#!/usr/bin/env python3
"""
Convert RIG YAML files to markdown format.
"""

import os
import sys
import yaml
import click
from pathlib import Path


def format_list_items(items, indent=""):
    """Format a list of items as markdown."""
    if not items:
        return ""
    if isinstance(items, str):
        return items
    return "\n".join([f"{indent}- {item}" for item in items])


def format_table_items(items, headers):
    """Format items as a markdown table."""
    if not items:
        return ""
    
    # Create table header
    table = f"| {' | '.join(headers)} |\n"
    table += f"| {' | '.join(['---'] * len(headers))} |\n"
    
    # Add table rows
    for item in items:
        row_data = []
        for header in headers:
            # Convert header to field name (lowercase, replace spaces with underscores)
            field_name = header.lower().replace(" ", "_")
            value = item.get(field_name, "")
            if isinstance(value, list):
                value = ", ".join(str(v) for v in value)
            row_data.append(str(value))
        table += f"| {' | '.join(row_data)} |\n"
    
    return table


def yaml_to_markdown(rig_data, rig_name):
    """Convert RIG YAML data to markdown format."""
    
    markdown = f"# {rig_data.get('name', rig_name)}\n\n"
    
    # Source Information
    if 'source_info' in rig_data:
        source = rig_data['source_info']
        markdown += "## Source Information\n\n"
        
        if 'infores_id' in source:
            markdown += f"**InfoRes ID:** {source['infores_id']}\n\n"
        
        if 'description' in source:
            markdown += f"**Description:** {source['description']}\n\n"
        
        if 'citations' in source and source['citations']:
            markdown += "**Citations:**\n"
            markdown += format_list_items(source['citations']) + "\n\n"
        
        if 'terms_of_use' in source:
            markdown += f"**Terms of Use:** {source['terms_of_use']}\n\n"
        
        if 'data_access_locations' in source and source['data_access_locations']:
            markdown += "**Data Access Locations:**\n"
            markdown += format_list_items(source['data_access_locations']) + "\n\n"
        
        if 'data_provision_mechanisms' in source and source['data_provision_mechanisms']:
            markdown += f"**Data Provision Mechanisms:** {', '.join(source['data_provision_mechanisms'])}\n\n"
        
        if 'data_formats' in source and source['data_formats']:
            markdown += f"**Data Formats:** {', '.join(source['data_formats'])}\n\n"
        
        if 'data_versioning_and_releases' in source:
            markdown += f"**Data Versioning and Releases:** {source['data_versioning_and_releases']}\n\n"
        
        if 'additional_notes' in source:
            markdown += f"**Additional Notes:** {source['additional_notes']}\n\n"
    
    # Ingest Information
    if 'ingest_info' in rig_data:
        ingest = rig_data['ingest_info']
        markdown += "## Ingest Information\n\n"
        
        if 'ingest_categories' in ingest and ingest['ingest_categories']:
            markdown += f"**Ingest Categories:** {', '.join(ingest['ingest_categories'])}\n\n"
        
        if 'utility' in ingest:
            markdown += f"**Utility:** {ingest['utility']}\n\n"
        
        if 'scope' in ingest:
            markdown += f"**Scope:** {ingest['scope']}\n\n"
        
        if 'relevant_files' in ingest and ingest['relevant_files']:
            markdown += "### Relevant Files\n\n"
            headers = ['File Name', 'Location', 'Description']
            markdown += format_table_items(ingest['relevant_files'], headers) + "\n"
        
        if 'included_content' in ingest and ingest['included_content']:
            markdown += "### Included Content\n\n"
            headers = ['File Name', 'Included Records', 'Fields Used']
            markdown += format_table_items(ingest['included_content'], headers) + "\n"
        
        if 'filtered_content' in ingest and ingest['filtered_content']:
            markdown += "### Filtered Content\n\n"
            headers = ['File Name', 'Filtered Records', 'Rationale']
            markdown += format_table_items(ingest['filtered_content'], headers) + "\n"
        
        if 'future_considerations' in ingest and ingest['future_considerations']:
            markdown += "### Future Content Considerations\n\n"
            for consideration in ingest['future_considerations']:
                markdown += f"**{consideration.get('category', 'General')}:** {consideration.get('consideration', '')}\n"
                if 'relevant_files' in consideration:
                    markdown += f"  - Relevant files: {consideration['relevant_files']}\n"
                markdown += "\n"
        
        if 'additional_notes' in ingest:
            markdown += f"**Additional Notes:** {ingest['additional_notes']}\n\n"
    
    # Target Information
    if 'target_info' in rig_data:
        target = rig_data['target_info']
        markdown += "## Target Information\n\n"
        
        if 'infores_id' in target:
            markdown += f"**Target InfoRes ID:** {target['infores_id']}\n\n"
        
        if 'edge_type_info' in target and target['edge_type_info']:
            markdown += "### Edge Types\n\n"
            headers = ['Subject Categories', 'Predicate', 'Object Categories', 'Knowledge Level', 'Agent Type', 'UI Explanation']
            
            edge_table_data = []
            for edge in target['edge_type_info']:
                edge_data = {
                    'subject_categories': ', '.join(edge.get('subject_categories', [])),
                    'predicate': edge.get('predicate', ''),
                    'object_categories': ', '.join(edge.get('object_categories', [])),
                    'knowledge_level': ', '.join(edge.get('knowledge_level', [])),
                    'agent_type': ', '.join(edge.get('agent_type', [])),
                    'ui_explanation': edge.get('ui_explanation', '')
                }
                edge_table_data.append(edge_data)
            
            markdown += format_table_items(edge_table_data, headers) + "\n"
        
        if 'node_type_info' in target and target['node_type_info']:
            markdown += "### Node Types\n\n"
            headers = ['Node Category', 'Source Identifier Types', 'Additional Notes']
            
            node_table_data = []
            for node in target['node_type_info']:
                node_data = {
                    'node_category': node.get('node_category', ''),
                    'source_identifier_types': node.get('source_identifier_types', ''),
                    'additional_notes': node.get('additional_notes', '')
                }
                node_table_data.append(node_data)
            
            markdown += format_table_items(node_table_data, headers) + "\n"
        
        if 'future_considerations' in target and target['future_considerations']:
            markdown += "### Future Modeling Considerations\n\n"
            for consideration in target['future_considerations']:
                markdown += f"**{consideration.get('category', 'General')}:** {consideration.get('consideration', '')}\n\n"
        
        if 'additional_notes' in target:
            markdown += f"**Additional Notes:** {target['additional_notes']}\n\n"
    
    # Provenance Information
    if 'provenance_info' in rig_data:
        provenance = rig_data['provenance_info']
        markdown += "## Provenance Information\n\n"
        
        if 'contributions' in provenance and provenance['contributions']:
            markdown += "**Contributors:**\n"
            markdown += format_list_items(provenance['contributions']) + "\n\n"
        
        if 'artifacts' in provenance and provenance['artifacts']:
            markdown += "**Artifacts:**\n"
            markdown += format_list_items(provenance['artifacts']) + "\n\n"
    
    return markdown


@click.command()
@click.option('--input-dir', default='src/docs/rigs', help='Directory containing RIG YAML files')
@click.option('--output-dir', default='docs', help='Directory to write markdown files')
@click.option('--file', help='Convert a specific RIG file (optional)')
def main(input_dir, output_dir, file):
    """Convert RIG YAML files to markdown format."""
    
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    
    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)
    
    if file:
        # Convert specific file
        yaml_files = [input_path / file]
    else:
        # Convert all YAML files in directory
        yaml_files = list(input_path.glob('*.yaml'))
    
    for yaml_file in yaml_files:
        if yaml_file.name == 'test_rig.yaml':
            continue  # Skip test file
            
        try:
            with open(yaml_file, 'r') as f:
                rig_data = yaml.safe_load(f)
            
            # Generate markdown
            rig_name = yaml_file.stem
            markdown_content = yaml_to_markdown(rig_data, rig_name)
            
            # Write markdown file
            markdown_file = output_path / f"{rig_name}.md"
            with open(markdown_file, 'w') as f:
                f.write(markdown_content)
            
            click.echo(f"Converted {yaml_file} -> {markdown_file}")
            
        except Exception as e:
            click.echo(f"Error converting {yaml_file}: {e}", err=True)


if __name__ == '__main__':
    main()