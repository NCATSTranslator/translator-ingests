#!/usr/bin/env python3
"""Generate RIG table for documentation."""

import os
import yaml
from pathlib import Path
import re


def find_rig_files(base_path):
    """Find all RIG YAML files in the ingests directory."""
    rig_files = []
    ingests_path = Path(base_path) / "src" / "translator_ingest" / "ingests"
    
    for rig_file in ingests_path.rglob("*rig*.yaml"):
        if rig_file.name.endswith("-rig.yaml") or rig_file.name.endswith("_rig.yaml"):
            rig_files.append(rig_file)
    
    return sorted(rig_files)


def extract_rig_info(rig_file):
    """Extract information from a RIG YAML file."""
    try:
        with open(rig_file, 'r') as f:
            content = f.read()
            # Handle YAML files that might not start with ---
            if not content.strip().startswith('---'):
                content = '---\n' + content
            data = yaml.safe_load(content)
        
        # Extract basic info
        name = data.get('name', 'Unknown')
        source_info = data.get('source_info', {})
        infores_id = source_info.get('infores_id', 'Unknown')
        
        # Get filename without extension for linking
        filename = rig_file.stem
        
        return {
            'name': name,
            'infores_id': infores_id,
            'filename': filename,
            'yaml_path': str(rig_file),
            'md_exists': check_md_exists(rig_file)
        }
    except Exception as e:
        print(f"Error processing {rig_file}: {e}")
        return None


def check_md_exists(rig_file):
    """Check if corresponding markdown file exists."""
    md_file = rig_file.parent / f"{rig_file.stem}.md"
    return md_file.exists()


def generate_table(rigs):
    """Generate the markdown table."""
    table = "| InfoRes ID | Source Name | YAML | Markdown |\n"
    table += "|------------|-------------|------|----------|\n"
    
    for rig in rigs:
        # From /src/docs/rig_index.md, we need to go up two levels to reach /rigs/
        yaml_link = f"[{rig['filename']}.yaml](../../rigs/{rig['filename']}.yaml)"
        # Always create markdown link since we're generating markdown files from YAML
        md_link = f"[{rig['filename']}.md](../../rigs/{rig['filename']}.md)"
        
        table += f"| {rig['infores_id']} | {rig['name']} | {yaml_link} | {md_link} |\n"
    
    return table


def update_rig_index(table, docs_path):
    """Update the rig_index.md file with the generated table."""
    rig_index_path = Path(docs_path) / "src" / "docs" / "rig_index.md"
    
    with open(rig_index_path, 'r') as f:
        content = f.read()
    
    # Replace content between markers
    start_marker = "<!-- RIG_TABLE_START -->"
    end_marker = "<!-- RIG_TABLE_END -->"
    
    start_idx = content.find(start_marker)
    end_idx = content.find(end_marker) + len(end_marker)
    
    if start_idx != -1 and end_idx != -1:
        new_content = (
            content[:start_idx + len(start_marker)] + 
            "\n" + table + "\n" +
            content[end_idx - len(end_marker):]
        )
        
        with open(rig_index_path, 'w') as f:
            f.write(new_content)
        
        print(f"Updated {rig_index_path} with {len(table.split('\n')) - 3} RIGs")
    else:
        print("Could not find RIG table markers in rig_index.md")


def main():
    """Main function."""
    base_path = Path(__file__).parent.parent
    docs_path = base_path / "docs"
    
    # Find all RIG files
    rig_files = find_rig_files(base_path)
    print(f"Found {len(rig_files)} RIG files")
    
    # Extract info from each RIG
    rigs = []
    for rig_file in rig_files:
        rig_info = extract_rig_info(rig_file)
        if rig_info:
            rigs.append(rig_info)
    
    # Generate table
    table = generate_table(rigs)
    
    # Update the index file
    update_rig_index(table, docs_path)


if __name__ == "__main__":
    main()