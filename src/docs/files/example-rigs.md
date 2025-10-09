# Writing a Reference Ingest Guide (RIG)

A Reference Ingest Guide (RIG) is a structured document that describes the scope, rationale, and modeling approach for 
ingesting content from an external source to a data repository compliant with the Biolink Model. This guide will walk 
you through creating a RIG using the provided schema and template.

## Overview

A RIG consists of four main sections:

1. **Source Information** - Details about the data source
2. **Ingest Information** - What content is included/excluded and why
3. **Target Information** - How the data is modeled in the output graph
4. **Provenance Information** - Who contributed and relevant artifacts

## Getting Started

Start with the template file `rig_template.yaml` and fill in each section according to your data source. The template 
provides the complete structure with comments indicating required vs optional fields.

## Section 1: Source Information

Document key details about your data source:

```yaml
source_info:
  infores_id: "infores:your-source-id"  # Required: InfoRes identifier
  description: "Brief description of the source"  # Optional but recommended
  citations:  # Optional: Publications about the source
    - "PMID:12345678"
    - "https://doi.org/10.1000/example"
  terms_of_use: "CC-BY 4.0"  # Required: License or terms
  data_access_locations:  # Optional: Where to access the data
    - "https://example.com/download"
  data_provision_mechanisms:  # Optional: How data is distributed
    - file_download
    - api_endpoint
  data_formats:  # Optional: Data formats available
    - json
    - csv
  data_versioning_and_releases: "Monthly releases with semantic versioning"  # Optional
  additional_notes: "Any other relevant information"  # Optional
```

## Section 2: Ingest Information

Describe what content you're ingesting and why:

```yaml
ingest_info:
  ingest_categories:  # Optional: Type of source
    - primary_knowledge_provider
  utility: "Why this data is valuable for your use case"  # Required
  scope: "High-level description of what's included/excluded"  # Optional but recommended
  
  relevant_files:  # Required: Source files being processed
    - file_name: "data.json"
      location: "https://example.com/data.json"
      description: "Main dataset containing..."
  
  included_content:  # Optional: What records are included
    - file_name: "data.json"
      included_records: "All gene-disease associations with evidence scores > 0.5"
      fields_used: "gene_id, disease_id, evidence_score, publication_refs"
  
  filtered_content:  # Optional: What's excluded and why
    - file_name: "data.json"
      filtered_records: "Associations with evidence scores <= 0.5"
      rationale: "Low confidence associations excluded to maintain data quality"
  
  future_considerations:  # Optional: Future content to consider
    - category: edge_content
      consideration: "Include pathway information when available"
      relevant_files: "pathway_data.json"
```

## Section 3: Target Information

Describe the output graph structure:

```yaml
target_info:
  infores_id: "infores:[source-abbreviation]"  # Optional: Target resource identifier
  
  edge_type_info:  # Required: Types of edges created
    - subject_categories:
        - "biolink:Gene"
      predicate: "biolink:associated_with"
      object_categories:
        - "biolink:Disease"
      knowledge_level:
        - knowledge_assertion
      agent_type:
        - manual_agent
      edge_properties:
        - "biolink:evidence_count"
        - "biolink:publications"
      ui_explanation: "Gene-disease associations curated from literature with evidence scores"
  
  node_type_info:  # Required: Types of nodes created
    - node_category: "biolink:Gene"
      source_identifier_types: "NCBIGene"
      node_properties:
        - "biolink:name"
        - "biolink:synonym"
    - node_category: "biolink:Disease"
      source_identifier_types: "MONDO"
      node_properties:
        - "biolink:name"
```

## Section 4: Provenance Information

Document contributors and related artifacts:

```yaml
provenance_info:  # Optional but recommended
  contributions:
    - "Jane Doe - code author"
    - "John Smith - domain expertise"
  artifacts:
    - "GitHub issue: https://github.com/NCATSTranslator/translator-ingests/issues/123"
    - "Ingest survey: https://docs.google.com/document/xyz"
```

## Example Structure

```yaml
ReferenceIngestGuide:
  name: "[source-name] RIG"
  source_info: { ... }
  ingest_info: { ... }
  target_info: { ... }
  provenance_info: { ... }
```

The complete template in `rig_template.yaml` provides the full structure with all available fields and their data 
types. Use this as your starting point and fill in the relevant sections for your specific data source.