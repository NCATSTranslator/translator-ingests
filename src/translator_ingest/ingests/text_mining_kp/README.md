# Text Mining KP KGX Pass-through Ingest

## Purpose

This ingest provides a mechanism to process pre-existing KGX files from the Text Mining Knowledge Provider (TMKP) through the translator-ingests pipeline. Unlike other ingests that transform external data formats into KGX, this module reads existing KGX files and applies validation, normalization, and optional transformations while maintaining the KGX format.

### Why Pass-through Processing?

1. **Validation**: Ensures KGX files conform to Biolink Model standards
2. **Normalization**: Applies node normalization via the Node Normalizer service
3. **Filtering**: Allows filtering based on confidence scores, evidence, or other criteria
4. **Provenance**: Maintains or enhances knowledge source attribution
5. **Pipeline Integration**: Leverages existing pipeline infrastructure for logging, monitoring, and quality control

## Processing Functions

The pass-through ingest performs the following operations:

### Node Processing
- Validates required fields (id, category)
- Ensures proper Biolink category assignment
- Preserves node properties (name, description, synonyms, xrefs)
- Creates typed Biolink objects (Gene, Protein, Disease, etc.)

### Edge Processing
- Validates required fields (id, subject, predicate, object)
- Ensures proper knowledge level and agent type attribution
- Optionally filters based on confidence scores
- Preserves evidence and publication information
- Maintains qualifiers and other edge properties

### Optional Transformations
- **Confidence Filtering**: Remove low-confidence associations
- **Category Normalization**: Ensure Biolink-compliant categories
- **Predicate Mapping**: Map to preferred Biolink predicates
- **Source Attribution**: Add or update knowledge source metadata

## Input/Output Specifications

### Input Format
- **Format**: KGX JSON Lines (JSONL)
- **Files**: 
  - `nodes.jsonl`: One node object per line
  - `edges.jsonl`: One edge object per line
- **Schema**: Standard KGX format following Biolink Model

### Output Format
- **Format**: KGX JSON Lines (JSONL)
- **Files**:
  - `text_mining_kp_nodes.jsonl`: Processed nodes
  - `text_mining_kp_edges.jsonl`: Processed edges
- **Enhancements**: Validated, normalized, and optionally filtered

## Configuration

The `text_mining_kp.yaml` file allows customization of:
- Input file paths
- Confidence thresholds for filtering
- Output property selection
- Additional filters or transformations

## Run Instructions

### Prerequisites
1. Place KGX files in the designated data directory
2. Update file paths in `text_mining_kp.yaml`
3. Ensure Koza environment is properly configured

### Execution

```bash
# Run the pass-through ingest
koza transform \
  --config src/translator_ingest/ingests/text_mining_kp/text_mining_kp.yaml \
  --writer-config writer_config.yaml

# Optional: Run with custom confidence threshold
koza transform \
  --config src/translator_ingest/ingests/text_mining_kp/text_mining_kp.yaml \
  --writer-config writer_config.yaml \
  --transform-config '{"min_confidence_threshold": 0.7}'
```

### Pipeline Integration

When integrated with the full pipeline:

```bash
# 1. Skip download (using existing KGX files)
# 2. Transform with pass-through
make transform INGEST=text_mining_kp

# 3. Normalize nodes
make normalize INGEST=text_mining_kp

# 4. Validate output
make validate INGEST=text_mining_kp
```

## Use Cases

### When to Use This Pass-through
- Processing KGX files from external knowledge providers
- Applying consistent validation across multiple KGX sources
- Filtering or subsetting existing KGX data
- Integrating external KGX into the standard pipeline
- Adding provenance or updating metadata

### When NOT to Use
- Creating KGX from non-KGX sources (use standard ingests)
- Simple file copying without processing
- One-time validation checks (use validation utilities directly)

## Monitoring and Logging

The ingest provides detailed logging including:
- Number of nodes/edges processed
- Number of records filtered
- Validation errors encountered
- Processing statistics

Logs are written to the standard Koza output and can be monitored for quality control.

## Future Enhancements

Potential improvements for this pass-through ingest:
1. Advanced filtering based on node/edge properties
2. Batch processing for very large KGX files
3. Parallel processing of nodes and edges
4. Custom validation rules via configuration
5. Integration with specific TMKP metadata standards