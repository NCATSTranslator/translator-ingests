# Translator Ingest Platform

This folder contains both knowledge-source-specific ingest specifications (declarative metadata and code) and shared code libraries used by those specifications.

## Scripts

The `scripts` folder contains scripts used to facilitate generation of knowledge-source-specific ingest documentation, like the RIG.

- **mkg_to_rig.py:** script copies over node and edge metadata, from a TRAPI-style meta_knowledge_graph.json file, into the **`node_type_info`** and **`edge_type_info`** in the **`target_info`** section of a specified translator ingests knowledge source RIG.
