## Documentation Scripts

The `scripts` folder contains scripts used to facilitate generation of knowledge-source-specific ingest documentation, like the RIG.

- **[create_rig.py](./create_rig.py):** script creates a new RIG from a template.
- **[generate_rig_index.py](./generate_rig_index.py):** script generates an index of all RIGs in the `translator-knowledge-sources` repo. 
- **[mkg_to_rig.py](./mkg_to_rig.py):** script copies over node and edge metadata, from a TRAPI-style meta_knowledge_graph.json file, into the **`node_type_info`** and **`edge_type_info`** in the **`target_info`** section of a specified translator ingests knowledge source RIG.
- **[rig_to_markdown.py](./rig_to_markdown.py):** script converts a RIG into a Markdown file.

Also found here is the [rig_template.yaml](./rig_template.yaml) file, which is used as a template for new RIGs.
