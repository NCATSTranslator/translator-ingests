# KGX Passthrough Ingest Parser

Sometimes the source of Translator knowledge graph data is (just) a generated KGX dataset. Such a dataset is generally compliant to a previous release of the Biolink Model, which may not be up to date.

Here we describe how to write a KGX passthrough ingest parser using available tools and procedures.

## Overall Process

1. Create an ingest-specific branch within a development copy of the Translator Ingest (TI) pipeline repository, e.g. "icees-ingest" for ICEES.
2. Identify the (internet endpoint) visible sources of the previously generated [KGX-compliant node and edge jsonl target files](https://github.com/biolink/kgx) for the knowledge source, then record the endpoint urls within the ingest-specific **download.yaml** file. This will trigger downloading of the files for local processing by the pipeline.
3. It is helpful at this point to perform some kind of assessment of the ['meta knowledge graph' of the Biolink Model contents](#characterizing-the-basic-metagraph) of the KGX nodes and edges, both for RIG documentation purposes and to identify any need for filtering or Biolink Model updating of the legacy dataset (which can perhaps be applied within the parser code - step 6 below), to ensure proper normalization and validation downstream in the pipeline.
4. Create a RIG (e.g., **icees-rig.yaml**) for the ingest task (using the **make** **`new-rig`** target, or using the equivalent **just** recipe). Fill out the RIG details using the available meta knowledge graph information from the previous step.
5. Write a simple ingest specification (e.g., **icees.yaml**) which specifies separate tagged readers to parse the nodes and edges jsonl rows (records) into separate Koza transform streams for processing.
6. (Optionally) Write some basic unit tests with sample KGX-style input records representative of the input KGX file, to test the Pydantic remapping, for example, for any filtering or updating of fields.
7. Write a simple Python ingest module (e.g., **icees.py**) alongside the **download.yaml** file, encoding the methods which simply open each downloaded KGX node and edge file, to processes in the distinct reader streams through the transformer methods, to conversion the records to the stream of Biolink Model-compliant Pydantic objects, for uniform downstream processing by the TI pipeline.

The challenge is: What do we do when these KGX files don't validate?.  The TMKP KGX passthrough ingest parser is an example on how to handle this. Note that validation during ingest may be more important than to just use what the KGX dataset provides.

## Characterizing the Basic Meta Knowledge Graph

It is generally advised to perform some kind of assessment of the Biolink Model contents of the KGX dataset, both for RIG documentation purposes and also, to identify any need for filtering or Biolink Model updating of the legacy dataset (which can perhaps be applied within the parser code - step 6 above), to ensure proper normalization and validation downstream in the pipeline.

There are several ways to do this:

1. If the KGX dataset is retrieved from a server sitting behind a Plater TRAPI instance (e.g., like RENCI's Automat installation), chances are that a static **meta_knowledge_graph.json** file describing the Biolink Model content of the KGX is  being used as the source of data for the **`/meta_knowledge_graph`** TRAPI endpoint.  The contents of that file can be accessed for the required KGX assessment.  A new [mkg_to_rig.py](https://github.com/biolink/resource-ingest-guide-schema/blob/main/src/scripts/mkg_to_rig.py) script is available to copy over node and edge metadata from the meta_knowledge_graph.json file into the **`node_type_info`** and **`edge_type_info`** in the **`target_info`** section of a specified translator ingests knowledge source RIG.
2. The [KGX library graph summarization method](https://biolink.github.io/kgx/reference/graph_operations/summarize_graph.html) can be applied to the KGX data files (note: the aforementioned **mkg_to_rig.py** doesn't yet support importing the resulting KGX graph summary directly into the RIG)
3. A new Translator project KGX profiler tool (by Daniel Korn) may soon also be available to perform this assessment (note: the aforementioned **mkg_to_rig.py** doesn't yet support importing the resulting KGX graph summary directly into the RIG).
