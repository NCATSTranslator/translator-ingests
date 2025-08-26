# HPOA Ingest Things to Do

- review the latest ingest project templates for any changes to method protocols and expectations
    - Look at the latest ctd.py and ctd.yaml for the code design which merges all ingest Python and the specifications into single files, using tagged readers/writers and method 'tag' argument
    - Re-introduce the metadata.yaml file (but ask about the relationship of this with the new RIG.yaml convention)
- review rig.md once more to see if expectations are fully met in the ingest code
    - use MONDO terms in **`disease_context_qualifier`** field (insofar as possible, using mondo_map); check if this if the latest Koza properly implements this! 
- document potential sharable code snippets and perhaps, move them to a more shared location; communicate to the team; check other ingests for similar code snippets and coordinates consolidation with other teammates
