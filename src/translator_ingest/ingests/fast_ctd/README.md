# Fast CTD

This is just a proof of concept example for how an ingest could be implemented using transform() to process records without iterating through them one at a time (as transform_record necessitates). It should be faster, but there doesn't seem to be a big difference, probably due to the bottleneck being in file reading and not in transformation. It will be removed as we implement more real examples.


