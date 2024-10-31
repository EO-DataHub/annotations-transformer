# Changelog


## v0.1.0 (18-10-2024)
Initial Commit of annotations transformer
- Listens for Pulsar message
- Reads annotation data (.json) from S3 and transforms it to RDF formatted data (.trig)
- Updates destination key by replacing input source with input target string
- Uploads transformed data to S3 in the transformed/ directory
- Sends another Pulsar message to trigger ingestion pipeline

