# QA Notes

This is a lightweight local backlog for QA Browser ideas and follow-up work
captured while features are moving quickly. Items here are reminders, not
approved implementation plans.

## Curation Follow-ups

- Convert the Pharos TDL override records into the shared curation batch format,
  store them under the appropriate S3 graph prefix, and apply them during graph
  builds instead of treating them as a special manual adapter.
- Generalize QA Browser curation beyond metabolite harmonization: allow an
  authenticated curator to create supported node or edge curations for any ODIN
  graph and save the resulting batches to that graph's S3 curation prefix.
- Preserve curator identity, creation time, target graph, stable target IDs, and
  operation provenance in every general-purpose curation batch.

These are intentionally deferred until the metabolite S3 curation path is
working end to end.
