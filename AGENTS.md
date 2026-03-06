## Ingest Workflow Preferences

- For new ingest work, start by downloading/profiling source files and validating real payload shape before implementing adapter/model changes.
- For new ingest work, pause after discovery and propose a short implementation plan; get user confirmation before making code changes.

## Ingest Standards

- Start new ingest development in `src/use_cases/working.yaml`; only promote to `src/use_cases/pharos/target_graph.yaml` after validation.
- Keep source-specific mapping/coverage decisions documented in a design doc under `designs/`.
- Prefer deriving and persisting datasource metadata (`version`, `version_date`, `download_date`) during download/prep, then have adapters consume it.
- Keep first-pass ingest scope intentionally minimal, then expand in follow-up iterations.
- Use stable IDs and consistent prefixes across nodes/edges.
- Validate assumptions against real payloads (field presence, cardinality, identifier shape) before finalizing model changes.
- Avoid speculative parsing when source text is ambiguous; preserve source text when parsing would be lossy.
- In adapters, dedupe repeated entities in-memory using deterministic IDs.
- Emit ingest output in type-grouped batches (for example: primary nodes, related nodes, then edges).

## Workflow References

- For ingest procedures and execution checklists, use `playbooks/ingest_playbook.md`.
