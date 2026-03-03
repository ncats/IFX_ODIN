# New Data Source Ingest Playbook

## Goal
Provide a repeatable checklist for adding a new data source to the target graph pipeline.

## Checklist

1) **Identify and download the source data**
   - Add download rules to the relevant Snakefile (e.g., `workflows/pharos.Snakefile`).
   - Store files under `input_files/auto/<source>/`.
   - Record exact URLs in a design doc.

2) **Capture version metadata**
   - Prefer an official version endpoint if available.
   - If not, use a stable proxy (e.g., `Last-Modified` header).
   - Write version data to a small file (e.g., `reactome_version.tsv`) so adapters can reuse it.

3) **Implement an InputAdapter**
   - Inherit from `src/interfaces/input_adapter.py` (or `FlatFileAdapter`).
   - Implement:
     - `get_all`
     - `get_datasource_name`
     - `get_version` (include `version`, `version_date`, `download_date`).
   - Emit `Node` / `Relationship` (or `Edge`) models that match the schema.

4) **Map to the data model**
   - Confirm existing node/edge classes or add new ones in `src/models/`.
   - Use stable IDs and consistent prefixes.

5) **Wire configuration into YAML**
   - Add the adapter to the YAML (`working.yaml` for trial; later `target_graph.yaml`).
   - Pass file paths and version metadata file paths via `kwargs`.

6) **Run and validate**
   - Run the download rule(s) in Snakemake.
   - Run the ETL via the YAML entrypoint (e.g., `src/use_cases/working.py`).
   - Validate that counts and labels look correct.

7) **Document the ingest**
   - Create or update a design doc in `designs/` covering:
     - Inputs + URLs
     - Model mapping
     - Adapter responsibilities
     - Version strategy

## Notes
- Start in `working.yaml` for iteration; copy configuration into `target_graph.yaml` once stable.
- Prefer deriving metadata (version, version_date) during download so adapters stay simple.
