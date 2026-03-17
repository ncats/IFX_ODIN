# New Data Source Ingest Playbook

## Goal
Provide a repeatable checklist for adding a new data source to the pharos graph.  The second step of that ETL (pharos graph to TCRD format) is a separate task, not to be included in this playbook.

## Checklist

1) **Identify and download the source data**
   - Add download rules to the relevant Snakefile (e.g., `workflows/pharos.Snakefile`).
   - If at all possible, get a discrete version and version date for the download.
   - Store files under `input_files/auto/<source>/`.
   - User will run the Snakemake workflow to download the data, and report back when finished, to continue to the next step.
   - Record exact URLs in a design doc.

2) **Capture version metadata**
   - Prefer an official version endpoint if available.
   - If not, use a stable proxy (e.g., `Last-Modified` header).
   - Write version data to a small file (e.g., `reactome_version.tsv`) so adapters can reuse it.

3) **Explore and profile the downloaded files**
   - Before adapter implementation, run several iterative passes to inspect real payload shape.
   - Confirm field presence, cardinality, identifiers, edge semantics, and metadata quality.
   - Finalize inclusion/exclusion and mapping decisions based on observed data.

4) **Review previous ETLs for this data**
   - Ask the user about the TCRD ETL, or any later ETLs for the same data.
   - Note that the TCRD format is not an intuitive fit, but usually captures important data.

5) **Review data that makes it into TCRD**
   - Currently Pharos uses pharos319.
   - Review the relevant tables and row counts to understand what was ingested previously.

6) **Write a design document**
   - Create a document in `designs/` to record findings from steps 3–5:
     - Source URLs and file format
     - Version strategy
     - Field mapping decisions and anything excluded
     - How it fits the existing data model

7) **Implement an InputAdapter**
   - Inherit from `src/interfaces/input_adapter.py` (or `FlatFileAdapter`).
   - Implement:
     - `get_all`
     - `get_datasource_name`
     - `get_version` (include `version`, `version_date`, `download_date`).
   - Emit `Node` / `Relationship` (or `Edge`) models that match the schema.

8) **Map to the data model**
   - Confirm existing node/edge classes or add new ones in `src/models/`.
   - Use stable IDs and consistent prefixes.

9) **Wire configuration into YAML**
   - Add the adapter to the YAML (`working.yaml` for trial; later `target_graph.yaml`).
   - Pass file paths and version metadata file paths via `kwargs`.
   - For Pharos data sources: add to **both** `src/use_cases/pharos/pharos.yaml` and
     `src/use_cases/pharos/target_graph.yaml`.

10) **Run and validate**
    - Run the ETL via the YAML entrypoint (e.g., `src/use_cases/working.py`).
    - Validate that counts and labels look correct.

11) **Update the design document**
    - Revise the design doc to reflect what actually ended up in the code:
      - Final field mappings and any decisions that changed during implementation
      - Actual node/edge counts produced
      - Any data quality issues encountered and how they were handled

---

## Lessons Learned

### File format
- **Always survey the file before writing the adapter.** Column names, headers, and ID formats
  frequently differ from documentation or old code. Use `head`, `cut`, and a quick Python profile
  script to confirm shape, cardinality, and value ranges before committing to a design.
- **Headerless files are common.** Pass explicit `fieldnames` to `csv.DictReader` rather than
  relying on a header row.
- **"Human" in the filename doesn't mean human-filtered.** E.g. JensenLab's
  `human_tissue_integrated_full.tsv` contains BTO tissues spanning all organisms. Profile
  the actual gene IDs and tissue ontology prefixes to understand scope.

### Gene / protein identifiers
- **Check what ID type the file actually uses.** JensenLab TISSUES uses ENSP (Ensembl protein)
  IDs mixed with gene symbols for non-coding entries — not the ENSG IDs the old TCRD code implied.
  Filter with `startswith("ENS")` to accept both ENSP and ENSG while dropping miRNAs, rRNAs, etc.
- **Don't emit a `Protein` node if there is no extra data to contribute.** When there are no
  `calculated_properties` or other protein-level fields to add, the edge's `start_node` is
  sufficient — a standalone `Protein` node just adds noise.

### Human-specificity: adapters vs. resolvers

- **Adapters are responsible for human filtering; resolvers are not.** A resolver's job is to map
  IDs to canonical equivalents — it doesn't skip unmatched entities, it just returns them as-is.
  Filtering to human-only data must happen in the adapter at emit time, before records enter the
  graph. If a non-human tissue node is emitted, it will land in the graph even without a UBERON
  mapping.
- **`uberon.obo` is multi-species, not human-only.** It covers vertebrates broadly (human, mouse,
  zebrafish, xenopus, etc.). A BTO ID appearing as an xref in the OBO does not guarantee it is
  human. Use the **FMA xref filter**: UBERON/CL terms with an FMA (Foundational Model of Anatomy)
  xref are human-grounded, because FMA is a human-specific ontology. This mirrors the old TCRD
  `getOntologyMap()` approach.
- **The FMA filter belongs in the adapter, not the resolver.** `TissueResolver` should resolve
  whatever it's given — it's a general-purpose tool used across multiple pipelines. Human-specificity
  is a data-source concern. Encode it in `_load_valid_tissue_ids()` (or equivalent) at adapter
  instantiation, not in the resolver logic.

### Tissue / ontology IDs
- **Verify that the ontology prefix appears in `uberon.obo` before adding it to
  `valid_ontologies`.** CLDB (BRENDA Cell Line Database) is a valid prefix in `constants.py` but
  has zero xrefs in UBERON, so adding it to the resolver list does nothing.
- **Watch for typos in source data ontology IDs.** JensenLab has a single erroneous `CLBD:0007212`
  entry (should be `CLDB`) — worth reporting upstream.

### Tau and tissue specificity
- **Tau requires a curated, flat, non-redundant tissue panel.** It breaks down when:
  - The tissue set is ontology-driven with multiple levels of granularity (e.g. "liver",
    "hepatocytes", and "liver parenchymal cell" all present as separate rows).
  - Cell lines and anatomical tissues are mixed in the same dataset.
  - HPM, HPA, and GTEx all use curated atlases where tau is well-defined; JensenLab TISSUES
    does not.

### Versioning
- **Use the `Last-Modified` HTTP header when no official version endpoint exists.** JensenLab
  regenerates files on a weekly Sunday schedule — the `Last-Modified` header captures that publish
  date reliably. Write it to a small TSV in Snakemake so the adapter can read it as `version_date`.
