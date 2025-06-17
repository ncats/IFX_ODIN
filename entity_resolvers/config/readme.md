# Configuration Files for Entity Resolution Pipeline

This directory contains YAML configuration files used to control the behavior of various modules in the ODIN data integration pipeline. Each YAML file defines paths, parameters, and options for downloading, transforming, and integrating datasets from a specific biomedical domain.

---

## 📁 Directory Structure

- `config/`
  - `targets_config.yaml` — Configuration for processing gene/protein/target datasets (e.g., Ensembl, RefSeq, UniProt)
  - `diseases_config.yaml` — Configuration for disease ID resolution, harmonization (e.g., MONDO, DOID, MedGen, OMIM)
  - `drugs_config.yaml` — Configuration for drug identifier resolution and metadata extraction (e.g., GSRS, ChEMBL)
  - `pathways_config.yaml` — Configuration for pathway resources (e.g., Reactome, WikiPathways)
  - `phenotypes_config.yaml` — Configuration for phenotypic associations and metadata
  - `go_config.yaml` — Configuration for Gene Ontology data processing
  - `ppi_config.yaml` — Configuration for protein-protein interaction data integration

---

## 🛠️ Configuration Structure

Each YAML file follows a standardized structure with keys like:

```yaml
download_url: "<URL to download raw data>"
raw_file: "path/to/raw_file.txt"
cleaned_file: "path/to/cleaned_file.csv"
log_file: "path/to/log_file.log"
dl_metadata_file: "path/to/download_metadata.json"
transform_metadata_file: "path/to/transform_metadata.json"
qc_mode: true  # Enables saving of .diff, .backup, and QC stats
