# Entity Resolver Pipeline

This repository contains a modular, config-driven data curation and processing pipeline for biomedical entities such as genes, transcripts, proteins, pathways, and diseases. It is designed to support reproducibility, automation, and downstream graph modeling.

## 🛠️ Getting Started

### 0) Setup

```bash
bash setup.sh
pip install -r requirements.txt
python src/code/main.py --help
```
## 📁 Structure
```
config/
  └── targets/         # YAML configs per domain
src/
  ├── code/
  │   └── publicdata/  # Modular data processing scripts
  └── data/
      ├── raw/         # Unmodified downloaded files
      ├── cleaned/     # Transformed and merged outputs
      ├── qc/          # Intermediate debug/QC files
      └── metadata/    # Metadata logs and reports
```
```bash
src/code/                    # Core processing scripts
├── publicdata/         # Domain-specific modules (targets, drugs, etc.)
│   └── target_data/    # e.g., ensembl_download.py, ncbi_transform.py...
│   └── disease_data/    # e.g., mondo_download.py, disease_merge.py...
src/data/                   # Input/output
│   └── target_data/
│        └── raw/                # Downloaded files (ignored in Git)
│        └── cleaned/            # Final curated outputs
│            └── sources/         # cleaned dataframes
│        └── metadata/               # Metadata & provenance
src/workflows/              # Snakemake workflows, cron scripts
src/tests/                  # Pytest unit tests per module
```

## 🛠️ Usage
examples:
```bash
python src/scripts/main.py TARGETS --all
```
or 
```bash
python main.py TARGETS --ncbi_download
```
Or use Snakemake:

```bash
snakemake -s src/workflows/targets.Snakefile --cores 4
```

## 📦 Dependencies

Install with pip:

```bash
pip install -r requirements.txt
```

Or via conda:

```bash
conda env create -f tgbuild.yml
```

## 📊 Outputs
- Raw source data downloads in `*data/raw/`
- Cleaned TSVs/CSVs in `*data/cleaned/`
- Metadata, logs, and diffs in `*data/metadata/`

## 📅 Automate
Schedule with cron or run full DAG via Snakemake.

---

© 2025 NCATS_IFX
