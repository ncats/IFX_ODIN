# Entity Resolver Pipeline

This repository contains a modular, config-driven data curation and processing pipeline for biomedical entities such as genes, transcripts, proteins, pathways, and diseases. It is designed to support reproducibility, automation, and downstream graph modeling.

## 🛠️ Getting Started

### 0) Setup

```bash
bash setup.sh
pip install -r requirements.txt
python src/code/main.py --help
```

## 🧩 Global `qc_mode` Flag

All YAML config files include a top-level `qc_mode` setting:

```yaml
qc_mode: true
```

When enabled, this flag activates the following quality control features:

* Intermediate `.qc.csv` files with mapping or merge stats
* Flagged rows for manual review and debugging
* Partial transformation outputs for inspection
* All saved under `qc/` directories alongside cleaned outputs

Set `qc_mode: false` in production for faster runs and minimal output.


## 📁 Structure
```bash
src/code/                    # Core processing scripts
├── publicdata/         # Domain-specific modules (targets, drugs, etc.)
│   └── target_data/    # e.g., ensembl_download.py, ncbi_transform.py...
│   └── disease_data/    # e.g., mondo_download.py, disease_merge.py...
src/data/                   # Input/output
│   └── target_data/
│        └── raw/                # Downloaded files (ignored in Git)
│        └── cleaned/            # Final curated outputs
│            └── sources/        # cleaned dataframes
│        └── metadata/           # Metadata & provenance
│        └── qc/               # intermediate files for QC & mapping stats
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

Workflows can be scheduled using tools like `cron`, SLURM, or integrated into CI/CD pipelines. For DAG-based execution:

```bash
snakemake -s src/workflows/targets.Snakefile --cores 4
```
---

© 2025 NCATS_IFX