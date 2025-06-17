# Target Data Pipeline

This repository contains a modular, config-driven data curation and processing pipeline for biomedical entities such as genes, transcripts, proteins, pathways, and diseases. It is designed to support reproducibility, automation, and downstream graph modeling.

## 📁 Structure

```bash
src/                    # Core processing scripts
├── publicdata/         # Domain-specific modules (targets, drugs, etc.)
│   └── target_data/    # e.g., ensembl_data.py, ncbi_data.py...
data/                   # Input/output
├── raw/                # Downloaded files (ignored in Git)
├── cleaned/            # Final curated outputs
├── semi/               # Intermediate/merged
├── logs/               # Metadata & provenance
reports/                # JSON/CSV summaries (included in Git)
tests/                  # Pytest unit tests per module
workflows/              # Snakemake workflows, cron scripts
scripts/                # CLI entry points like main.py
```

## 🧪 Testing

Run all tests using `pytest`:

```bash
pytest tests/
```

## 🛠️ Usage

```bash
python src/scripts/main.py TARGETS --all
```
or 
```bash
python main.py TARGETS --modules ensembl ncbi
```
Or use Snakemake:

```bash
snakemake -s workflows/Snakefile --cores 4
```

## 📦 Dependencies

Install with pip:

```bash
pip install -r requirements.txt
```

Or via conda:

```bash
conda env create -f environment.yml
```

## 📊 Outputs
- Cleaned CSVs in `data/cleaned/`
- Summary reports in `reports/`
- Metadata logs in `data/logs/`

## 📅 Automate
Schedule with cron or run full DAG via Snakemake.

---

© 2025 NCATS_IFX
