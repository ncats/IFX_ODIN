## 🔧 Capabilities

- Modular pipelines for each biomedical entity domain (Targets, Diseases, Drugs, etc.)
- Full provenance tracking and QC toggling
- Unified identifier resolution and consolidation logic
- Reproducible metadata and structured diffs
- Support for FTP, REST, and SPARQL-based ingestion
- Designed for seamless handoff or csv files for resolved entities

---

## 🛠️ Getting Started

### 0) Setup

```bash
bash setup.sh
pip install -r requirements.txt
python src/code/main.py --help
```

### 1) Run the pipeline

#### Run all processors:
```bash
python src/code/main.py ALL
```

#### Run a specific domain (e.g., TARGETS):
```bash
python src/code/main.py TARGETS --all
python src/code/main.py DISEASES --all
python src/code/main.py DRUGS --all
```

#### Run specific processor(s) (e.g., gene_merge, protein_merge):
```bash
python src/code/main.py TARGETS --gene_merge --protein_merge
```

> ⚠️ Each processor can be toggled into `qc_mode` using the global config flag to control intermediate qc outputs.

---

## 🧠 Pipeline Logic

Each domain (TARGETS, DRUGS, etc.) consists of modular Python scripts for:

- **Download**: Retrieve raw files from FTP, REST APIs, or SPARQL endpoints.
- **Transform**: Clean and structure files into intermediate CSVs.
- **Merge**: Harmonize identifiers across sources with detailed provenance.
- **Resolve IDs**: Consolidate and upsert NCATS identifiers.
- **Metadata**: Each step logs metadata in a structured JSON format.

QC and diff outputs are automatically routed to `src/data/**/qc/` and metadata to `src/data/**/metadata/`.

---

## 📁 Directory Layout

```
config/         # YAML configs per domain
src/
  ├── code/
  │   └── publicdata/  # Modular data processing scripts
  └── data/
      ├── raw/         # Unmodified downloaded files
      ├── cleaned/     # Transformed and merged outputs
      ├── qc/          # Intermediate debug/QC files
      └── metadata/    # Metadata logs and reports
```
