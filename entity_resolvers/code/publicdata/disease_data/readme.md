# 🔬 Disease Data Pipeline (TargetGraph)

The `disease_data/` pipeline in TargetGraph is a modular system for downloading, transforming, and harmonizing disease-related data from multiple biomedical resources into a standardized and ID-resolved format. This supports robust node creation and target-disease associations for the knowledge graph.

---

## 📂 Directory Structure

```
src/code/publicdata/disease_data/
│
├── downloads/              # Scripts to fetch raw data from external sources
│   ├── mondo_download.py
│   ├── doid_download.py
│   ├── omim_download.py
│   ├── medgen_download.py
│   └── nodenorm_disease_download.py
│
├── transformers/           # Scripts to clean, normalize, and extract structured info
│   ├── mondo_transform.py
│   ├── doid_transform.py
│   ├── omim_transform.py
│   ├── medgen_transform.py
│   ├── nodenorm_disease_transform.py
│   └── orphanet_transform.py
│
├── disease_merge.py        # Master merger that harmonizes all cleaned sources
├── disease_config.yaml     # YAML config defining inputs/outputs per source
└── outputs/                # Cleaned + merged disease data (defined in config)
```

---

## ⚙️ How the Pipeline Works

Each disease data source goes through two stages:

1. **Download**

   * Retrieves raw files or API responses.
   * Tracked by metadata (`*_metadata.json`).
   * Example: `mondo_download.py` fetches `mondo.owl`.

2. **Transform**

   * Cleans, renames, and standardizes fields.
   * Explodes cross-references and applies consistent prefixes (e.g., `doid_DOID`, `mondo_OMIM`).
   * Applies provenance suffixes for traceability.

3. **Merge**

   * `disease_merge.py` combines the transformed outputs.
   * Uses ID resolution rules, score-based mapping, and provenance consolidation.
   * Final output includes:

     * `disease_ids.csv`
     * Mapping stats, diffs (if `qc_mode` is enabled)

---

## ✅ Supported Sources

| Source   | Description                          | Notes                                       |
| -------- | ------------------------------------ | ------------------------------------------- |
| MONDO    | Unified disease ontology             | Used as backbone for merging                |
| DOID     | Disease Ontology                     | Cross-referenced with MONDO                 |
| OMIM     | Online Mendelian Inheritance in Man  | Queried via API, includes gene associations |
| MedGen   | NCBI clinical concepts               | Includes CUI-based mappings                 |
| Orphanet | Rare disease resource                | Transformed from XML or TSV                 |
| NodeNorm | Translator’s disease resolver output | Used as fallback resolution set             |

---

## 🚀 Usage

### Run all disease modules via CLI:

```bash
python src/code/main.py DISEASES --all
```

### Run specific modules:

```bash
python src/code/main.py DISEASES --omim_download --omim_transform
```

---

## 📁 Configuration

All file paths, URLs, and toggles are defined in `config/diseases_config.yaml`. You can modify this to:
* Update source URLs
* Change output paths
* Enable QC mode and logging
* Specify the location of the gene/protein ID files for mappings

---

## QC Mode

Set `qc_mode: true` in your config to:
* Generate diff files (e.g., `.diff.txt`, `.diff.html`)
* Save intermediate CSVs for manual inspection
* Track processing metadata in JSON
