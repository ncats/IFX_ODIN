# CUREID Drug Standardization Pipeline

Pipeline to map patient-reported drug names from CUREID to standardized chemical identifiers (ChEBI, RxNorm, PubChem) for knowledge graph integration and data sharing.

## Overview

Patient-reported drug names in clinical databases are often inconsistent, containing typos, brand names, dosage information, and various formulations. This pipeline standardizes these names to enable:
- Data sharing across research institutions
- Integration into biomedical knowledge graphs
- Semantic analysis and drug-disease associations

### Key Features

- **Multi-source resolution**: Queries SRI Name Resolver, PubChem, NCATS Chemical Resolver
- **Semantic matching**: Uses SRI's Translator Name Resolver for fuzzy matching
- **LLM-assisted curation**: Reviews ambiguous cases with detailed reasoning
- **Human curation integration**: Incorporates expert-curated preferred names
- **Reproducible pipeline**: All steps documented with versioned outputs
- **Deduplication**: Optimized API queries to avoid redundant calls

## Quick Start

### Prerequisites

```bash
pip install pandas openpyxl requests
```

### Run Pipeline

```bash
# Phase 1: Clean drug names
python src/phase1_clean_drugs.py

# Phase 2: Query SRI Name Resolver
python src/phase2_query_sri.py

# Phase 3: Query additional sources (for non-exact matches)
python src/phase3_multi_source.py

# Phase 4: LLM batch review (done manually - see llm_review/)

# Phase 5: Finalize standardization
python src/phase5_finalize.py

# Phase 2b (optional): Re-query with preferred names
python src/phase2b_requery_sri_with_preferred_names.py
```

## Pipeline Stages

### Phase 1: Data Cleaning
- Remove dosage (e.g., "500 mg"), routes (e.g., "Oral Tablet")
- Extract base drug names and combinations
- Output: `phase1_cleaned_YYYY.MM.DD.xlsx`

### Phase 2: SRI Name Resolver
- Query SRI for semantic matching to ChEBI/RxNorm
- Split EXACT vs NON-EXACT matches
- Output: `phase2_sri_results_YYYY.MM.DD.xlsx`

### Phase 3: Multi-Source Enrichment
- For NON-EXACT only: query PubChem, NCATS, MyChemInfo
- Provides alternative identifiers for comparison
- Output: `phase3_multi_source_YYYY.MM.DD.xlsx`

### Phase 4: LLM Review
- Review non-exact matches row-by-row
- Compare SRI vs other sources
- Make decisions: KEEP_SRI, USE_PUBCHEM, NEEDS_HUMAN_REVIEW
- Output: Batch files in `llm_review/`

### Phase 5: Final Standardization
- Consolidate all results
- Integrate human curations
- Output: `drug_standardization_YYYY.MM.DD.xlsx`

### Phase 2b: Re-query with Preferred Names (Optional)
- Use human-curated preferred names instead of cleaned names
- Compare results and merge improvements
- Resolves additional ambiguous cases

## Output Format

### Main Output File
`data/output/drug_standardization_YYYY.MM.DD.xlsx`

**Tabs:**
1. **All Drugs** - Complete dataset (19,361 drugs)
2. **Exact Matches** - High confidence (10,817 drugs)
3. **Needs Review** - Manual QC required (8,544 drugs)
4. **Human Curation Helps** - Cases where manual curation improves results

**Key Columns:**
- `standard_id` - Standardized identifier (ChEBI, RxNorm, or PubChem)
- `preferred_name` - Human-readable drug name
- `standard_id_source` - Provenance (ChEBI, RxNorm, PubChem, etc.)
- `match_decision` - Quality indicator
  - `EXACT_MATCH` - SRI exact match ✅
  - `KEEP_SRI` - SRI non-exact but acceptable ✅
  - `USE_PUBCHEM` - PubChem better than SRI ✅
  - `NEEDS_HUMAN_REVIEW` - Requires manual review ⚠️
- `llm_notes` - Detailed reasoning for decisions

## Directory Structure

```
drug_resolver/
├── README.md                           # This file
├── PIPELINE_DOCUMENTATION.md           # Detailed pipeline docs
├── CHANGELOG.md                        # Version history
├── .gitignore                          # Git ignore rules
│
├── data/
│   ├── input/                          # Source data
│   │   ├── 2026.04.09-all_cureid_drugs.csv
│   │   ├── 2026.04.08_cure_id_drug_preferred_names_danielle_boyce.xlsx
│   │   └── 2025.08.01_cure_id_lc_drug_map_sheet.xlsx
│   └── output/                         # Pipeline outputs
│       ├── phase1_cleaned_2026.04.18.xlsx
│       ├── phase2_sri_results_2026.04.18.xlsx
│       ├── phase3_multi_source_2026.04.19.xlsx
│       ├── MASTER_drug_resolution_2026.04.20.xlsx
│       └── drug_standardization_2026.04.20_with_phase2b.xlsx
│
├── src/                                # Pipeline scripts
│   ├── phase1_clean_drugs.py
│   ├── phase2_query_sri.py
│   ├── phase2b_requery_sri_with_preferred_names.py
│   ├── phase3_multi_source.py
│   └── phase5_finalize.py
│
├── llm_review/                         # LLM batch reviews
│   └── BATCH_*_REVIEWED_YYYY.MM.DD.xlsx
│
└── archive/                            # Previous runs
```

## Results Summary

**Total drugs**: 19,361

| Category | Count | % |
|----------|-------|---|
| Exact matches | 10,817 | 56% |
| KEEP_SRI | 6,153 | 32% |
| USE_PUBCHEM | 1,454 | 8% |
| NEEDS_HUMAN_REVIEW | 937 | 5% |

**Phase 2b Impact**:
- Re-queried 4,300 drugs with preferred names
- Found 444 improvements (all NEEDS_HUMAN_REVIEW cases)
- Reduced manual review from 1,381 → 937 cases (32% reduction)

## APIs Used

- **SRI Name Resolver**: https://name-resolution-sri.renci.org
- **PubChem**: https://pubchem.ncbi.nlm.nih.gov/rest/pug/
- **NCATS Chemical Resolver**: https://chem.nlm.nih.gov/api
- **MyChemInfo**: https://mychem.info/v1

## Reproducibility

All pipeline stages are deterministic except Phase 4 (LLM review). However, LLM decisions are preserved in output files with detailed notes, making the overall pipeline reproducible.

Key reproducibility features:
- Versioned outputs by date
- All decisions documented with reasoning
- Human curations tracked in input files
- API responses cached in intermediate outputs

## Next Steps

After running this pipeline:

1. **Node Normalization**: Use SRI Node Normalizer to get canonical Biolink CURIEs
2. **Manual QC**: Review the 937 NEEDS_HUMAN_REVIEW cases
3. **Knowledge Graph Integration**: Link standardized IDs to drug-disease relationships
4. **Data Sharing**: Use standardized IDs for cross-institution collaboration

## Contributors

- **Pipeline Development**: Claude Code (Anthropic)
- **Human Curations**: Danielle Boyce, LC team
- **Project**: CURE ID / ODIN (NIH)

## License

Internal NIH project

## Citation

If you use this pipeline, please cite:
```
CUREID Drug Standardization Pipeline (2026)
NIH ODIN Project
https://github.com/[your-org]/cureid-drug-resolver
```

## Support

For questions or issues:
- See `PIPELINE_DOCUMENTATION.md` for detailed documentation
- Review `CHANGELOG.md` for version history
- Open an issue on GitHub

---

**Last Updated**: 2026-04-20
