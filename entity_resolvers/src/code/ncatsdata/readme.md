# CUREID Knowledge Graph Standardization Pipeline

A semi-automated hybrid pipeline for converting free-text clinical terms from patient case reports into standardized biomedical ontology identifiers, enabling computational analysis and knowledge graph integration.

---

## 🎯 Purpose

**Problem:** Patient case reports contain unstructured clinical language  
- "G-tube fed", "difficulty gaining weight", "widely spaced nipples"
- Inconsistent terminology, colloquialisms, multi-concept descriptions

**Solution:** Map to standardized ontology identifiers  
- HPO (Human Phenotype Ontology): HP:0011471, HP:0001824, HP:0006610
- MONDO (diseases), CHEBI (drugs), HGNC (genes)

**Result:** Machine-readable, interoperable biomedical data ready for:
- Knowledge graph construction
- Computational analysis and inference
- Integration with Translator ecosystem
- Clinical decision support systems

---

## 📊 Pipeline Overview

```
Raw JSON → SRI Resolution → AI Curation → Human QC → Standardized nodes/edges

```

### Four-Stage Process

1. **Automated Mapping** - SRI Name Resolver queries biomedical databases
2. **AI Curation** - Claude Code reviews non-exact matches, detects errors
3. **Human QC** - Expert validation of AI suggestions
4. **Final Application** - Apply finalized IDs to complete dataset

---

## 🚀 Quick Start

### Prerequisites

```bash
# Python 3.10+
pip install pandas openpyxl requests

# Reference ontology files
data/reference_data/hpo_ids.tsv      # ~20K HPO terms
data/reference_data/mondo_ids.csv    # ~26K MONDO terms
```

### Run Complete Pipeline

```bash
# Step 1: SRI Resolution
python cureid_sri_resolve.py \
    --json_in data/input/cureid_cases_VERSION.json \
    --sri_min_score 150.0

# Step 2: AI Curation (Claude Code)
# Use prompt: LLM_CURATION_PROMPT.md
# Input:  data/output/qc/SRI_nodes_non_exact_for_review_VERSION.xlsx
# Output: data/output/qc/SRI_nodes_Claude_reviewed_VERSION.xlsx

# Step 3: Human QC
# Open: SRI_resolved_cureid_VERSION_llm_ids.xlsx
# Add columns: human_decision, human_comments, final_curie_id, final_curie_label

# Step 4: Apply finalized IDs
python cureid_apply_final_ids.py \
    --json_in data/input/cureid_cases_VERSION.json \
    --final_nodes_xlsx cureid_resolved_full_with_manualQC_final_curie.xlsx \
    --out_tsv data/output/cureid_edges_final_VERSION.tsv
```

---

## 📁 Repository Structure

```
cureid-pipeline/
├── README.md                           # This file
├── PIPELINE_VISUAL.md                  # Detailed flowchart
├── requirements.txt                    # Python dependencies
│
├── scripts/
│   ├── cureid_sri_resolve.py    # Step 1: SRI resolution
│   ├── cureid_apply_final_ids.py      # Step 4: Apply final IDs
│
├── prompts/
│   ├── LLM_CURATION_PROMPT.md         # Step 2: AI curation instructions
│   └── PROMPT_FOR_CLAUDE_CODE.md      # Quick command for Claude Code
│
├── data/
    ├── input/
    │   └── cureid_cases_VERSION.json  # Raw CUREID data
    ├── reference_data/
    │   ├── hpo_ids.tsv                # HPO reference ontology
    │   └── mondo_ids.csv              # MONDO reference ontology
    └── output/
        ├── qc/                        # Curation files
        └── cureid_edges_final_*.tsv   # Final output


```

---

## 🔧 Core Scripts

### 1. `cureid_sri_resolve_fixed.py`
**Purpose:** Query SRI Name Resolver for standardized identifiers

**Key Features:**
- Min score filtering to improve quality
- Caching to avoid redundant API calls
- Handles multiple node types (Disease, Drug, Phenotype, Gene, etc.)
- Splits long phenotype strings for better matching

**Usage:**
```bash
python cureid_sri_resolve_fixed.py \
    --json_in data/input/cureid_cases_RASopathies.json \
    --sri_min_score 150.0 \
    --sri_max_hits 10 \
    --outdir data/output
```

**Outputs:**
- `SRI_resolved_cureid_VERSION.tsv` - All nodes with mappings
- `SRI_nodes_non_exact_for_review_VERSION.xlsx` - Needs curation

---

### 2. AI Curation (Claude Code)
**Purpose:** Review non-exact matches using biomedical expertise

**Input:** `SRI_nodes_non_exact_for_review_VERSION.xlsx`

**Process:**
Claude Code detects and corrects:
- **MULTI_CONCEPT_NEEDS_SPLIT** - "A, B, and C" → split into 3 terms
- **ROLE_MISMATCH** - Wrong anatomy/action (ACTH ≠ PTH, nipples ≠ incisors)
- **OVER_SPECIFIC** - Added unwarranted subtypes
- **UNDER_SPECIFIC** - Too generic/vague
- **WRONG_ONTOLOGY** - Disease term for phenotype

**Output:** `SRI_nodes_Claude_reviewed_VERSION.xlsx`

Columns added:
- `claude_decision` - KEEP | OVERRIDE
- `error_tag` - What error was detected
- `claude_mapped_curie_list` - Corrected CURIE(s)
- `claude_mapped_label_list` - Corrected label(s)
- `curation_notes` - Explanation

---

### 3. `cureid_apply_final_ids.py`
**Purpose:** Apply finalized mappings to complete dataset

**Key Features:**
- Re-parses original JSON from scratch
- Ignores any embedded CURIEs in JSON
- Applies human-validated final mappings
- Explodes multi-CURIE splits into separate edges
- Flags nodes missing final mappings

**Usage:**
```bash
python cureid_apply_final_ids.py \
    --json_in data/input/cureid_cases_VERSION.json \
    --final_nodes_xlsx cureid_resolved_full_with_manualQC_final_curie.xlsx \
    --out_tsv data/output/cureid_edges_final_VERSION.tsv
```

**Output:** Complete knowledge graph with standardized IDs

---

## 🎯 Quality Assurance

### 1. Reference File Constraints
- HPO and MONDO reference files constrain AI suggestions
- Prevents hallucination of invalid identifiers
- Only valid, non-obsolete CURIEs can be proposed

### 2. Multi-Layer Validation
- **Layer 1:** SRI API (automated database lookup)
- **Layer 2:** Claude AI (error detection + correction)
- **Layer 3:** Human expert (final validation)

### 3. Error Detection Categories
Systematic identification of:
- Anatomical mismatches (wrong body part)
- Action/verb errors (gaining ≠ bearing)
- Specificity problems (too broad/narrow)
- Ontology misuse (disease vs phenotype)
- Multi-concept labels needing splits

### 4. Provenance Tracking
Every mapping preserves:
- Original free-text label
- SRI suggestion + score
- AI suggestion + error tags
- Human decision + comments
- Final approved CURIE + label

---

## 🔍 Common Issues & Solutions

### Issue: Low SRI Match Quality
**Symptoms:** Too many vague/generic matches  
**Solution:** Increase `--sri_min_score` (try 150-200 for phenotypes)

### Issue: AI Missing Obvious Splits
**Symptoms:** Multi-concept labels not detected  
**Solution:** Check prompt includes clear split examples; emphasize comma/slash detection

### Issue: AI Hallucinating CURIEs
**Symptoms:** Invalid identifiers like HP:9999999  
**Solution:** Ensure reference files are loaded; prompt constrains to reference file CURIEs

### Issue: Inconsistent AI Performance
**Symptoms:** First 10 rows careful, then rushed  
**Solution:** Add "review ALL rows carefully" reminder; consider chunking large files

---

## 📚 Additional Resources

### Documentation
- `PIPELINE_VISUAL.md` - Detailed flowchart with examples
- `CURATION_WORKFLOW_README.md` - Step-by-step workflow guide
- `LLM_CURATION_PROMPT.md` - Full AI curation instructions

### Related Tools
- SRI Name Resolver: https://name-resolution-sri.renci.org/
- Translator: https://ncats.nih.gov/translator
- KGX: https://github.com/biolink/kgx

**Questions?** Open an issue or contact the ODIN team.

---

## 🔄 Version History

### Version 3.0 (December 2024)
- Added AI-assisted curation layer
- Reference file constraint system
- Multi-concept split detection
- Improved error categorization
- Human QC workflow integration

### Version 2.0
- SRI Name Resolver integration
- Min score filtering
- Caching system

### Version 1.0
- Initial manual curation workflow
