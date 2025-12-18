
---
## 📈 Pipeline Statistics (RASopathies Example)

| Stage | Input | Output | Time |
|-------|-------|--------|------|
| **SRI Resolution** | 211 JSON records | 149 unique nodes | ~5-10 min |
| **Exact Matches** | 149 nodes | 69 auto-accepted (46%) | instant |
| **AI Review** | 64 non-exact + 16 variants | 80 curated rows | ~5 min |
| **Human QC** | 80 AI-reviewed | 80 validated | ~60-90 min |
| **Final Apply** | 211 records | 258 edges (after splits) | ~2 min |

**Total Time:** ~75-115 minutes per dataset (most time is human QC)

---

## 📊 Output Formats

### Final Edge File (`cureid_edges_final_VERSION.tsv`)

**Columns:**
- `subject_label_original` - Original free-text (for audit)
- `subject_label` - Cleaned version
- `subject_type` - Disease | Drug | Phenotype | Gene | etc.
- `subject_final_label` - Standardized ontology term
- `subject_final_curie` - Standardized identifier (HP:*, MONDO:*, etc.)
- `subject_missing_final` - Y/N flag for QC
- `predicate_raw` - Original relationship type
- `biolink_predicate` - Biolink model predicate
- `association_category` - Biolink association type
- `object_*` - Same structure for object node
- `report_id`, `pmid`, `link`, `outcome` - Provenance

**Edge Types Included:**
- Drug → Disease (treatment)
- Drug → Phenotype (target symptoms)
- Drug → AdverseEvent (side effects)
- Disease → Phenotype (manifestations)
- Gene → Disease (associations)
- Gene → SequenceVariant (mutations)
- SequenceVariant → Disease (causality)
