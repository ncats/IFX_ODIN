
---
## 📈 Pipeline Statistics (RASopathies Example)

| Stage | Input | Output | Time |
|-------|-------|--------|------|
| **SRI Resolution** | 223 rows from JSON | 148 unique nodes | ~5-10 min |
| **Exact Matches** | 148 nodes | 58 auto-accepted (39%) | instant |
| **AI Review** | 91 non-exact + 16 variants | 13 KEEP + 60 OVERRIDE | ~5-10 min |
| **Human QC** | 91 non-exact | 54 AGREED_LLM(7 AGREED_SRI_KEEP) (67%), 5 SRI > LLM, 7 HUMAN OVERRIDE | ~90-120 min |
| **Final Apply** | 148 records | 222 edges (after splits) | instant |

**Total Time:** ~75-155 minutes per dataset (most time is human QC)

---

## 📊 Output Formats
 also available here: https://opendata.ncats.nih.gov/public/cureid/
 
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
