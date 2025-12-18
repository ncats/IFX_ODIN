# 🔬 CUREID Node Curation Prompt V3 - Error Detection & Correction

## ⚠️ CRITICAL: DO NOT WRITE CODE - CURATE DIRECTLY

You are Claude Code. Load the file, curate each row using your biomedical expertise, and write the output file. DO NOT create Python scripts.

---

## 🎯 Your Task

Review SRI Name Resolver mappings and detect/correct errors using your clinical expertise.

**Input:** `data/output/qc/SRI_nodes_non_exact_for_review_12.16.25.xlsx`

**Output:** `data/output/qc/SRI_nodes_Claude_reviewed_12.16.25.xlsx`

---

## 📥 Input Columns

- `original_node_label` - Free-text clinical term
- `node_type` - Disease | Drug | Gene | PhenotypicFeature | AdverseEvent | SequenceVariant
- `SRI_node_curie` - SRI's top choice
- `SRI_resolved_label` - SRI's label for that CURIE
- `resolution_source` - Where SRI found it
- `SRI_score` - Confidence score
- `exact_match` - Y/N
- `SRI_alternates_top5` - JSON array of alternatives
- `n_edges` - Frequency in knowledge graph

---

## 📤 Output Columns TO ADD

Add these columns to the input file:

### 1. `claude_decision`
- `KEEP` - SRI is correct
- `OVERRIDE` - Need different CURIE(s)

### 2. `error_tag`
One or more error types (pipe-separated if multiple):

- `MULTI_CONCEPT_NEEDS_SPLIT` - Label contains multiple distinct concepts
- `ROLE_MISMATCH` - Wrong anatomical site, action, or body system
- `OVER_SPECIFIC` - SRI added unwarranted specificity (e.g., numbered subtype)
- `UNDER_SPECIFIC` - SRI too broad/generic
- `QUALIFIER_ONLY` - Mapped only qualifier, missed main concept
- `VARIANT_UNMAPPABLE` - Sequence variant without genomic context
- `WRONG_ONTOLOGY` - Used disease when phenotype needed (or vice versa)
- `0` - No errors (only if KEEP)

**Can combine:** `ROLE_MISMATCH|UNDER_SPECIFIC`

### 3. `claude_mapped_curie_list`
- KEEP: empty or copy SRI CURIE
- OVERRIDE: your corrected CURIE(s)
- MULTI_CONCEPT_NEEDS_SPLIT: pipe-separated CURIEs (`HP:0001|HP:0002|HP:0003`)
- VARIANT_UNMAPPABLE: empty

### 4. `claude_mapped_label_list`
- Labels corresponding to CURIEs
- Pipe-separated for splits
- Empty for UNMAPPABLE

### 5. `curation_notes`
- 1-2 sentences explaining decision
- For OVERRIDE: explain what error you fixed
- For MULTI_CONCEPT_NEEDS_SPLIT: explain how you split it

---

## 🔍 ERROR DETECTION GUIDE

### Error Type 1: MULTI_CONCEPT_NEEDS_SPLIT

**Detect when original label contains:**
- Multiple comma-separated features: `"feature A, feature B, and feature C"`
- Slash-separated alternatives: `"Chylacites/Chylous effusion"`
- Multiple "and"-connected distinct concepts: `"thin eyebrows and scarce hair"`
- Lists of phenotypes: `"curly hair, sparse eyebrows, prominent forehead"`

**Examples:**
```
✓ SPLIT: "Sparse scalp hair and no eyelashes, eyebrows, or body hair"
  → Split into: Sparse scalp hair | Absent eyelashes | Absent eyebrows | Absent body hair
  → CURIEs: HP:0002209 | HP:0000561 | HP:0002223 | HP:0002230

✓ SPLIT: "Chylacites/Chylous effusion"
  → Split into: Chylous ascites | Chylous effusion
  → CURIEs: HP:0025406 | HP:0025309

✓ SPLIT: "Hypertrophic cardiomyopathy and a decrease in left ventricular stroke volume"
  → Split into: Hypertrophic cardiomyopathy | Decreased left ventricular stroke volume
  → CURIEs: HP:0001639 | HP:0025473

✗ DO NOT SPLIT: "Severe congenital neutropenia" (single concept with modifiers)
✗ DO NOT SPLIT: "Growth and development" (standard compound term)
```

---

### Error Type 2: ROLE_MISMATCH

**Detect when SRI mapped to:**
- Wrong anatomical site: nipples → incisors, scalp → face
- Wrong action/verb: "gaining weight" → "weight-bearing", "passing stool" → "standing"
- Wrong body system: gastrostomy tube → eustachian tube, pulmonary → intestinal
- Wrong hormone: ACTH → PTH, FSH → TSH

**Examples:**
```
✓ ROLE_MISMATCH:
  Original: "Difficulty gaining weight"
  SRI: "Difficulty weight-bearing"
  Fix: HP:0001824 "Poor weight gain"

✓ ROLE_MISMATCH:
  Original: "G tube fed"
  SRI: "Eustachian tube disorder"
  Fix: HP:0011471 "Gastrostomy tube feeding dependence"

✓ ROLE_MISMATCH:
  Original: "ACTH (Adrenocorticotropic Hormone)"
  SRI: "Parathyroid hormone"
  Fix: CHEBI:3892 "Corticotropin"

✓ ROLE_MISMATCH:
  Original: "Widely spaced nipples"
  SRI: "Widely-spaced incisors"
  Fix: HP:0006610 "Wide intermamillary distance"
```

---

### Error Type 3: OVER_SPECIFIC

**Detect when SRI added:**
- Numbered subtypes not in original: "cardiomyopathy" → "cardiomyopathy 7"
- Location qualifiers not stated: "atrophy" → "posterior cortical atrophy"
- Specific variants of generic terms: "rash" → "maculopapular rash, type 2A"

**Examples:**
```
✓ OVER_SPECIFIC:
  Original: "Hypertrophic cardiomyopathy"
  SRI: "Hypertrophic cardiomyopathy 7"
  Fix: Use generic HP:0001639 "Hypertrophic cardiomyopathy"

✓ OVER_SPECIFIC:
  Original: "Developmental delay"
  SRI: "Developmental delay with autism spectrum disorder"
  Fix: HP:0001263 "Global developmental delay" (if general)
```

---

### Error Type 4: UNDER_SPECIFIC

**Detect when SRI too broad:**
- Specific symptom → generic category: "severe itching" → "skin disorder"
- Detailed phenotype → vague term: "widely spaced nipples" → "chest abnormality"

**Examples:**
```
✓ UNDER_SPECIFIC:
  Original: "Severe itching of extremities"
  SRI: "Skin disorder"
  Fix: HP:0000989 "Pruritus"
```

---

### Error Type 5: WRONG_ONTOLOGY

**Detect when:**
- PhenotypicFeature mapped to MONDO disease (should be HP)
- Disease mapped to HP phenotype (should be MONDO)

**Ontology preferences:**
- PhenotypicFeature → **HP** >> MONDO
- Disease → **MONDO** >> DOID
- Drug → **CHEBI** >> RXCUI
- AdverseEvent → HP > UMLS

---

### Error Type 6: VARIANT_UNMAPPABLE

**Detect when:**
- SequenceVariant with only protein notation: `p.Gly464Ala`
- No genomic context available
- Free-text variant descriptor

**Action:** Mark VARIANT_UNMAPPABLE, leave CURIEs empty

---

## 🔧 Using Reference Files (Optional but Recommended)

**Available in:** `data/reference_data/`

### HPO Reference (`hpo_ids.tsv`)
For PhenotypicFeature nodes:
```python
import pandas as pd
hpo = pd.read_csv('data/reference_data/hpo_ids.tsv', sep='\t')
matches = hpo[hpo['hpo_label'].str.contains('weight gain', case=False, na=False)]
```

### MONDO Reference (`mondo_ids.csv`)
For Disease nodes:
```python
mondo = pd.read_csv('data/reference_data/mondo_ids.csv')
matches = mondo[mondo['mondo_preferred_label'].str.contains('cardiomyopathy', case=False)]
```

**When to use:**
- ✅ Check reference files for Phenotypes and Diseases
- ✅ Validates CURIE exists and isn't obsolete
- ✅ If found in reference → high confidence
- ⚠️ If not found → use your expert knowledge
- ❌ No reference for Drugs/Genes → always use knowledge

---

## 📊 Target Metrics

Aim for this distribution:

| Category | Target % | Example Count (100 rows) |
|----------|----------|--------------------------|
| MULTI_CONCEPT_NEEDS_SPLIT | 10-15% | ~11 |
| ROLE_MISMATCH | 15-20% | ~14 |
| UNDER_SPECIFIC | 20-30% | ~25 |
| OVER_SPECIFIC | 5-10% | ~4 |
| VARIANT_UNMAPPABLE | 10-20% | ~16 |
| WRONG_ONTOLOGY | 5-10% | ~5 |
| No errors (KEEP) | 10-20% | ~9 |

**Note:** Many rows will have multiple error tags (e.g., `ROLE_MISMATCH|UNDER_SPECIFIC`)

---

## ✅ Decision Making Process

For EACH row:

### Step 1: Check for Multiple Concepts
- Does label contain commas, slashes, or "and" connecting distinct concepts?
- If YES → `MULTI_CONCEPT_NEEDS_SPLIT`
  - Split into pipe-separated terms
  - Find CURIE for each term
  - Set `claude_decision: OVERRIDE`

### Step 2: Check SRI's Mapping (if not split)
- Does SRI CURIE match the clinical concept?
- Check anatomy, action/verb, body system
- Check specificity level

### Step 3: Determine Error Type
- Wrong site/action/system → `ROLE_MISMATCH`
- Added unwarranted specificity → `OVER_SPECIFIC`
- Too broad/generic → `UNDER_SPECIFIC`
- Disease for phenotype → `WRONG_ONTOLOGY`
- Sequence variant → `VARIANT_UNMAPPABLE`

### Step 4: Find Correct CURIE(s)
- Search reference files (HPO/MONDO) if available
- Use your biomedical knowledge
- Check SRI alternates
- Can propose CURIEs not in alternates

### Step 5: Set Decision
- Errors found → `claude_decision: OVERRIDE`
- SRI correct → `claude_decision: KEEP` + `error_tag: 0`

---

## 🚫 Common Mistakes to Avoid

❌ **Don't:**
- Skip rows (curate ALL rows carefully)
- Accept anatomical mismatches (nipples ≠ teeth!)
- Accept action/verb errors (gaining ≠ bearing)
- Miss obvious splits (commas, slashes, lists)
- Use only SRI alternates (use your knowledge!)
- Write code instead of curating

✅ **Do:**
- Read EVERY row carefully
- Use reference files when available
- Split composite labels
- Correct anatomical/system errors
- Use appropriate ontology (HP for phenotypes)
- Explain your reasoning in notes

---

## 📝 Example Curations

### Example 1: Multi-concept Split
```
Input:
  original_node_label: "Curly hair, sparse eyebrows, prominent forehead, and a small chin"
  node_type: PhenotypicFeature
  SRI_node_curie: MONDO:0011053
  SRI_resolved_label: "intellectual disability-sparse hair-brachydactyly syndrome"

Output:
  claude_decision: OVERRIDE
  error_tag: MULTI_CONCEPT_NEEDS_SPLIT
  claude_mapped_curie_list: HP:0002212|HP:0045075|HP:0011220|HP:0000347
  claude_mapped_label_list: Curly hair|Sparse eyebrow|Prominent forehead|Micrognathia
  curation_notes: "Label describes 4 distinct craniofacial phenotypes; SRI incorrectly mapped to syndrome. Split into individual HP terms for each feature."
```

### Example 2: Role Mismatch
```
Input:
  original_node_label: "ACTH (Adrenocorticotropic Hormone Therapy)"
  node_type: Drug
  SRI_node_curie: UNII:N19A0T0E5J
  SRI_resolved_label: "Parathyroid hormone"

Output:
  claude_decision: OVERRIDE
  error_tag: ROLE_MISMATCH
  claude_mapped_curie_list: CHEBI:3892
  claude_mapped_label_list: Corticotropin
  curation_notes: "SRI confused ACTH with PTH - completely different hormones. CHEBI:3892 is correct identifier for corticotropin (ACTH)."
```

### Example 3: Over-Specific
```
Input:
  original_node_label: "Hypertrophic cardiomyopathy"
  node_type: Disease
  SRI_node_curie: MONDO:0011000
  SRI_resolved_label: "hypertrophic cardiomyopathy 7"

Output:
  claude_decision: OVERRIDE
  error_tag: OVER_SPECIFIC
  claude_mapped_curie_list: MONDO:0005045
  claude_mapped_label_list: Hypertrophic cardiomyopathy
  curation_notes: "SRI added numbered subtype not specified in original label. Generic MONDO:0005045 more appropriate."
```

### Example 4: Keep (Correct)
```
Input:
  original_node_label: "Everolimus (Afinitor, Afinitor Disperz, Zortress)"
  node_type: Drug
  SRI_node_curie: CHEBI:68478
  SRI_resolved_label: "Everolimus"

Output:
  claude_decision: KEEP
  error_tag: 0
  claude_mapped_curie_list: 
  claude_mapped_label_list: 
  curation_notes: "SRI correctly mapped to everolimus base drug. Brand names in parentheses are acceptable variants."
```

---

## 🚀 Execution Instructions

1. **Load input file:**
```python
import pandas as pd
df = pd.read_excel('data/output/qc/SRI_nodes_non_exact_for_review_12.16.25.xlsx')
```

2. **Load reference files (optional but helpful):**
```python
hpo = pd.read_csv('data/reference_data/hpo_ids.tsv', sep='\t')
mondo = pd.read_csv('data/reference_data/mondo_ids.csv')
```

3. **For EACH row, add the 5 new columns**

4. **Write output:**
```python
df.to_excel('data/output/qc/SRI_nodes_Claude_reviewed_12.16.25.xlsx', index=False)
```

5. **Print summary:**
```
Total rows: 97
By error type:
  MULTI_CONCEPT_NEEDS_SPLIT: 11
  ROLE_MISMATCH: 14
  UNDER_SPECIFIC: 25
  OVER_SPECIFIC: 4
  VARIANT_UNMAPPABLE: 16
  No errors (KEEP): 9
  
By decision:
  OVERRIDE: 88
  KEEP: 9
```

---

## ⏱️ IMPORTANT: Review ALL Rows Carefully

Don't rush through the file. Each row deserves careful biomedical reasoning.

If you find yourself speeding up after row 10 → STOP and refocus.

Quality > Speed.

---

## ✅ Final Checklist

Before completion:
- [ ] All rows have `claude_decision`
- [ ] All rows have `error_tag`
- [ ] OVERRIDE rows have CURIEs filled in
- [ ] MULTI_CONCEPT_NEEDS_SPLIT rows have pipe-separated values
- [ ] All rows have `curation_notes`
- [ ] Summary statistics printed
- [ ] Output file written

**Now begin curation.**
