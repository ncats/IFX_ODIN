# LLM Curation Prompt for CUREID Node Resolution

## IMPORTANT: DO NOT WRITE CODE - CURATE DIRECTLY

**You are Claude Code running in a live environment.** Your job is to:
1. **Read the input TSV file directly**
2. **Manually curate each row using your biomedical expertise**
3. **Write the output TSV file directly**

**DO NOT** write Python scripts for the user to run. **DO NOT** try to encode curation logic into automated rules. Use your biomedical reasoning case-by-case.

## Task Overview

You are an expert biomedical curator for the NIH NCATS TargetGraph team. Review and improve node mappings from the Translator SRI Name Resolver for patient-reported CUREID data.

## Input File

`data/output/SRI_nodes_non_exact_for_llm_<VERSION>.tsv`

### Input Columns:
- `node_label`: Original free-text label from patient/doctor report
- `node_type`: Disease | Drug | Gene | PhenotypicFeature | AdverseEvent | SequenceVariant
- `node_curie`: SRI's top choice CURIE (may be empty)
- `resolved_label`: SRI's label for that CURIE
- `resolution_score`: SRI confidence (NOT 0-1 normalized, can be >1000)
- `exact_match`: "Y" or "N" (based on exact string match to label or synonym)
- `alternates_top5`: JSON array of up to 5 alternatives: `[{"curie":"...", "label":"...", "score":...}, ...]`
- `n_edges`: How many times this node appears in the knowledge graph (importance metric)

## Output File

`data/output/SRI_nodes_non_exact_with_llm_mapping_<VERSION>.tsv`

Add these columns to the existing DataFrame:

### Required New Columns

1. **`recommendation`** (required): `KEEP | REPLACE | MULTI_SPLIT | UNMAPPABLE`

2. **`split_terms`** (required):
   - For MULTI_SPLIT: pipe-separated terms (`"term1|term2|term3"`)
   - For others: copy `node_label` as-is

3. **`mapped_curie_list`** (required):
   - KEEP/REPLACE: single CURIE
   - MULTI_SPLIT: pipe-separated CURIEs (`"HP:0001635|HP:0002091|HP:0012418"`)
   - UNMAPPABLE: empty string `""`

4. **`mapped_label_list`** (required):
   - Labels corresponding to CURIEs (pipe-separated for MULTI_SPLIT)
   - UNMAPPABLE: empty string

5. **`mapping_notes`** (required):
   - 1-2 sentences explaining your decision
   - Professional, biomedical, concise
   - Explain WHY you made this choice

## Recommendation Types Explained

### KEEP
SRI's `node_curie` is correct despite non-exact match.

**When to use:**
- Resolved term semantically matches node_label
- Formulation variants match base drug (e.g., "Leuprolide Acetate Depot" → Leuprolide)
- Clinical qualifiers don't change core concept (e.g., "with symptoms", "severe", "bilateral")

**Example:**
```
node_label: "Everolimus (Afinitor, Afinitor Disperz, Zortress)"
node_curie: CHEBI:68478
resolved_label: "Everolimus"
→ KEEP
notes: "SRI correctly mapped to Everolimus base drug; brand names in parentheses are acceptable synonyms."
```

### REPLACE
Choose different CURIE (from alternates OR your biomedical knowledge).

**CRITICAL: You CAN suggest CURIEs not in alternates_top5!**

**When to use:**
- SRI chose wrong concept entirely
- Wrong anatomical site (scalp ≠ face, nipples ≠ incisors)
- Wrong action/verb (gaining ≠ bearing, gastrostomy ≠ eustachian)
- Wrong specificity (generic label → overly specific numbered subtype)
- Wrong vocabulary (MONDO disease when HP phenotype needed)

**Examples:**
```
node_label: "Difficulty gaining weight"
SRI: "Difficulty weight-bearing"
→ REPLACE with HP:0001824 "Poor weight gain"
notes: "SRI incorrectly mapped 'gaining weight' (nutrition) to 'weight-bearing' (ambulation); HP:0001824 correctly captures inadequate weight gain phenotype."

node_label: "Adrenocorticotropic Hormone Therapy (ACTH)"
SRI: "Parathyroid hormone"
→ REPLACE with CHEBI:3892 "Corticotropin"
notes: "SRI incorrectly mapped ACTH to parathyroid hormone (PTH); completely different hormones. CHEBI:3892 (Corticotropin) is correct term for ACTH."

node_label: "Widely Spaced Nipples"
SRI: "Widely spaced nipples (male)"
→ REPLACE with HP:0006610 "Wide intermamillary distance"
notes: "Replaced UMLS term with HP:0006610 (Wide intermamillary distance) which is preferred HPO term for widely spaced nipples regardless of sex."
```

### MULTI_SPLIT
Label describes multiple distinct biomedical concepts.

**When to use:**
- Comma/slash-separated lists
- Multiple phenotypes in one label
- Compound descriptions with "and" connecting distinct concepts

**Example:**
```
node_label: "Curly hair, sparse eyebrows, prominent forehead, and a small chin"
→ MULTI_SPLIT
split_terms: "Curly hair|sparse eyebrows|prominent forehead|small chin"
mapped_curie_list: "HP:0002212|HP:0045075|HP:0011220|HP:0000347"
mapped_label_list: "Curly hair|Sparse eyebrow|Prominent forehead|Micrognathia"
notes: "Label describes four distinct craniofacial features separated by commas; split into individual HP terms for each phenotype."
```

### UNMAPPABLE
No reliable mapping exists.

**When to use:**
- SequenceVariants (protein notation without genomic context)
- Too vague/ambiguous labels
- SRI completely missed the concept and alternates don't help
- Wrong category (dermatologic → skeletal disorder with no good alternates)

**Reserve this for <15% of cases!**

**Example:**
```
node_label: "p.Gly464Ala"
node_type: SequenceVariant
→ UNMAPPABLE
notes: "Protein-level variant notation without genomic context; no standard ontology available for this free-text variant descriptor."
```

## Curation Principles

### 1. Vocabulary Preferences by Node Type

| Node Type | Preferred (in order) |
|-----------|---------------------|
| PhenotypicFeature | **HP** >> MONDO > UMLS |
| Disease | **MONDO** > DOID > UMLS |
| Drug | **CHEBI** > RXCUI > DRUGBANK |
| Gene | **HGNC** > NCBIGene |
| AdverseEvent | HP > UMLS |
| SequenceVariant | Usually unmappable |

### 2. Critical Semantic Checks

#### For Drugs:
- Match active pharmaceutical ingredient, not formulation
- "Acetate", "Depot", "Hydrochloride" = formulations, map to base drug
- Brand names in parentheses = acceptable
- **Watch for hormone mix-ups** (ACTH ≠ PTH, FSH ≠ TSH)

#### For Diseases:
- Avoid numbered subtypes unless specified (e.g., "hypertrophic cardiomyopathy" not "hypertrophic cardiomyopathy 7")
- Don't add location qualifiers not in original ("atrophy" ≠ "posterior atrophy")
- Match specificity level

#### For Phenotypic Features:
**CRITICAL**: These are the most error-prone!

- **Preserve actions/verbs**: "gaining" ≠ "bearing", "passing" ≠ "standing"
- **Preserve anatomical sites**: "pulmonary" ≠ "intestinal", "scalp" ≠ "face", **"nipples" ≠ "incisors"**
- **Preserve body systems**: "gastrostomy tube" ≠ "eustachian tube", "testicular" ≠ "vaginal"
- Use generic HP terms when label is general (avoid overly specific subtypes)
- Split composite descriptions into individual phenotypes
- **Prefer HP terms** over MONDO diseases for phenotype nodes

**Common mistakes to avoid:**
- ❌ "Difficulty gaining weight" → "Difficulty weight-bearing"
- ❌ "G tube fed" → "eustachian tube disorder"
- ❌ "Severe Itching" → "severe congenital neutropenia"
- ❌ "Widely Spaced Nipples" → "Widely-spaced incisors"

### 3. Using External Knowledge

**YOU ARE NOT LIMITED TO alternates_top5!**

If you know a better term from your biomedical training:
1. State the CURIE and label in `mapped_curie_list` / `mapped_label_list`
2. Mark as REPLACE
3. Explain in notes: "SRI alternates missed appropriate term; [CURIE] better matches clinical concept."

**Common cases requiring external knowledge:**
- Poor weight gain → HP:0001824
- Gastrostomy tube feeding → HP:0011471
- Global developmental delay → HP:0001263
- Oral ulcer → HP:0000155
- Hypsarrhythmia → HP:0002521
- Pruritus → HP:0000989
- Diarrhea (generic) → HP:0002014
- Skin rash (generic) → HP:0000988
- Low APGAR score → HP:0030917
- Corticotropin (ACTH) → CHEBI:3892

### 4. Multi-Split Detection

Split when label contains:
- Multiple comma-separated features: "feature A, feature B, feature C"
- Slash-separated alternatives: "Chylacites/Chylous effusion"
- Distinct "and"-connected concepts: "CHF, hypoxia, and hypercarbia"

**Do NOT split:**
- Standard compound terms: "growth and development"
- Single concepts with modifiers: "severe congenital neutropenia"
- Drug combinations that should stay together

### 5. Quality Checks

Before finalizing:
- [ ] Does mapped term match the **literal meaning** of node_label?
- [ ] Is the **specificity level** appropriate (not too general, not too specific)?
- [ ] For phenotypes, did I use **HP terms** whenever possible?
- [ ] For diseases, did I use **MONDO** when available?
- [ ] Did I preserve critical qualifiers (**anatomical site, action/verb, severity**)?
- [ ] Are my **mapping_notes clear and justified**?
- [ ] Did I check for **obvious anatomical/system mismatches**?

## Implementation Instructions

### DO NOT write code! Execute directly:

1. **Read the input file** using pandas:
   ```python
   import pandas as pd
   df = pd.read_csv('data/output/SRI_nodes_non_exact_for_llm_<VERSION>.tsv', sep='\t')
   ```

2. **Iterate through each row** and apply your biomedical reasoning:
   - Examine node_label, node_type, node_curie, resolved_label
   - Parse alternates_top5 JSON
   - Use your biomedical expertise to determine best mapping
   - Populate the 5 new columns

3. **Write the output** using pandas:
   ```python
   df.to_csv('data/output/SRI_nodes_non_exact_with_llm_mapping_<VERSION>.tsv', sep='\t', index=False)
   ```

4. **Print summary statistics**:
   - Total rows processed
   - Count by recommendation type
   - Examples of REPLACE cases showing improvements

## Target Metrics

Aim for:
- **REPLACE: 30-45%** (find better alternates or suggest new terms)
- **KEEP: 25-35%** (SRI was correct)
- **MULTI_SPLIT: 10-15%** (recognize composite labels)
- **UNMAPPABLE: 15-20%** (including ~10 SequenceVariants which are expected)

## Example Workflow

```
User: "Process CUREID nodes for version 251202"

You should:
1. Find: data/output/SRI_nodes_non_exact_for_llm_251202.tsv
2. Load it with pandas
3. For each row, apply biomedical reasoning:
   - Row 1: "Everolimus (Afinitor...)" → Drug formulation, KEEP
   - Row 2: "ACTH" → Wrong hormone mapping, REPLACE with CHEBI:3892
   - Row 3: "Difficulty gaining weight" → Wrong action, REPLACE with HP:0001824
   ...
4. Write: data/output/SRI_nodes_non_exact_with_llm_mapping_251202.tsv
5. Print summary showing improvements
```

## Common Pitfalls to Avoid

❌ **Don't**:
- Write code for the user to run manually
- Try to encode biomedical logic into automated rules
- Accept wrong anatomical sites (nipples ≠ incisors!)
- Accept wrong actions/verbs (gaining ≠ bearing)
- Map phenotypes to disease terms when HP exists
- Default to UNMAPPABLE when you know a better term
- Use only alternates_top5 when your knowledge has the answer

✅ **Do**:
- Read the file and curate directly
- Use your full biomedical reasoning on each case
- Suggest appropriate HP/MONDO/CHEBI terms even if not in alternates
- Split composite labels when they describe multiple concepts
- Preserve specificity and critical qualifiers from original label
- Write clear, professional mapping_notes
- Aim for high REPLACE rate (30-45%) by finding better matches

## Final Checklist

Before declaring completion:
- ✓ All 71 (or N) rows have been curated
- ✓ All 5 new columns are populated
- ✓ Output TSV file has been written
- ✓ Summary statistics printed
- ✓ REPLACE percentage is 30-45%
- ✓ No obvious anatomical/system mismatches remain
- ✓ All SequenceVariants marked UNMAPPABLE (expected)
