# CUREID Node Resolution Pipeline - Visual Flow

## Overview Diagram

```mermaid
flowchart TD
    Start([Patient/Doctor Reports<br/>Free Text Data]) --> Step1

    Step1[Step 1: Raw CUREID JSON Input<br/>cureid_cases_VERSION.json<br/>~70-100 non-exact nodes per dataset]

    Step1 --> Step2

    Step2[Step 2: SRI Name Resolution<br/>cureid_resolver_sri_only.py<br/>AUTOMATED]

    Step2 --> SRI_Out1[SRI_resolved_cureid_VERSION.tsv<br/>All edges with SRI mappings]
    Step2 --> SRI_Out2[SRI_new_nodes_all_VERSION.tsv<br/>All processed nodes]
    Step2 --> SRI_Out3[SRI_nodes_non_exact_for_llm_VERSION.tsv<br/>⚠️ Non-exact matches]

    SRI_Out3 --> Decision{Exact Match?}

    Decision -->|Yes ~50%| Auto[Automatically Accepted<br/>High confidence]
    Decision -->|No ~50%| Manual[Needs Manual Curation<br/>~70 nodes]

    Manual --> Step3

    Step3[Step 3: LLM Expert Curation<br/>Claude Code + Biomedical Expertise<br/>MANUAL - NO CODE SCRIPTS]

    Step3 --> Curator{For Each Node:<br/>Apply Biomedical<br/>Reasoning}

    Curator --> Keep[KEEP 25-35%<br/>SRI mapping correct<br/>Drug formulations OK]
    Curator --> Replace[REPLACE 30-45%<br/>Better term found<br/>Fix anatomy/action errors]
    Curator --> Split[MULTI_SPLIT 10-15%<br/>Composite labels<br/>Split into multiple terms]
    Curator --> Unmap[UNMAPPABLE 15-20%<br/>SequenceVariants<br/>Too vague/ambiguous]

    Keep --> LLM_Out
    Replace --> LLM_Out
    Split --> LLM_Out
    Unmap --> LLM_Out

    LLM_Out[SRI_nodes_non_exact_with_llm_mapping_VERSION.tsv<br/>✅ Curated with 5 new columns]

    LLM_Out --> Step4
    SRI_Out1 --> Step4

    Step4[Step 4: LLM Mapping Application<br/>cureid_resolver_llm.py VERSION<br/>AUTOMATED]

    Step4 --> Step4_Process[Merge LLM curations<br/>into edge table<br/>Create final_* columns]

    Step4_Process --> Final_Out[SRI_resolved_cureid_VERSION_llm_ids.tsv<br/>🎯 FINAL CURATED EDGES]

    Final_Out --> Step5

    Step5[Step 5: Knowledge Graph Export]

    Step5 --> Use1[Neo4j Ingestion]
    Step5 --> Use2[Biolink Validation]
    Step5 --> Use3[TargetGraph Integration]
    Step5 --> Use4[NCATS Tools & Databases]

    style Step3 fill:#ff9999
    style Curator fill:#ff9999
    style LLM_Out fill:#99ff99
    style Final_Out fill:#99ff99
    style Step2 fill:#99ccff
    style Step4 fill:#99ccff
```

## Critical Errors Fixed by Manual Curation

```mermaid
flowchart LR
    A[Wrong Anatomy<br/>Nipples → Incisors ❌] --> Fix1[HP:0006610<br/>Wide intermamillary distance ✅]
    B[Wrong Action<br/>Gaining → Bearing ❌] --> Fix2[HP:0001824<br/>Poor weight gain ✅]
    C[Wrong Body System<br/>G-tube → Eustachian ❌] --> Fix3[HP:0011471<br/>Gastrostomy tube feeding ✅]
    D[Wrong Hormone<br/>ACTH → PTH ❌] --> Fix4[CHEBI:3892<br/>Corticotropin ✅]

    style Fix1 fill:#99ff99
    style Fix2 fill:#99ff99
    style Fix3 fill:#99ff99
    style Fix4 fill:#99ff99
    style A fill:#ff9999
    style B fill:#ff9999
    style C fill:#ff9999
    style D fill:#ff9999
```

## Quality Metrics (Version 251128)

```mermaid
pie title LLM Curation Results (71 nodes)
    "REPLACE (Better match found)" : 29
    "KEEP (SRI correct)" : 19
    "UNMAPPABLE (Truly impossible)" : 14
    "MULTI_SPLIT (Composite labels)" : 9
```

## Vocabulary Preferences by Node Type

```mermaid
flowchart TD
    Pheno[PhenotypicFeature] --> HP[HP Human Phenotype Ontology<br/>GOLD STANDARD]
    Disease[Disease] --> MONDO[MONDO<br/>Best cross-references]
    Drug[Drug] --> CHEBI[CHEBI Chemical Entities<br/>Preferred for drugs]
    Gene[Gene] --> HGNC[HGNC Gene Nomenclature<br/>Standard gene IDs]
    AE[AdverseEvent] --> HP2[HP or UMLS]
    SV[SequenceVariant] --> Unmap2[Usually UNMAPPABLE<br/>Free-text protein notation]

    style HP fill:#99ff99
    style MONDO fill:#99ff99
    style CHEBI fill:#99ff99
    style HGNC fill:#99ff99
```

## Manual vs Automated Approach

```mermaid
flowchart LR
    subgraph Automated["❌ Automated Script Approach (FAILED)"]
        A1[Pattern Matching] --> A2[Brittle Rules]
        A2 --> A3[Edge Cases]
        A3 --> A4[0% REPLACE<br/>70% KEEP rubber-stamp]
        A4 --> A5[Nipples → Incisors ❌]
    end

    subgraph Manual["✅ Manual Biomedical Reasoning (SUCCESS)"]
        M1[Read TSV] --> M2[Apply Expertise<br/>Per Case]
        M2 --> M3[External Knowledge<br/>Beyond 5 alternates]
        M3 --> M4[40.8% REPLACE<br/>26.8% KEEP]
        M4 --> M5[All Anatomy Preserved ✅]
    end

    Automated -.Failed after<br/>3 attempts.-> Manual

    style Automated fill:#ffcccc
    style Manual fill:#ccffcc
```

## How to View This Diagram

### Option 1: GitHub/GitLab
If you push this file to GitHub or GitLab, the Mermaid diagrams will render automatically.

### Option 2: VS Code
Install the "Markdown Preview Mermaid Support" extension and preview this file.

### Option 3: Online Mermaid Editor
Copy each diagram block to: https://mermaid.live/

### Option 4: Export as PNG/SVG
Use the Mermaid CLI or online editor to export diagrams as images for PowerPoint/email.

---

**Pipeline Owner**: NIH NCATS TargetGraph Team
**Last Updated**: December 2024
**Current Version**: 251128
**Current Performance**: 40.8% REPLACE, 26.8% KEEP, 12.7% MULTI_SPLIT, 19.7% UNMAPPABLE
