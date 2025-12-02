# CUREID Node Resolution Pipeline - Visual Flow

## Overview Diagram

flowchart TD
    A[Raw CUREID JSON<br/>cureid_cases_VERSION.json] --> B

    B[SRI Name Resolution<br/>cureid_resolver_sri_only.py] --> C1[SRI_resolved_cureid_VERSION.tsv]
    B --> C2[SRI_nodes_non_exact_for_llm_VERSION.tsv]

    C2 --> D{Exact Match?}
    D -->|Yes| E1[Auto-Accept]
    D -->|No| E2[LLM Curation<br/>Claude Code]

    E2 --> F[SRI_nodes_non_exact_with_llm_mapping_VERSION.tsv]

    C1 --> G
    F --> G

    G[Apply LLM Mapping<br/>cureid_resolver_llm.py] --> H[SRI_resolved_cureid_VERSION_llm_ids.tsv]

    H --> I[Graph Export / Downstream Use]
