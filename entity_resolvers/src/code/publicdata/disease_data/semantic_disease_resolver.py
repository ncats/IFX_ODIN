# semantic_disease_resolver.py - Query multiple disease names from the provenance file and resolve cross-source mappings using FAISS

import os
import faiss
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

# ───────────── CONFIG ─────────────
INDEX_PATH = "src/data/publicdata/disease_data/index/layered_disease_index.faiss"
METADATA_CSV = "src/data/publicdata/disease_data/index/layered_disease_index_metadata.csv"
MODEL_PATH = "src/data/publicdata/disease_data/index/sentence_model"
PROVENANCE_FILE = "src/data/publicdata/disease_data/cleaned/sources/disease_mapping_provenance.csv"
ADDITIONAL_DISEASE_LIST = "src/data/publicdata/disease_data/cleaned/sources/GARD_ids.csv"  
OUTPUT_TSV = "src/data/publicdata/disease_data/index/resolved_disease_mappings.tsv"
CLUSTERS_PIVOT_TSV = "src/data/publicdata/disease_data/index/resolved_clusters_pivot.tsv"
TOP_MATCH_TSV = "src/data/publicdata/disease_data/index/resolved_top_matches.tsv"
TOP_K = 25
SCORE_THRESHOLD = 0.70
CLUSTER_THRESHOLD = 0.95

# ───────────── LOAD MODELS ─────────────
print("\U0001F4E6 Loading model, FAISS index, and metadata...")
model = SentenceTransformer(MODEL_PATH)
index = faiss.read_index(INDEX_PATH)
metadata_df = pd.read_csv(METADATA_CSV, dtype={2: str}, low_memory=False)
texts = metadata_df['combined_text'].tolist()
ids = metadata_df['resolved_id'].tolist()
sources = metadata_df['source'].tolist()

# ───────────── LOAD QUERIES ─────────────
print("\U0001F4E5 Loading disease names from provenance and additional list...")
prov_df = pd.read_csv(PROVENANCE_FILE, dtype=str)
prov_df.rename(columns={
    "nodenorm_Nodenorm_name": "nodenorm_preferred_label",
    "medgen_Preferred_Name": "medgen_preferred_label",
    "orphanet_Disease_Name": "orphanet_preferred_label"
}, inplace=True)

query_cols = [col for col in prov_df.columns if col.endswith("preferred_label")]
prov_df['query_text'] = prov_df[query_cols].fillna('').agg('|'.join, axis=1)
prov_queries = prov_df['query_text'].dropna().unique().tolist()

extra_queries = []
if os.path.exists(ADDITIONAL_DISEASE_LIST):
    extra_df = pd.read_csv(ADDITIONAL_DISEASE_LIST, dtype=str)
    if 'GARD_preferred_label' in extra_df.columns:
        extra_queries = extra_df['GARD_preferred_label'].dropna().unique().tolist()

all_queries = list(set(prov_queries + extra_queries))

# ───────────── FILTER OUT ALREADY-QUERIED TERMS ─────────────
if os.path.exists(OUTPUT_TSV):
    existing_results = pd.read_csv(OUTPUT_TSV, sep='\t')
    already_queried = set(existing_results['query'])
    all_queries = [q for q in all_queries if q not in already_queried]
    print(f"⚠️ Skipping {len(already_queried)} already-queried terms")

# ───────────── INFERENCE ─────────────
print(f"🔢 Running inference for {len(all_queries)} unique disease names...")
all_results = []
top_matches = []

for query in tqdm(all_queries):
    emb = model.encode([query], convert_to_numpy=True)
    faiss.normalize_L2(emb)
    scores, indices = index.search(emb, TOP_K)

    top_match_recorded = False

    for i, (score, idx) in enumerate(zip(scores[0], indices[0])):
        if score < SCORE_THRESHOLD:
            continue
        row = {
            "query": query,
            "matched_text": texts[idx],
            "resolved_id": ids[idx],
            "source": sources[idx],
            "score": round(float(score), 4)
        }
        all_results.append(row)
        if not top_match_recorded:
            top_matches.append(row)
            top_match_recorded = True

# Append to existing result file if present
result_df = pd.DataFrame(all_results)
if os.path.exists(OUTPUT_TSV):
    existing_df = pd.read_csv(OUTPUT_TSV, sep='\t')
    result_df = pd.concat([existing_df, result_df], ignore_index=True)

result_df.to_csv(OUTPUT_TSV, sep='\t', index=False)
print(f"✅ Saved resolved mappings to {OUTPUT_TSV}")

# Save top matches only
pd.DataFrame(top_matches).to_csv(TOP_MATCH_TSV, sep='\t', index=False)
print(f"⭐ Saved top matches to {TOP_MATCH_TSV}")

# ───────────── OPTIONAL: CLUSTER EXTRACTION ─────────────
high_conf_df = result_df[result_df['score'] >= CLUSTER_THRESHOLD]
grouped = high_conf_df.groupby('query')

pivot_rows = []
for query, group in grouped:
    row = {"query": query}
    for _, r in group.iterrows():
        col_key = f"{r['source']}__{r['resolved_id']}"
        row[col_key] = r['matched_text']
    pivot_rows.append(row)

pivot_df = pd.DataFrame(pivot_rows)
pivot_df.to_csv(CLUSTERS_PIVOT_TSV, sep='\t', index=False)
print(f"\U0001F4CA Saved high-confidence clusters pivot table to {CLUSTERS_PIVOT_TSV}")
