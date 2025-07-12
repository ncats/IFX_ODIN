#!/usr/bin/env python

# semantic_disease_resolver.py - Query disease names from provenance and resolve cross-source mappings using FAISS

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
EXCLUDE_GARD = True

# ───────────── LOAD MODELS ─────────────
print("📦 Loading model, FAISS index, and metadata...")
model = SentenceTransformer(MODEL_PATH)
index = faiss.read_index(INDEX_PATH)
metadata_df = pd.read_csv(METADATA_CSV, dtype=str)
texts = metadata_df['combined_text'].tolist()
sources = metadata_df['source'].tolist()

# Dynamically get the resolved ID per row
def get_resolved_id(row):
    col = f"{row['source']}_id"
    return row.get(col)

ids = [get_resolved_id(r) for _, r in metadata_df.iterrows()]

# ───────────── LOAD QUERIES ─────────────
print("📥 Loading disease names from provenance and additional list...")
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
if not EXCLUDE_GARD and os.path.exists(ADDITIONAL_DISEASE_LIST):
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
        if idx == -1 or score < SCORE_THRESHOLD:
            continue
        matched_id = ids[idx]
        matched_text = texts[idx]
        matched_source = sources[idx]
        row = {
            "query": query,
            "matched_text": matched_text,
            "resolved_id": matched_id,
            "source": matched_source,
            "score": round(float(score), 4)
        }
        all_results.append(row)
        if not top_match_recorded:
            top_matches.append(row)
            top_match_recorded = True

# ───────────── SAVE RESULTS ─────────────
result_df = pd.DataFrame(all_results)
if os.path.exists(OUTPUT_TSV):
    existing_df = pd.read_csv(OUTPUT_TSV, sep='\t')
    result_df = pd.concat([existing_df, result_df], ignore_index=True)

result_df.to_csv(OUTPUT_TSV, sep='\t', index=False)
print(f"✅ Saved resolved mappings to {OUTPUT_TSV}")

pd.DataFrame(top_matches).to_csv(TOP_MATCH_TSV, sep='\t', index=False)
print(f"⭐ Saved top matches to {TOP_MATCH_TSV}")

# ───────────── BUILD PIVOT CLUSTER TABLE ─────────────
print("🧮 Building pivot table for high-confidence clusters...")
high_conf_df = result_df[result_df['score'] >= CLUSTER_THRESHOLD]
grouped = high_conf_df.groupby('query')

pivot_rows = []
for query, group in grouped:
    row = {"query": query}
    for _, r in group.iterrows():
        src = r['source']
        row[f"{src}_id"] = r['resolved_id']
        row[f"{src}_disease_name"] = r['matched_text']
        row[f"{src}_score"] = r['score']
    pivot_rows.append(row)

pivot_df = pd.DataFrame(pivot_rows)
pivot_df.to_csv(CLUSTERS_PIVOT_TSV, sep='\t', index=False)
print(f"📊 Saved high-confidence clusters pivot table to {CLUSTERS_PIVOT_TSV}")
