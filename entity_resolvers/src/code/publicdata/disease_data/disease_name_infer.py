#!/usr/bin/env python
"""
disease_name_infer.py - Modular disease name resolver using FAISS + sentence embeddings.
Supports single or list input for integration with GUI or API.
"""

import os
import faiss
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer

# ───────────── CONFIG ─────────────
INDEX_PATH = "src/data/publicdata/disease_data/index/layered_disease_index.faiss"
METADATA_CSV = "src/data/publicdata/disease_data/index/layered_disease_index_metadata.csv"
MODEL_PATH = "src/data/publicdata/disease_data/index/sentence_model"
TOP_K = 25
SCORE_THRESHOLD = 0.70

# ───────────── LOAD MODEL AND INDEX ─────────────
print("📦 Loading model, FAISS index, and metadata...")
model = SentenceTransformer(MODEL_PATH)
index = faiss.read_index(INDEX_PATH)
metadata_df = pd.read_csv(METADATA_CSV)

texts = metadata_df['combined_text'].tolist()
ids = metadata_df['resolved_id'].tolist()
sources = metadata_df['source'].tolist()

# ───────────── SINGLE QUERY FUNCTION ─────────────
def query_disease_name(name_query, top_k=TOP_K, score_threshold=SCORE_THRESHOLD) -> pd.DataFrame:
    """
    Query a single disease name and return a ranked DataFrame of matches.
    """
    if not name_query:
        return pd.DataFrame()

    emb = model.encode([name_query], convert_to_numpy=True)
    faiss.normalize_L2(emb)
    scores, indices = index.search(emb, top_k)

    results = []
    for score, idx in zip(scores[0], indices[0]):
        if score < score_threshold:
            continue
        results.append({
            "query": name_query,
            "source": sources[idx],
            "resolved_id": ids[idx],
            "score": round(float(score), 4),
            "matched_text": texts[idx]
        })

    return pd.DataFrame(results).sort_values(by='score', ascending=False)

# ───────────── BATCH QUERY FUNCTION ─────────────
def query_disease_list(name_list, top_k=TOP_K, score_threshold=SCORE_THRESHOLD) -> pd.DataFrame:
    """
    Query a list of disease names and return a combined DataFrame of matches.
    """
    all_results = []
    for name in name_list:
        df = query_disease_name(name, top_k=top_k, score_threshold=score_threshold)
        all_results.append(df)
    return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()
