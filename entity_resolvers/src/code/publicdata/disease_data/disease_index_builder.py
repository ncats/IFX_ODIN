#!/usr/bin/env python
"""
disease_index_builder.py - Build a layered FAISS index from raw cleaned disease source files
"""

import os
import logging
import pandas as pd
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer

# ───────────── CONFIG ─────────────
OUTPUT_DIR = "src/data/publicdata/disease_data/index"
MODEL_NAME = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384

# Map source → (path, [columns to use for text], ID column)
SOURCE_CONFIG = {
    "nodenorm": (
        "src/data/publicdata/disease_data/cleaned/sources/nodenorm_disease.csv",
        ["nodenorm_Nodenorm_name"],
        "NodeNorm_id"
    ),
    "mondo": (
        "src/data/publicdata/disease_data/cleaned/sources/mondo_ids.csv",
        ["mondo_preferred_label", "mondo_synonyms"],
        "mondo_id"
    ),
    "doid": (
        "src/data/publicdata/disease_data/cleaned/sources/doid.csv",
        ["doid_preferred_label", "doid_synonyms"],
        "doid_DOID"
    ),
    "omim": (
        "src/data/publicdata/disease_data/cleaned/sources/OMIM_diseases.csv",
        ["omim_preferred_label", "omim_alternative_labels"],
        "omim_OMIM"
    ),
    "orphanet": (
        "src/data/publicdata/disease_data/cleaned/sources/orphanet_disease_ids.csv",
        ["orphanet_Disease_Name"],
        "orphanet_Orphanet_ID"
    ),
    "medgen": (
        "src/data/publicdata/disease_data/cleaned/sources/medgen_id_mappings.csv",
        ["medgen_Preferred_Name"],
        "medgen_MedGen"
    )
}

os.makedirs(OUTPUT_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ───────────── LOAD MODEL ─────────────
logging.info(f"🧠 Loading model: {MODEL_NAME}")
model = SentenceTransformer(MODEL_NAME)

# ───────────── BUILD INDEX FROM SOURCES ─────────────
all_embeddings = []
all_ids = []
all_texts = []
all_sources = []
used_ids = set()

for source, (file_path, text_cols, id_col) in SOURCE_CONFIG.items():
    if not os.path.exists(file_path):
        logging.warning(f"❌ Missing file for {source}: {file_path}")
        continue

    df = pd.read_csv(file_path, dtype=str)
    if id_col not in df.columns or all(col not in df.columns for col in text_cols):
        logging.warning(f"⚠️ Skipping {source} — missing required columns")
        continue

    df = df[df[id_col].notna()].copy()
    df['combined_text'] = df[text_cols].fillna('').agg(' '.join, axis=1).str.lower().str.strip()
    df = df[df['combined_text'].astype(bool)]  # Drop empty text
    df = df.drop_duplicates(subset=['combined_text', id_col])
    df = df[~df[id_col].isin(used_ids)]
    used_ids.update(df[id_col])

    logging.info(f"🧪 Encoding {len(df)} rows from {source}")
    emb = model.encode(df['combined_text'].tolist(), convert_to_numpy=True, show_progress_bar=True)
    faiss.normalize_L2(emb)

    all_embeddings.append(emb)
    all_ids.extend(df[id_col].tolist())
    all_texts.extend(df['combined_text'].tolist())
    all_sources.extend([source] * len(df))

# ───────────── FAISS INDEXING ─────────────
logging.info("🏗️ Building final FAISS index...")
all_embeddings = np.vstack(all_embeddings)
index = faiss.IndexFlatIP(EMBEDDING_DIM)
index.add(all_embeddings)
logging.info(f"✅ Added {index.ntotal} total entries")

# ───────────── SAVE OUTPUT ─────────────
logging.info("💾 Saving outputs...")
index_path = os.path.join(OUTPUT_DIR, "layered_disease_index.faiss")
meta_path = os.path.join(OUTPUT_DIR, "layered_disease_index_metadata.csv")
model_save_path = os.path.join(OUTPUT_DIR, "sentence_model")

# Build metadata with source-specific ID columns (e.g., doid_id, omim_id)
meta_records = []
for src, src_id, text in zip(all_sources, all_ids, all_texts):
    row = {
        "source": src,
        "combined_text": text,
        f"{src}_id": src_id
    }
    meta_records.append(row)

meta_df = pd.DataFrame(meta_records)

# Ensure all source-specific ID columns are present in consistent order
for s in SOURCE_CONFIG:
    col = f"{s}_id"
    if col not in meta_df.columns:
        meta_df[col] = None

# Reorder columns
ordered_cols = ['source', 'combined_text'] + [f"{s}_id" for s in SOURCE_CONFIG]
meta_df = meta_df[ordered_cols]

# Write outputs
faiss.write_index(index, index_path)
meta_df.to_csv(meta_path, index=False)
model.save(model_save_path)

logging.info(f"📦 Saved FAISS index to: {index_path}")
logging.info(f"📄 Saved metadata to: {meta_path}")
logging.info(f"💾 Saved model to: {model_save_path}")

