#!/usr/bin/env python
"""
disease_index_builder.py - Incrementally build a disease name FAISS index by source (NodeNorm → MONDO → DOID → ...)
"""
import os
import logging
import pandas as pd
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer

# ───────────── CONFIG ─────────────
INPUT_FILE = "src/data/publicdata/disease_data/cleaned/sources/disease_mapping_provenance.csv"
OUTPUT_DIR = "src/data/publicdata/disease_data/index"
MODEL_NAME = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
SOURCES = [
    ("nodenorm", ["nodenorm_Nodenorm_name"], "NodeNorm_id"),
    ("mondo", ["mondo_preferred_label", "mondo_synonyms"], "mondo_id"),
    ("doid", ["doid_preferred_label", "doid_synonyms"], "doid_DOID"),
    ("omim", ["omim_preferred_label"], "omim_OMIM"),
    ("orphanet", ["orphanet_Disease_Name"], "orphanet_Orphanet_ID"),
    ("medgen", ["medgen_Preferred_Name"], "medgen_MedGen")
]

os.makedirs(OUTPUT_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ───────────── LOAD MODEL ─────────────
logging.info(f"🧠 Loading model: {MODEL_NAME}")
model = SentenceTransformer(MODEL_NAME)

# ───────────── LOAD DATA ─────────────
logging.info("📥 Loading provenance file...")
df = pd.read_csv(INPUT_FILE, dtype=str)
all_embeddings = []
all_ids = []
all_texts = []
all_sources = []

used_ids = set()

for source, text_cols, id_col in SOURCES:
    logging.info(f"🔍 Processing source: {source.upper()} using ID column: {id_col}")

    if id_col not in df.columns:
        logging.warning(f"⚠️ Skipping {source} — ID column missing: {id_col}")
        continue

    sub = df[df[id_col].notna()].copy()
    sub = sub[sub[text_cols].fillna('').agg(' '.join, axis=1).str.strip().astype(bool)]

    # Avoid re-using IDs already embedded
    sub = sub[~sub[id_col].isin(used_ids)]
    used_ids.update(sub[id_col])

    # Build combined text
    sub['combined_text'] = sub[text_cols].fillna('').agg(' '.join, axis=1).str.lower().str.strip()
    sub = sub.drop_duplicates(subset=['combined_text', id_col])

    logging.info(f"🧪 Encoding {len(sub)} rows for {source}...")
    emb = model.encode(sub['combined_text'].tolist(), convert_to_numpy=True, show_progress_bar=True)
    faiss.normalize_L2(emb)

    all_embeddings.append(emb)
    all_ids.extend(sub[id_col].tolist())
    all_texts.extend(sub['combined_text'].tolist())
    all_sources.extend([source] * len(sub))

# ───────────── BUILD INDEX ─────────────
logging.info("🏗️ Building final FAISS index...")
all_embeddings = np.vstack(all_embeddings)
index = faiss.IndexFlatIP(EMBEDDING_DIM)
index.add(all_embeddings)
logging.info(f"✅ Added {index.ntotal} total entries")

# ───────────── SAVE ─────────────
index_path = os.path.join(OUTPUT_DIR, "layered_disease_index.faiss")
meta_path = os.path.join(OUTPUT_DIR, "layered_disease_index_metadata.csv")
model_save_path = os.path.join(OUTPUT_DIR, "sentence_model")

faiss.write_index(index, index_path)
pd.DataFrame({
    'source': all_sources,
    'combined_text': all_texts,
    'resolved_id': all_ids
}).to_csv(meta_path, index=False)
model.save(model_save_path)

logging.info(f"📦 Saved layered FAISS index to {index_path}")
logging.info(f"📄 Saved metadata CSV to {meta_path}")
logging.info(f"💾 Saved sentence transformer model to {model_save_path}")
