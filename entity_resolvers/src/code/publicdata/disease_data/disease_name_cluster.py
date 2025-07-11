#!/usr/bin/env python
"""
disease_name_cluster.py - Cluster disease names by sentence similarity to flag missed mappings
"""
import os
import json
import logging
import pandas as pd
from datetime import datetime
from sentence_transformers import SentenceTransformer
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import argparse
import yaml
from tqdm import tqdm

class DiseaseNameClusterer:
    def __init__(self, config):
        self.config = config['disease_name_cluster']
        self.input_file = self.config['input_file']
        self.output_file = self.config['output_file']
        self.metadata_file = self.config['metadata_file']
        self.qc_mode = config.get('global', {}).get('qc_mode', True)
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        logging.info(f"QC Mode: {self.qc_mode}")
        self.metadata = {
            "timestamp": {"start": str(datetime.now())},
            "input_file": self.input_file,
            "output_file": self.output_file,
            "steps": []
        }

    def normalize(self, s):
        return str(s).lower().strip().replace('\u2019', "'").replace('"', '').replace('\u00a0', ' ')

    def run(self):
        logging.info("📥 Loading merged disease file for name clustering...")
        df = pd.read_csv(self.input_file, dtype=str)

        name_cols = [
            'mondo_preferred_label',
            'doid_preferred_label',
            'medgen_Preferred_Name',
            'omim_preferred_label',
            'orphanet_Disease_Name',
            'nodenorm_Nodenorm_name'
        ]

        logging.info(f"🔍 Extracting and normalizing disease names from: {name_cols}")
        name_records = []
        for col in name_cols:
            if col not in df.columns:
                logging.warning(f"⚠️ Skipping missing column: {col}")
                continue
            name_records.extend([(self.normalize(val), col) for val in df[col].dropna().unique()])

        name_df = pd.DataFrame(name_records, columns=['name', 'source'])
        name_df = name_df.drop_duplicates().reset_index(drop=True)

        logging.info(f"🧠 Computing embeddings for {len(name_df)} unique names...")
        embeddings = self.model.encode(name_df['name'].tolist(), convert_to_tensor=True).cpu().numpy()

        logging.info("🔗 Running Agglomerative Clustering...")
        clustering = AgglomerativeClustering(n_clusters=None, distance_threshold=0.1, metric='cosine', linkage='average')
        name_df['cluster'] = clustering.fit_predict(embeddings)

        # Log stats about the clustering output
        cluster_sizes = name_df['cluster'].value_counts()
        logging.info("🔢 Cluster count: %d | Min size: %d | Max size: %d | Median size: %d",
                    cluster_sizes.size, cluster_sizes.min(), cluster_sizes.max(), cluster_sizes.median())

        logging.info("📊 Calculating average similarity for each of %d clusters...", name_df['cluster'].nunique())
        similarity_scores = []
        grouped_names = name_df.groupby('cluster')['name'].apply(list)

        for i, names in enumerate(tqdm(grouped_names, disable=not self.qc_mode)):
            emb = self.model.encode(names, convert_to_tensor=True).cpu().numpy()
            sim_matrix = cosine_similarity(emb)
            avg_sim = np.mean(sim_matrix[np.triu_indices(len(sim_matrix), 1)]) if len(sim_matrix) > 1 else 1.0
            similarity_scores.append(round(float(avg_sim), 4))
            if i % 100 == 0:
                logging.info("🧪 Processed %d clusters...", i)

        cluster_df = name_df.groupby('cluster').agg({
            'name': lambda x: '|'.join(sorted(set(x))),
            'source': lambda x: '|'.join(sorted(set(x)))
        }).reset_index()
        cluster_df['similarity_score'] = similarity_scores
        cluster_df = cluster_df.sort_values(by='similarity_score', ascending=False)

        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        cluster_df.to_csv(self.output_file, sep='\t', index=False)
        logging.info(f"✅ Clustered disease names saved to {self.output_file}")

        self.metadata['timestamp']['end'] = str(datetime.now())
        self.metadata['output_records'] = len(cluster_df)
        os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
        with open(self.metadata_file, 'w') as f:
            json.dump(self.metadata, f, indent=2)
        logging.info(f"📝 Metadata saved to {self.metadata_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    clusterer = DiseaseNameClusterer(config)
    clusterer.run()
