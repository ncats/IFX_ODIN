#!/usr/bin/env python
import os
import json
import logging
import pandas as pd
from datetime import datetime
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import AgglomerativeClustering
import yaml
import argparse
import numpy as np
from collections import defaultdict
from tqdm import tqdm
import matplotlib.pyplot as plt
from upsetplot import UpSet, from_memberships
import warnings

def setup_logging(log_file):
    if not log_file:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
        return
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
    )

class PathwayMergerTransformer:
    def __init__(self, config):
        logging.info("🚀 Initializing PathwayMergerTransformer")
        self.config = config['pathways_merge']
        self.qc_mode = config.get('global', {}).get('qc_mode', True)
        logging.info(f"QC Mode: {self.qc_mode}")
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        self.metadata = {
            "timestamp": {"start": str(datetime.now())},
            "processing_steps": [],
            "data_sources": [],
            "outputs": []
        }

    def load_and_clean(self):
        logging.info("STEP 1: Load and normalize pathway names from all sources")
        reactome = pd.read_csv(self.config['reactome_file'], dtype=str)[['reactome_id', 'pathway_name']].dropna()
        panther = pd.read_csv(self.config['panther_file'], dtype=str)[['pathway_accession', 'pathway_name']].dropna()
        wiki = pd.read_csv(self.config['wikipathways_file'], sep='\t', dtype=str)[['id', 'name']].dropna()

        reactome = reactome.rename(columns={'reactome_id': 'source_id', 'pathway_name': 'name'})
        panther = panther.rename(columns={'pathway_accession': 'source_id', 'pathway_name': 'name'})
        wiki = wiki.rename(columns={'id': 'source_id', 'name': 'name'})

        reactome['source'] = 'Reactome'
        panther['source'] = 'Panther'
        wiki['source'] = 'WikiPathway'

        all_df = pd.concat([reactome, panther, wiki], ignore_index=True)
        def normalize_text(s):
            return (
                s.strip().lower()
                .replace('–', '-')
                .replace('—', '-')
                .replace('\u00a0', ' ')
                .replace('\u200b', '')
                .replace('\u2013', '-')
                .replace('\u2014', '-')
                .replace('\u2019', "'")
                .replace('\u201c', '"').replace('\u201d', '"')
                .replace('\xa0', ' ')
                .strip()
            )
        all_df['name'] = all_df['name'].fillna('').astype(str).apply(normalize_text)

        return all_df

    def generate_upset_plot(self, final_df):
        logging.info("📊 Generating UpSet plot for pathway source overlaps...")

        memberships = []
        for _, row in final_df.iterrows():
            sources = []
            if pd.notna(row['Reactome']):
                sources.append("Reactome")
            if pd.notna(row['WikiPathway']):
                sources.append("WikiPathway")
            if pd.notna(row['Panther']):
                sources.append("Panther")
            memberships.append(sources)

        plot_path = os.path.join(
            os.path.dirname(self.config['output_file']),
            "pathway_upset_plot.png"
        )

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=FutureWarning)
            data = from_memberships(memberships)
            plt.figure(figsize=(8, 6))
            upset = UpSet(data, subset_size='count', show_counts=True)
            upset.plot()
            plt.suptitle("Pathway ID Overlaps Across Sources")
            plt.savefig(plot_path)
            plt.close()

        logging.info(f"📈 UpSet plot saved to {plot_path}")

    def run(self):
        df = self.load_and_clean()
        logging.info("STEP 2: Compute embeddings, cluster similar names, and generate pivot table")
        logging.info("🔄 Computing sentence embeddings for all pathway names...")
        embeddings = self.model.encode(df['name'].tolist(), convert_to_tensor=True, show_progress_bar=False).cpu().numpy()

        logging.info("🧠 Performing agglomerative clustering on embeddings...")
        clustering = AgglomerativeClustering(n_clusters=None, metric='cosine', distance_threshold=0.1, linkage='average')
        cluster_labels = clustering.fit_predict(embeddings)

        df['cluster'] = cluster_labels

        grouped = df.groupby(['cluster', 'source']).agg({
            'source_id': lambda x: '|'.join(sorted(set(x))),
            'name': lambda x: '|'.join(sorted(set(x)))
        }).unstack().reset_index()
        grouped.columns = ['_'.join(col).strip('_') for col in grouped.columns.values]

        cluster_names = df.groupby('cluster')['name'].apply(list)
        similarity_scores = []
        for names in tqdm(cluster_names, desc="🔍 Calculating average similarity per cluster", disable=not self.qc_mode):
            emb = self.model.encode(names, convert_to_tensor=True, show_progress_bar=False).cpu().numpy()
            sim_matrix = cosine_similarity(emb)
            avg_score = np.mean(sim_matrix[np.triu_indices(len(sim_matrix), 1)]) if len(sim_matrix) > 1 else 1.0
            similarity_scores.append(round(float(avg_score), 4))

        grouped['similarity_score'] = similarity_scores
        final_df = grouped.rename(columns={
            'source_id_Reactome': 'Reactome',
            'name_Reactome': 'Reactome_name',
            'source_id_WikiPathway': 'WikiPathway',
            'name_WikiPathway': 'WikiPathway_name',
            'source_id_Panther': 'Panther',
            'name_Panther': 'Panther_name'
        })

        # ⏱ Accurate match/unmatch summary by comparing clustered IDs
        matched_ids_by_source = {src: set() for src in ['Reactome', 'WikiPathway', 'Panther']}

        for src in matched_ids_by_source:
            if src in final_df.columns:
                matched_ids_by_source[src] = (
                    final_df[src].dropna().str.split('|').explode().str.strip().dropna().unique().tolist()
                )

        summary = {}
        for src in ['Reactome', 'WikiPathway', 'Panther']:
            all_ids = set(df[df['source'] == src]['source_id'])
            matched_ids = set(matched_ids_by_source.get(src, []))
            unmatched_ids = all_ids - matched_ids
            summary[src] = {
                "total": len(all_ids),
                "matched": len(matched_ids),
                "unmatched": len(unmatched_ids)
            }

        for src, stats in summary.items():
            logging.info(f"🔍 {src} — Total: {stats['total']}, Matched: {stats['matched']}, Unmatched: {stats['unmatched']}")

        output_path = self.config['output_file'].replace("pivoted_pathway_clusters", "pathway_provenance")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        final_df.to_csv(output_path, sep='\t', index=False)
        self.generate_upset_plot(final_df)
        self.metadata['outputs'].append({
            'name': 'pathway_provenance',
            'records': len(final_df),
            'generated_at': str(datetime.now()),
            'path': output_path
        })

        metadata_path = self.config['metadata_file']
        self.metadata['timestamp']['end'] = str(datetime.now())
        os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
        with open(metadata_path, 'w') as f:
            json.dump(self.metadata, f, indent=2)

        logging.info(f"✅ Finished. Output written to {output_path} and metadata to {metadata_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cluster pathway names using embedding similarity")
    parser.add_argument("--config", type=str, default="config/pathways_config.yaml",
                        help="Path to YAML config file")
    args = parser.parse_args()

    with open(args.config) as f:
        full_cfg = yaml.safe_load(f)

    setup_logging(full_cfg.get('pathways_merge', {}).get('log_file', ""))
    merger = PathwayMergerTransformer(full_cfg)
    merger.run()
