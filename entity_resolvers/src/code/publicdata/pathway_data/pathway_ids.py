#!/usr/bin/env python
import os
import json
import logging
import pandas as pd
from datetime import datetime
import argparse
import yaml
import secrets

def setup_logging(log_file):
    if not log_file:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
        return

    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

class PathwayIDGenerator:
    def __init__(self, config):
        logging.info("🚀 Initializing PathwayIDGenerator")
        self.config = config['pathways_ids']

        self.input_file = self.config['input_file']
        self.output_file = self.config['output_file']
        self.metadata_file = self.config['metadata_file']
        self.id_map_path = self.config.get("id_map_file", "cache/pathway_id_map.json")

        self.metadata = {
            "timestamp": {"start": str(datetime.now())},
            "processing_steps": [],
            "records": 0,
            "path": self.output_file
        }

    def assign_ifx_ids(self):
        logging.info("STEP 1: assign_ifx_ids")
        df = pd.read_csv(self.input_file, dtype=str, sep='\t')

        # Generate provenance key
        provenance_cols = ['Reactome', 'WikiPathway', 'Panther']
        df['provenance_key'] = df[provenance_cols].fillna('').agg('|'.join, axis=1)

        # 🚨 Drop fuzzy duplicates if similarity score is available
        if 'similarity_score' in df.columns:
            before = len(df)
            df.sort_values(by='similarity_score', ascending=False, na_position='last', inplace=True)
            df = df.drop_duplicates(subset='provenance_key', keep='first')
            after = len(df)
            logging.info(f"🧹 Removed {before - after} fuzzy duplicate rows")

        # Load previous ID map
        os.makedirs(os.path.dirname(self.id_map_path), exist_ok=True)
        if os.path.exists(self.id_map_path):
            with open(self.id_map_path) as f:
                id_map = json.load(f)
            logging.info(f"🧠 Loaded {len(id_map)} prior ID mappings")
            original_id_map = dict(id_map)
        else:
            id_map = {}
            original_id_map = {}

        # Mint and log new IFX IDs
        def mint_ifx():
            return f"IFXPathway:{''.join(secrets.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789') for _ in range(7))}"

        new_ids = {}
        for key in df['provenance_key'].unique():
            if key not in id_map:
                new_id = mint_ifx()
                id_map[key] = new_id
                new_ids[key] = new_id
                logging.info(f"🆕 Minted new IFX ID: {new_id} for provenance key: {key}")

        # Save updated map and .diff
        with open(self.id_map_path, 'w') as f:
            json.dump(id_map, f, indent=2)
        if new_ids:
            diff_path = self.id_map_path.replace('.json', '.diff.json')
            with open(diff_path, 'w') as f:
                json.dump(new_ids, f, indent=2)
            logging.info(f"📄 Wrote {len(new_ids)} new ID mappings to diff file: {diff_path}")
        else:
            logging.info("✅ No new IFX IDs were minted")

        # Map IDs and clean up
        df['ncats_pathway_id'] = df['provenance_key'].map(id_map)
        df.drop(columns=['provenance_key'], inplace=True)

        # Consolidate pathway names
        df['consolidated_pathway_name'] = df[['WikiPathway_name', 'Reactome_name', 'Panther_name']].bfill(axis=1).iloc[:, 0]
        logging.info("✅ Consolidated pathway names added from available sources")

        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        df.to_csv(self.output_file, index=False, sep='\t')

        self.metadata['records'] = len(df)
        self.metadata['timestamp']['end'] = str(datetime.now())
        with open(self.metadata_file, 'w') as f:
            json.dump(self.metadata, f, indent=2)
        logging.info(f"✅ IFX Pathway IDs saved to {self.output_file} and metadata to {self.metadata_file}")

    def run(self):
        self.assign_ifx_ids()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Assign IFX Pathway IDs to harmonized data")
    parser.add_argument("--config", type=str, required=True, help="Path to YAML config file")
    args = parser.parse_args()

    with open(args.config) as f:
        full_cfg = yaml.safe_load(f)

    setup_logging(full_cfg.get('pathways_ids', {}).get('log_file', ""))
    generator = PathwayIDGenerator(full_cfg)
    generator.run()
