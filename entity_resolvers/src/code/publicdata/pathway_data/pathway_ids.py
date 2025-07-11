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

        # 🚨 Drop duplicates: keep row with non-null similarity_score if exists
        if 'similarity_score' in df.columns:
            before = len(df)
            df.sort_values(by='similarity_score', ascending=False, na_position='last', inplace=True)
            df = df.drop_duplicates(subset='provenance_key', keep='first')
            after = len(df)
            logging.info(f"🧹 Removed {before - after} fuzzy duplicate rows")

        # Path to persistent ID map
        id_map_path = self.config.get("id_map_file", "cache/pathway_id_map.json")
        os.makedirs(os.path.dirname(id_map_path), exist_ok=True)

        if os.path.exists(id_map_path):
            with open(id_map_path) as f:
                id_map = json.load(f)
            logging.info(f"🧠 Loaded {len(id_map)} prior ID mappings")
        else:
            id_map = {}

        # Mint new IFX IDs for new provenance keys
        def mint_ifx():
            return f"IFXPathway:{''.join(secrets.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789') for _ in range(7))}"

        for key in df['provenance_key'].unique():
            if key not in id_map:
                id_map[key] = mint_ifx()

        df['ncats_pathway_id'] = df['provenance_key'].map(id_map)
        df.drop(columns=['provenance_key'], inplace=True)

        # Consolidate pathway names
        df['consolidated_pathway_name'] = df[['WikiPathway_name', 'Reactome_name', 'Panther_name']].bfill(axis=1).iloc[:, 0]
        logging.info("✅ Consolidated pathway names added from available sources")

        # Save final output and metadata
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
    generator.assign_ifx_ids()
