#!/usr/bin/env python

import os
import yaml
import json
import logging
import argparse
import pandas as pd
from datetime import datetime

class StringPPITransformer:
    def __init__(self, config):
        self.cfg = config["ppi"]["string"]
        self.qc_mode = config.get("global", {}).get("qc_mode", False)
        self.metadata = {
            "timestamp": {"start": str(datetime.now())},
            "processing_steps": [],
            "outputs": []
        }
        logging.basicConfig(level=logging.INFO)

    def append_step(self, step, desc):
        self.metadata["processing_steps"].append({
            "step_name": step,
            "description": desc,
            "performed_at": str(datetime.now())
        })

    def run(self):
        logging.info("🔍 Loading STRING data...")
        df = pd.read_csv(self.cfg["raw_file"], sep=' ')
        df.columns = ['protein1', 'protein2'] + df.columns[2:].tolist()

        df['protein1'] = df['protein1'].str.replace("9606.", "")
        df['protein2'] = df['protein2'].str.replace("9606.", "")
        self.append_step("Prefix removal", "Stripped '9606.' from protein columns")

        df = df[df["combined_score"] > 200]
        self.append_step("Score filtering", "Filtered rows with combined_score > 200")

        logging.info("📦 Loading protein reference file...")
        df_protein = pd.read_csv(self.cfg["protein_reference"], sep="\t", dtype=str)
        df_protein = df_protein.dropna(subset=["consolidated_ensembl_protein_id", "ncats_protein_id"]).copy()

        # Split and remove version
        df_protein["consolidated_ensembl_protein_id"] = df_protein["consolidated_ensembl_protein_id"].str.split("|")
        df_exploded = df_protein.explode("consolidated_ensembl_protein_id").copy()
        df_exploded["consolidated_ensembl_protein_id"] = df_exploded["consolidated_ensembl_protein_id"].str.replace(r"\.\d+$", "", regex=True)

        id_map = df_exploded.set_index("consolidated_ensembl_protein_id")["ncats_protein_id"].to_dict()
        logging.info(f"🔗 Built Ensembl-to-NCATS protein map with {len(id_map)} entries")
        self.append_step("ID mapping", "Exploded and version-stripped protein reference and built mapping")

        # Map STRING IDs to NCATS IDs
        df['protein1_mapped'] = df['protein1'].map(id_map)
        df['protein2_mapped'] = df['protein2'].map(id_map)

        # Save unmapped rows if QC mode is on
        if self.qc_mode:
            unmapped = df[df['protein1_mapped'].isna() | df['protein2_mapped'].isna()]
            os.makedirs(os.path.dirname(self.cfg["unmapped_output"]), exist_ok=True)
            unmapped.to_csv(self.cfg["unmapped_output"], index=False)
            self.append_step("Unmapped export", f"Saved {len(unmapped)} unmapped entries to {self.cfg['unmapped_output']}")

        # Filter to mapped rows only
        df = df.dropna(subset=['protein1_mapped', 'protein2_mapped'])
        df['protein1'] = df['protein1_mapped']
        df['protein2'] = df['protein2_mapped']
        df = df.drop(columns=['protein1_mapped', 'protein2_mapped'])

        # Chunked output
        os.makedirs(os.path.dirname(self.cfg["cleaned_prefix"]), exist_ok=True)
        chunk_size = len(df) // 5
        for i in range(5):
            start, end = i * chunk_size, (i + 1) * chunk_size if i < 4 else len(df)
            part_path = f"{self.cfg['cleaned_prefix']}_part{i+1}.csv"
            df.iloc[start:end].to_csv(part_path, index=False)
            self.metadata["outputs"].append(part_path)
            logging.info(f"✅ Saved chunk {i+1}: {part_path}")

        self.metadata["timestamp"]["end"] = str(datetime.now())
        with open(self.cfg["metadata_file"], "w") as f:
            json.dump(self.metadata, f, indent=4)
        logging.info(f"🧾 Metadata written to: {self.cfg['metadata_file']}")

def main(config_path):
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    transformer = StringPPITransformer(cfg)
    transformer.run()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="YAML config path")
    args = parser.parse_args()
    main(args.config)
