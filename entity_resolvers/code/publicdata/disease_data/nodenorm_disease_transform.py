#!/usr/bin/env python
"""
nodenorm_disease_transform.py - Fetch and transform NodeNorm Disease.txt data

This script:
  1. Downloads the Disease.txt file from NodeNorm.
  2. Parses line-delimited JSON into a structured DataFrame.
  3. Cleans and reformats identifiers (e.g., KEGG, UMLS, OMIM).
  4. Writes the cleaned output and metadata for tracking.
"""

import os
import json
import yaml
import logging
import argparse
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode="a"),
            logging.StreamHandler()
        ],
        force=True
    )

class NodeNormDiseaseTransformer:
    def __init__(self, full_config):
        self.cfg = full_config["nodenorm"]
        self.input_url = self.cfg["url_base"].rstrip("/") + "/2025jan23/compendia/Disease.txt"
        self.output_file = Path(self.cfg["cleaned_output"])
        self.metadata_file = Path(self.cfg["transform_metadata_file"])
        self.log_file = Path(self.cfg["log_file"])
        self.qc_mode = self.cfg.get("qc_mode", full_config.get("global", {}).get("qc_mode", True))

        os.makedirs(self.output_file.parent, exist_ok=True)
        os.makedirs(self.metadata_file.parent, exist_ok=True)
        setup_logging(self.log_file)

    def fetch_data(self):
        logging.info(f"📥 Fetching: {self.input_url}")
        r = requests.get(self.input_url)
        r.raise_for_status()
        return r.text.splitlines()

    def parse_lines(self, lines):
        logging.info("🧪 Parsing lines...")
        parsed_data = []

        for entry_id, line in enumerate(lines, start=1):
            entry = json.loads(line.strip())
            row_data = {
                "NodeNorm_id": entry_id,
                "biolinkType": entry.get("type"),
                "Nodenorm_name": entry.get("preferred_name"),
            }

            for ident in entry.get("identifiers", []):
                code = ident.get("i")
                if not code:
                    continue
                prefix = code.split(":")[0]
                row_data[prefix] = code

            parsed_data.append(row_data)

        return pd.DataFrame(parsed_data)

    def clean_df(self, df):
        # Drop unwanted columns
        df.drop(columns=[col for col in ["IC", "Taxa"] if col in df.columns], errors="ignore", inplace=True)

        # Normalize values
        replacements = {
            "KEGG": ("KEGG.DISEASE:", "KEGG:"),
            "UMLS": ("UMLS:", ""),
            "medgen": ("medgen:", "MEDGEN:"),
            "OMIM.PS": ("OMIM.PS:", "OMIMPS:"),
            "orphanet": ("orphanet:", "Orphanet:"),
            "ICD10": ("ICD10:", "ICD10CM:")
        }

        for col, (old, new) in replacements.items():
            if col in df.columns:
                df[col] = df[col].apply(lambda x: x.replace(old, new) if isinstance(x, str) else x)

        # Rename and suffix columns
        rename_columns = {
            "OMIM.PS": "OMIMPS",
            "KEGG.DISEASE": "KEGG"
        }
        df.rename(columns=rename_columns, inplace=True)
        # Apply norm_ prefix to all columns except whitelist
        prefix_exclude = ["NodeNorm_id", "biolinkType"]
        df.rename(columns={col: f"nodenorm_{col}" for col in df.columns if col not in prefix_exclude}, inplace=True)
        return df

    def transform(self):
        lines = self.fetch_data()
        df = self.parse_lines(lines)
        df = self.clean_df(df)

        logging.info(f"📝 Saving cleaned output to: {self.output_file}")
        df.to_csv(self.output_file, index=False)

        metadata = {
            "timestamp": datetime.now().isoformat(),
            "source_url": self.input_url,
            "output_file": str(self.output_file),
            "num_records": len(df)
        }
        with open(self.metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

        logging.info(f"📊 Metadata written to: {self.metadata_file}")
        logging.info("✅ NodeNorm Disease transformation complete.")

    def run(self):
        self.transform()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform NodeNorm Disease.txt into cleaned CSV")
    parser.add_argument("--config", required=True, help="YAML configuration file path")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    NodeNormDiseaseTransformer(config).run()
