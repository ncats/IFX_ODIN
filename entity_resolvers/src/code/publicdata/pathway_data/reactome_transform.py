#!/usr/bin/env python
"""
reactome_transform.py - Transform Reactome pathway data for Homo sapiens
  • Filters for Homo sapiens entries
  • Outputs harmonized TSVs
  • Logs metadata, processing steps, diffs
"""

import os
import json
import yaml
import logging
import pandas as pd
from datetime import datetime

def setup_logging(log_file):
    handlers = [logging.StreamHandler()]
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        handlers.insert(0, logging.FileHandler(log_file, mode='a'))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
        force=True
    )

class ReactomeTransformer:
    def __init__(self, config):
        self.cfg = config["pathways"]["reactome"]
        setup_logging(self.cfg.get("log_file"))
        self.metadata = {
            "timestamp": {"start": str(datetime.now())},
            "data_sources": [],
            "processing_steps": [],
            "outputs": []
        }

    def _read_and_filter(self, raw_path, columns, species_column):
        logging.info(f"📥 Reading {raw_path}")
        df = pd.read_table(raw_path, header=None)
        df.columns = columns
        df = df[df[species_column] == "Homo sapiens"].copy()
        logging.info(f"✅ {len(df)} Homo sapiens rows")
        return df

    def run(self):
        for key, entry in self.cfg["files"].items():
            df = self._read_and_filter(entry["raw_path"], entry["columns"], entry["species_column"])

            os.makedirs(os.path.dirname(entry["csv_path"]), exist_ok=True)
            df.to_csv(entry["csv_path"], index=False)
            logging.info(f"💾 Saved {len(df)} rows to {entry['csv_path']}")

            self.metadata["data_sources"].append({
                "name": key,
                "path": entry["raw_path"],
                "host": "reactome.org",
                "description": "Reactome data",
                "accessed_at": str(datetime.now())
            })
            self.metadata["processing_steps"].append({
                "step_name": f"transform_{key}",
                "description": f"Filtered {key} to Homo sapiens",
                "performed_at": str(datetime.now())
            })
            self.metadata["outputs"].append({
                "name": os.path.basename(entry["csv_path"]),
                "path": entry["csv_path"],
                "generated_at": str(datetime.now())
            })

        self.metadata["timestamp"]["end"] = str(datetime.now())
        with open(self.cfg["transform_metadata_file"], "w") as f:
            json.dump(self.metadata, f, indent=2)
        logging.info(f"📝 Metadata saved to {self.cfg['transform_metadata_file']}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    ReactomeTransformer(config).run()
