#!/usr/bin/env python
"""
wikipathways_transform.py - Transform WikiPathways GMT to CSV
  • Converts latest Homo sapiens GMT file to long-form CSV
  • Tracks metadata
  • Logs progress
"""

import os
import json
import logging
from datetime import datetime
import pandas as pd
import re

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

class WikiPathwaysTransformer:
    def __init__(self, config):
        self.cfg = config["pathways"]["wikipathways"]
        setup_logging(self.cfg.get("log_file"))
        self.metadata = {
            "timestamp": {"start": str(datetime.now())},
            "data_sources": [],
            "processing_steps": [],
            "outputs": []
        }

    def gmt_to_dataframe(self, gmt_path):
        logging.info(f"🔄 Converting {gmt_path} to DataFrame")
        rows = []
        with open(gmt_path, 'r') as file:
            for line in file:
                parts = line.strip().split('\t')
                meta_info = parts[0]
                wpid_url = parts[1]
                genes = parts[2:]

                # Extract readable name and WP ID
                match = re.match(r"(.*?)%.*?(WP\d+)%Homo sapiens", meta_info)
                if match:
                    name, wpid = match.groups()
                else:
                    name, wpid = meta_info, "NA"

                for gene in genes:
                    rows.append([name, wpid, gene])

        df = pd.DataFrame(rows, columns=["Pathway", "WPID", "Gene"])
        return df

    def json_to_dataframe(self, json_path):
        logging.info(f"🔄 Converting JSON: {json_path}")
        with open(json_path, 'r') as f:
            data = json.load(f)
        records = data.get('pathways', [])
        df = pd.DataFrame(records)
        return df

    def run(self):
        for key, entry in self.cfg["files"].items():
            input_path = entry.get("raw_path") or entry.get("latest_local_copy")
            output_path = entry.get("csv_path")
            if not input_path or not output_path:
                logging.warning(f"⚠️ Skipping {key} due to missing input or output paths")
                continue

            if input_path.endswith(".gmt"):
                df = self.gmt_to_dataframe(input_path)
                transform_type = "GMT"
            elif input_path.endswith(".json"):
                df = self.json_to_dataframe(input_path)
                transform_type = "JSON"
            else:
                logging.warning(f"⚠️ Skipping unsupported file type: {input_path}")
                continue

            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_csv(output_path, sep="\t", index=False)
            logging.info(f"💾 Saved {len(df)} rows to {output_path}")

            self.metadata["data_sources"].append({
                "name": key,
                "path": input_path,
                "host": "data.wikipathways.org" if "wikipathways_latest_human.gmt" in input_path else "webservice.wikipathways.org",
                "description": entry.get("description", ""),
                "accessed_at": str(datetime.now())
            })
            self.metadata["processing_steps"].append({
                "step_name": f"transform_{key}",
                "description": f"Converted {transform_type} to TSV",
                "performed_at": str(datetime.now())
            })
            self.metadata["outputs"].append({
                "name": os.path.basename(output_path),
                "path": output_path,
                "generated_at": str(datetime.now())
            })

        self.metadata["timestamp"]["end"] = str(datetime.now())
        with open(self.cfg["transform_metadata_file"], "w") as f:
            json.dump(self.metadata, f, indent=2)
        logging.info(f"📝 Metadata saved to {self.cfg['transform_metadata_file']}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    with open(args.config) as f:
        config = yaml.safe_load(f)
    WikiPathwaysTransformer(config).run()
