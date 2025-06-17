#!/usr/bin/env python

# medgen_transform.py - MedGen data parser and pivot transformer

import os
import pandas as pd
import argparse
import yaml
import logging
import json
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

class MedGenTransformer:
    def __init__(self, config):
        self.cfg = config["medgen"]
        self.metadata_path = Path(self.cfg["transform_metadata"])
        self.log_file = self.cfg["log_file"]
        setup_logging(self.log_file)

    def parse_and_clean(self, file_path, column_names):
        df = pd.read_csv(file_path, sep="|", dtype=str, header=None, engine="python")
        df.dropna(axis=1, how="all", inplace=True)

        if df.shape[1] != len(column_names):
            logging.warning(f"Expected {len(column_names)} columns but found {df.shape[1]} in {file_path}")
            raise ValueError("Column mismatch detected.")

        df.columns = column_names

        if "OMIM_ID" in df.columns:
            df["OMIM_ID"] = df["OMIM_ID"].str.replace("OMIM:", "OMIM_", regex=False)
        if "HPO_ID" in df.columns:
            df["HPO_ID"] = df["HPO_ID"].str.replace("HP:", "HPO_", regex=False)

        return df

    def apply_prefix(self, value, prefix):
        if pd.isna(value):
            return value
        return "|".join(
            prefix + v[len("Orphanet_"):] if v.startswith("Orphanet_") and prefix == "Orphanet:" else
            v if v.startswith(prefix) else prefix + v for v in value.split("|")
        )

    def pivot_and_save(self, df, output_file):
        df_pivoted = df.pivot_table(
            index=["CUI_or_CN_id", "Preferred_Name"],
            columns="Source",
            values="Source_ID",
            aggfunc=lambda x: '|'.join(x.dropna().unique())
        ).reset_index()

        renamed = {col: (
            "medgen_UMLS" if col == "CUI_or_CN_id" else
            "medgen_Preferred_Name" if col == "Preferred_Name" else
            f"medgen_{col}"
        ) for col in df_pivoted.columns}
        df_pivoted.rename(columns=renamed, inplace=True)

        prefix_map = {
            "medgen_GARD": "GARD:",
            "medgen_Orphanet": "Orphanet:",
            "medgen_SNOMEDCT_US": "SNOMEDCT:",
            "medgen_MedGen": "MEDGEN:",
            "medgen_OMIM": "OMIM:",
            "medgen_MeSH": "MESH:"
        }

        for col, prefix in prefix_map.items():
            if col in df_pivoted:
                df_pivoted[col] = df_pivoted[col].apply(lambda x: self.apply_prefix(x, prefix))

        df_pivoted.to_csv(output_file, index=False)
        logging.info(f"✅ Saved pivoted data → {output_file}")

    def save_flat(self, df, output_file):
        df.to_csv(output_file, index=False)
        logging.info(f"✅ Saved flat data → {output_file}")

    def run(self):
        metadata = {
            "timestamp": datetime.now().isoformat(),
            "files": []
        }

        for key, entry in self.cfg.items():
            if key in ["transform_metadata", "log_file", "dl_metadata_file"]:
                continue
            if not isinstance(entry, dict) or "local_txt" not in entry:
                continue

            input_path = Path(entry["local_txt"])
            output_file = entry["output_csv"]
            column_names = entry["column_names"]

            try:
                logging.info(f"Parsing: {input_path}")
                df = self.parse_and_clean(input_path, column_names)

                if "medgen_id_mappings.csv" in output_file:
                    self.pivot_and_save(df, output_file)
                else:
                    self.save_flat(df, output_file)

                metadata["files"].append({
                    "label": key,
                    "input_file": str(input_path),
                    "output_file": str(output_file),
                    "records": len(df)
                })
            except Exception as e:
                logging.error(f"❌ Error processing {key}: {e}")
                metadata["files"].append({
                    "label": key,
                    "input_file": str(input_path),
                    "output_file": str(output_file),
                    "status": "error",
                    "error": str(e)
                })

        with open(self.metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)
        logging.info(f"📝 Metadata written → {self.metadata_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    MedGenTransformer(cfg).run()
