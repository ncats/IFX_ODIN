#!/usr/bin/env python
"""
pathwaycommons_transform.py - Transform PathwayCommons data into Node↔Pathway↔Hierarchy structure.
Includes QC diff generation and transformation metadata logging.
"""

import os
import json
import gzip
import pandas as pd
import logging
from datetime import datetime
import hashlib
import difflib

def compute_md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def update_metadata(path, new_metadata):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(new_metadata, f, indent=2)

class PathwayCommonsTransformer:
    def __init__(self, config):
        self.cfg = config["pathways"]["pathwaycommons"]
        self.qc_mode = config["global"].get("qc_mode", False)
        files = self.cfg["files"]

        self.outputs = {
            "hgnc": files["pc-hgnc.txt.gz"]["cleaned_file"],
            "pathways": files["pathways.txt.gz"]["cleaned_file"],
            "metadata": self.cfg["transform_metadata_file"]
        }

        self.inputs = {
            "hgnc": os.path.join(self.cfg["raw_dir"], "pc-hgnc.txt.gz"),
            "pathways": os.path.join(self.cfg["raw_dir"], "pathways.txt.gz"),
        }

        self.qc_dir = self.cfg.get("qc_dir", "src/data/publicdata/pathway_data/qc")
        os.makedirs(self.qc_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.outputs["metadata"]), exist_ok=True)
        logging.info("✅ Initialized PathwayCommonsTransformer")

    def parse_tsv(self, path, expected_cols=4):
        open_func = gzip.open if path.endswith(".gz") else open
        rows = []
        line_count = 0
        with open_func(path, 'rt', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line_count += 1
                if line_count % 100000 == 0:
                    logging.info(f"🔁 Parsed {line_count} lines from {path}")
                parts = line.strip().split('\t')
                if len(parts) < expected_cols:
                    parts += [''] * (expected_cols - len(parts))
                rows.append(parts[:expected_cols])
        logging.info(f"📊 Done parsing {line_count} total lines from {path}")
        df = pd.DataFrame(rows, columns=["pathway_id", "pathway_name", "direct_sub_pathways", "all_sub_pathways"])
        logging.info(f"🧪 Columns: {df.columns.tolist()}")
        return df

    def generate_diff(self, output_path, df):
        if not os.path.exists(output_path):
            return
        old_lines = open(output_path, "r", encoding="utf-8", errors="ignore").readlines()
        new_lines = df.to_csv(index=False).splitlines(keepends=True)
        diff = list(difflib.unified_diff(old_lines, new_lines, fromfile="old", tofile="new"))

        if diff:
            base = os.path.splitext(os.path.basename(output_path))[0]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            diff_txt = os.path.join(self.qc_dir, f"{base}_diff_{timestamp}.txt")
            diff_html = os.path.join(self.qc_dir, f"{base}_diff_{timestamp}.html")

            with open(diff_txt, "w") as f:
                f.write("".join(diff))

            html_diff = difflib.HtmlDiff().make_file(
                old_lines, new_lines, fromdesc="Old", todesc="New", context=True, numlines=0
            )
            with open(diff_html, "w") as f:
                f.write(html_diff)

            logging.info(f"📝 Diff files saved: {diff_txt}, {diff_html}")

    def transform_all(self):
        logging.info("🔄 Starting transformation of Pathway Commons datasets")
        metadata = {"timestamp": {"start": str(datetime.now())}, "steps": []}

        # Parse pc-hgnc.gmt.gz (pathway_id ↔ gene_id)
        gmt_path = self.inputs["hgnc"]
        gmt_data = []
        with gzip.open(gmt_path, 'rt') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) > 2:
                    pathway_id = parts[0]
                    genes = parts[2:]
                    for gene in genes:
                        gmt_data.append((pathway_id, gene))
        gmt_df = pd.DataFrame(gmt_data, columns=["pathway_id", "gene_id"])

        node2pathway_path = os.path.join(os.path.dirname(self.outputs["pathways"]), "pathwaycommons_node2pathway.csv")
        gmt_df.to_csv(node2pathway_path, index=False)
        logging.info(f"✅ Saved Node↔Pathway table to {node2pathway_path} with {len(gmt_df)} rows")

        try:
            if self.qc_mode:
                self.generate_diff(node2pathway_path, gmt_df)
        except Exception as e:
            logging.error(f"🔥 Failed diff generation for node2pathway: {e}", exc_info=True)

        metadata["steps"].append({
            "step": "transform_node2pathway",
            "input": gmt_path,
            "output": node2pathway_path,
            "records": len(gmt_df),
            "md5": compute_md5(node2pathway_path),
            "timestamp": str(datetime.now())
        })

        logging.info("🔽 Finished node2pathway. Now starting parse_tsv on pathways.txt.gz")

        # Parse pathways.txt.gz (pathway_id ↔ name + hierarchy)
        pwy_path = self.inputs["pathways"]
        pwy_df = self.parse_tsv(pwy_path)

        pathways_path = os.path.join(os.path.dirname(self.outputs["pathways"]), "pathwaycommons_pathways.csv")
        pwy_df.to_csv(pathways_path, index=False)
        logging.info(f"✅ Saved Pathway Hierarchy table to {pathways_path} with {len(pwy_df)} rows")

        try:
            if self.qc_mode:
                self.generate_diff(pathways_path, pwy_df)
        except Exception as e:
            logging.error(f"🔥 Failed diff generation for pathways table: {e}", exc_info=True)

        metadata["steps"].append({
            "step": "transform_pathways_table",
            "input": pwy_path,
            "output": pathways_path,
            "records": len(pwy_df),
            "md5": compute_md5(pathways_path),
            "timestamp": str(datetime.now())
        })

        metadata["timestamp"]["end"] = str(datetime.now())
        update_metadata(self.outputs["metadata"], metadata)
        logging.info(f"📝 Transformation metadata saved to {self.outputs['metadata']}")

    def run(self):
        self.transform_all()

if __name__ == "__main__":
    import yaml
    import argparse
    from src.code.utils.logger import setup_logging

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    setup_logging(config["pathways"]["pathwaycommons"].get("log_file", "pathwaycommons_transform.log"))
    PathwayCommonsTransformer(config).run()
