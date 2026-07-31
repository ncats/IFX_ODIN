# jensen_transform.py - Full disease-protein association transformer with edge diffs

import os
import json
import yaml
import pandas as pd
import numpy as np
import argparse
from pathlib import Path
from datetime import datetime
import logging


def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file, mode="a"), logging.StreamHandler()],
        force=True
    )


class JensenDiseaseTransformer:
    def __init__(self, config):
        self.cfg = config["jensen"]
        self.input_file = Path(self.cfg["raw_file"])
        self.output_file = Path(self.cfg["cleaned_file"])
        self.unmapped_file = self.output_file.with_name("jensen_unmapped_disease_ids.csv")
        self.metadata_file = Path(self.cfg["transform_metadata_file"])
        self.log_file = Path(self.cfg["log_file"])
        self.protein_id_file = Path(self.cfg["protein_id_file"])

        os.makedirs(self.output_file.parent, exist_ok=True)
        os.makedirs(self.metadata_file.parent, exist_ok=True)
        setup_logging(self.log_file)

    def _edge_diff(self, old_df, new_df):
        old_edges = set(zip(old_df["ncats_disease_id"].fillna(""), old_df["ncats_protein_id"].fillna("")))
        new_edges = set(zip(new_df["ncats_disease_id"].fillna(""), new_df["ncats_protein_id"].fillna("")))
        return {
            "added_edges": sorted(list(new_edges - old_edges)),
            "removed_edges": sorted(list(old_edges - new_edges))
        }

    def transform(self):
        df = pd.read_csv(
            self.input_file,
            sep="\t",
            names=['protein_stable_id', 'symbol', 'DOID', 'disease', 'score'],
            dtype=str
        )
        df['score'] = pd.to_numeric(df['score'], errors='coerce')

        df_with_prefix = df[df['DOID'].str.contains("DOID:|OMIM:", na=False)].copy()

        disease_ids = pd.DataFrame(columns=["ncats_disease_id", "DOID", "OMIM"])
        disease_merge_cfg = self.cfg.get("resolved_disease_ids")
        if disease_merge_cfg and os.path.exists(disease_merge_cfg):
            disease_ids = pd.read_csv(disease_merge_cfg, dtype=str)[['ncats_disease_id', 'DOID', 'OMIM']]

        merged = df_with_prefix.merge(disease_ids, on='DOID', how='left')
        unmapped = merged[merged['ncats_disease_id'].isna()]
        unmapped.to_csv(self.unmapped_file, index=False)

        filtered = merged[merged['score'] > 2.5].copy()

        proteins = pd.read_csv(self.protein_id_file, dtype=str)[['ncats_protein_id', 'consolidated_ensembl_protein_id']]
        proteins['consolidated_ensembl_protein_id'] = proteins['consolidated_ensembl_protein_id'].str.replace(r'\.\d+$', '', regex=True)

        filtered['protein_stable_id'] = filtered['protein_stable_id'].str.replace(r'\.\d+$', '', regex=True)
        joined = proteins.merge(filtered, left_on='consolidated_ensembl_protein_id', right_on='protein_stable_id', how='inner')

        joined = joined.dropna(subset=['ncats_disease_id'])
        mapped = joined[joined['ncats_protein_id'].notna()].copy()

        combined = mapped[['ncats_disease_id', 'DOID', 'protein_stable_id', 'symbol', 'disease', 'score', 'ncats_protein_id']]

        prev = self.output_file.with_suffix(".previous.csv")
        if prev.exists():
            old_df = pd.read_csv(prev, dtype=str)
            diff = self._edge_diff(old_df, combined)
            with open(self.output_file.with_name("jensen_changes.qc.json"), "w") as f:
                json.dump(diff, f, indent=2)

        combined.to_csv(self.output_file, index=False)
        combined.to_csv(prev, index=False)

        metadata = {
            "timestamp": str(datetime.now()),
            "input_file": str(self.input_file),
            "output_file": str(self.output_file),
            "unmapped_file": str(self.unmapped_file),
            "records": len(combined)
        }
        with open(self.metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

    def run(self):
        self.transform()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform Jensen disease-protein associations")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    JensenDiseaseTransformer(config).run()