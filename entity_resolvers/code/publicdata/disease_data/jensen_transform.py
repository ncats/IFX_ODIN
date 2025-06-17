# jensen_transform.py - Full disease-protein association transformer (preserves all mapping logic)

import os
import json
import yaml
import pandas as pd
import numpy as np
import argparse
from pathlib import Path
from datetime import datetime

def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode="a"),
            logging.StreamHandler()
        ],
        force=True
    )

class JensenDiseaseTransformer:
    def __init__(self, config):
        self.cfg = config["jensen"]
        self.input_file = Path(self.cfg["input_file"])
        self.output_file = Path(self.cfg["output_file"])
        self.unmapped_file = Path(self.cfg["unmapped_file"])
        self.metadata_file = Path(self.cfg["transform_metadata"])
        self.log_file = Path(self.cfg["log_file"])
        self.disease_id_file = Path(self.cfg["resolved_disease_ids"])
        self.protein_id_file = Path(self.cfg["resolved_protein_ids"])
        self.isoform_file = Path(self.cfg.get("resolved_isoform_ids", ""))

        os.makedirs(self.output_file.parent, exist_ok=True)
        os.makedirs(self.metadata_file.parent, exist_ok=True)
        setup_logging(self.log_file)
        self.metadata = {
            "timestamp": str(datetime.now()),
            "processing_steps": [],
            "outputs": []
        }

    def log_step(self, step, desc):
        self.metadata["processing_steps"].append({
            "step_name": step,
            "description": desc,
            "performed_at": str(datetime.now())
        })

    def transform(self):
        logging.info(f"📥 Reading: {self.input_file}")
        df = pd.read_csv(self.input_file, sep="\t", names=['protein_stable_id', 'symbol', 'DOID', 'disease', 'score'], dtype=str)
        df['score'] = pd.to_numeric(df['score'], errors='coerce')

        df_with_prefix = df[df['DOID'].str.contains("DOID:|OMIM:", na=False)]
        df_without_prefix = df[~df['DOID'].str.contains("DOID:|OMIM:", na=False)]
        df_with_prefix.to_csv("src/data/publicdata/disease_data/semi/d2p_with_DOID.csv", index=False)
        df_without_prefix.to_csv("src/data/publicdata/disease_data/semi/D2P_with_ICD10.csv", index=False)

        disease_ids = pd.read_csv(self.disease_id_file, dtype=str)[['ncats_disease_id', 'DOID', 'OMIM']]
        merged = df_with_prefix.merge(disease_ids, on='DOID', how='left')

        unmapped = merged[merged['ncats_disease_id'].isna()]
        unmapped.to_csv("src/data/publicdata/disease_data/cleaned/sources/jensen_unmapped_disease_ids.csv", index=False)

        filtered = merged[merged['score'] > 2.5]
        proteins = pd.read_csv(self.protein_id_file, dtype=str)[['ncats_protein_id', 'consolidated_ensembl_protein_id']]
        proteins['consolidated_ensembl_protein_id'] = proteins['consolidated_ensembl_protein_id'].str.replace(r'\.\d+$', '', regex=True)

        joined = proteins.merge(filtered, left_on='consolidated_ensembl_protein_id', right_on='protein_stable_id', how='inner')
        joined['ncats_gene_id'] = np.nan

        if self.isoform_file and os.path.exists(self.isoform_file):
            isoform_df = pd.read_csv(self.isoform_file, dtype=str)
            mapped_isoform_ids = set(isoform_df['consolidated_ensembl_protein_id'].dropna().str.replace(r'\.\d+$', '', regex=True))
            joined = joined[~joined['protein_stable_id'].isin(mapped_isoform_ids)]

        unmapped = joined[joined['ncats_protein_id'].isna()]
        unmapped.to_csv(self.unmapped_file, index=False)
        self.log_step("Save unmapped", f"Unmapped: {self.unmapped_file}")

        joined = joined.dropna(subset=['ncats_disease_id'])
        mapped = joined[joined['ncats_protein_id'].notna()]
        mapped_output_file = self.unmapped_file.with_name(self.unmapped_file.name.replace("unmapped", "mapped"))
        mapped.to_csv(mapped_output_file, index=False)
        self.log_step("Save mapped", f"Mapped: {mapped_output_file}")

        combined = mapped[['ncats_disease_id', 'DOID', 'protein_stable_id', 'symbol', 'disease', 'score', 'ncats_protein_id']]
        combined.to_csv(self.output_file, index=False)
        self.log_step("Final output", f"Output written: {self.output_file}")

        with open(self.metadata_file, "w") as f:
            json.dump(self.metadata, f, indent=2)
        logging.info(f"📊 Metadata saved: {self.metadata_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform Jensen disease-protein associations")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    JensenDiseaseTransformer(config).transform()
