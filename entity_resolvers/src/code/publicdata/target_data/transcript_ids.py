#!/usr/bin/env python
"""
transcript_ids.py – Post-process transcript provenance mappings:
  • load the merged transcript CSV
  • preserve old IFX IDs and mint new ones for new rows
  • generate NCATS Transcript IDs
  • record detailed metadata for each step
"""

import os
import json
import yaml
import logging
import argparse
import secrets
import pandas as pd
from datetime import datetime
from logging.handlers import RotatingFileHandler

def setup_logging(log_file):
    root = logging.getLogger()
    # If we've already added handlers, do nothing.
    if root.handlers:
        return

    root.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Always add console handler
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # And only add file handler if requested
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        root.addHandler(fh)

class TranscriptDataProcessor:
    def __init__(self, cfg):
        c = cfg['transcript_ids']
        self.config        = c
        self.source_file   = c['source_file']
        self.output_path   = c['transcript_ids_path']
        self.metadata_path = c['metadata_file']
        log_file           = c.get('log_file', self.metadata_path.replace('.json', '.log'))

        setup_logging(log_file)
        logging.info("🚀 Starting TranscriptDataProcessor")

        # initialize metadata
        self.metadata = {
            "timestamp": {"start": datetime.now().isoformat()},
            "data_sources": [self.source_file],
            "processing_steps": [],
            "outputs": []
        }

    def add_metadata_step(self, step_name, description, records=None, duration=None, path=None):
        entry = {
            "step_name": step_name,
            "description": description,
            "performed_at": datetime.now().isoformat()
        }
        if records is not None:
            entry["records"] = records
        if duration is not None:
            entry["duration_seconds"] = duration
        if path is not None:
            entry["output_path"] = path
        self.metadata["processing_steps"].append(entry)
        logging.info("Added metadata step: %s – %s", step_name, description)

    def load_dataset(self):
        t0 = datetime.now()
        logging.info("STEP 1: load_dataset")
        keep = [
            'ensembl_gene_id','refseq_ncbi_id','ensembl_transcript_name',
            'symbol','ensembl_transcript_id','ensembl_transcript_id_version',
            'ensembl_transcript_type','ensembl_trans_bp_start','ensembl_trans_bp_end',
            'ensembl_trans_length','ensembl_transcript_tsl','ensembl_canonical',
            'ensembl_refseq_NM','ensembl_refseq_MANEselect',
            'refseq_status','refseq_rna_id',
            'Ensembl_Transcript_ID_Provenance','RefSeq_Provenance'
        ]
        try:
            df = pd.read_csv(self.source_file, low_memory=False)
            df = df.reindex(columns=keep)
            logging.info("Loaded %d rows from %s", len(df), self.source_file)
        except Exception as e:
            logging.error("Error loading dataset: %s", e)
            df = pd.DataFrame(columns=keep)

        duration = (datetime.now() - t0).total_seconds()
        self.df = df
        self.add_metadata_step("Dataset Loading",
                               f"Loaded & filtered dataset from {self.source_file}",
                               records=len(df), duration=duration)
        self.metadata["outputs"].append({
            "name": "raw_transcript_data",
            "path": self.source_file,
            "records": len(df)
        })

    def generate_transcript_ids(self):
        t0 = datetime.now()
        logging.info("STEP 2: generate_transcript_ids")

        # keys to identify a transcript
        keys = ["ensembl_transcript_id_version", "refseq_rna_id"]

        # 1) load existing IDs if any
        if os.path.exists(self.output_path):
            old = pd.read_csv(self.output_path, dtype=str)
            old = old[keys + ["ncats_transcript_id","createdAt","updatedAt"]].drop_duplicates(subset=keys)
            logging.info("Loaded %d existing IFX IDs", len(old))
        else:
            old = pd.DataFrame(columns=keys + ["ncats_transcript_id","createdAt","updatedAt"])
            logging.info("No existing IFX IDs found")

        # 2) drop duplicate keys in source
        src = self.df.drop_duplicates(subset=keys)

        # 3) left-merge to bring in old IDs
        merged = pd.merge(src, old, on=keys, how="left")

        # 4) mint new IDs where missing
        now = datetime.now().isoformat()
        mask = merged["ncats_transcript_id"].isna()
        n_new = int(mask.sum())
        logging.info("Minting %d new IFX IDs", n_new)

        def mint():
            return "IFXTranscript:" + "".join(secrets.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(7))

        merged.loc[mask, "ncats_transcript_id"] = [mint() for _ in range(n_new)]
        merged.loc[mask, "createdAt"] = now

        # 5) update updatedAt for all
        merged["updatedAt"] = now

        # 6) attach the three new columns back onto the full dataset
        ids_only = merged.set_index(keys)[["ncats_transcript_id","createdAt","updatedAt"]]
        self.ncats_df = (
            self.df
            .join(ids_only, on=keys)
            # now move our new columns to the front
            .pipe(lambda df: df[["ncats_transcript_id","createdAt","updatedAt"] + 
                                [c for c in df.columns if c not in {"ncats_transcript_id","createdAt","updatedAt"}]])
        )
        duration = (datetime.now() - t0).total_seconds()
        total = len(self.ncats_df)
        logging.info("Generated/transferred IDs for %d rows (%d new)", total, n_new)
        self.add_metadata_step("NCATS Transcript ID Generation",
                               f"Preserved {total-n_new} old and minted {n_new} new IDs",
                               records=total, duration=duration)
        self.metadata["outputs"].append({
            "name": "transcript_ids",
            "path": self.output_path,
            "records": total
        })

    def save_transcript_ids(self):
        t0 = datetime.now()
        logging.info("STEP 3: save_transcript_ids")

        # Force .tsv extension if .csv is in the config
        if self.output_path.endswith(".csv"):
            self.output_path = self.output_path.replace(".csv", ".tsv")

        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        self.ncats_df.to_csv(self.output_path, index=False, sep='\t')

        duration = (datetime.now() - t0).total_seconds()
        logging.info("Transcript IDs saved to %s", self.output_path)
        self.add_metadata_step("Save Transcript IDs",
                            f"Saved {len(self.ncats_df)} rows to {self.output_path}",
                            records=len(self.ncats_df),
                            duration=duration,
                            path=self.output_path)

    def save_metadata(self):
        self.metadata["timestamp"]["end"] = datetime.now().isoformat()
        os.makedirs(os.path.dirname(self.metadata_path), exist_ok=True)
        with open(self.metadata_path, 'w') as mf:
            json.dump(self.metadata, mf, indent=2)
        logging.info("STEP 4: save_metadata     Metadata written to %s", self.metadata_path)

    def run(self):
        self.load_dataset()
        self.generate_transcript_ids()
        self.save_transcript_ids()
        self.save_metadata()
        logging.info("🎉 TranscriptDataProcessor complete!")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Post-process transcript provenance mappings")
    p.add_argument("--config", type=str,
               default="config/targets_config.yaml",
               help="YAML config (default: config/targets_config.yaml)")

    args = p.parse_args()

    full_cfg = yaml.safe_load(open(args.config))
    processor = TranscriptDataProcessor(full_cfg)
    processor.run()
