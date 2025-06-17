#!/usr/bin/env python
"""
nodenorm_protein_transform.py - Parse NodeNorm protein dump into structured CSV

This script reads a filtered NodeNorm protein file (in JSON-lines format),
parses each JSON record, and transforms it into a structured CSV.
It also writes detailed metadata (input/output counts, durations, paths, schema).
"""

import os
import json
import yaml
import argparse
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import pandas as pd

def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Prevent duplicate logs by clearing existing handlers
    if root.hasHandlers():
        root.handlers.clear()
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(sh)

class NodeNormProteinTransformer:
    def __init__(self, cfg):
        c = cfg["nodenorm_proteins"]
        # paths from config
        self.input_file    = c["raw_file"]
        self.output_file   = c["output_file"]
        self.metadata_file = c.get("tf_metadata_file",
                                  os.path.join(os.path.dirname(self.output_file),
                                               "tf_nodenorm_proteins_metadata.json"))
        log_file = c.get("log_file",
                         os.path.join(os.path.dirname(self.metadata_file),
                                      "nodenorm_protein_transform.log"))
        setup_logging(log_file)

        # track metadata
        self.metadata = {
            "timestamp": {
                "start": datetime.now().isoformat()
            },
            "input_file": self.input_file,
            "output_file": self.output_file,
            # below to be filled in
            "record_count_input": None,
            "record_count_output": None,
            "input_file_size_bytes": None,
            "output_file_size_bytes": None,
            "num_output_columns": None,
            "output_columns": None,
        }
        self.records = []
        self._df = None
        # record start for duration
        self._start_time = datetime.now()

    def pop_parentheses(self, text):
        text = text.strip()
        if text.endswith(")"):
            i = text.rfind("(")
            if i != -1:
                return text[i+1:-1].strip(), text[:i].strip()
        return None, text

    def parse_preferred_name(self, raw):
        parts = raw.split(" ", 1)
        if len(parts) < 2:
            return raw, raw, None, None
        uniprot, rem = parts[0], parts[1]
        src, rem = self.pop_parentheses(rem)
        typ, rem = self.pop_parentheses(rem)
        return uniprot, rem.strip(), typ, src

    def _parse_json_lines(self):
        logging.info(f"Reading JSON-lines from {self.input_file}")
        lines = []
        with open(self.input_file) as f:
            for ln in f:
                if '"NCBITaxon:9606"' in ln:
                    lines.append(ln)
        self.metadata["record_count_input"] = len(lines)
        self.metadata["input_file_size_bytes"] = os.path.getsize(self.input_file)
        logging.info(f"Found {len(lines)} human entries")
        return lines

    def parse_data(self):
        lines = self._parse_json_lines()
        for idx, ln in enumerate(lines, 1):
            try:
                obj = json.loads(ln)
            except json.JSONDecodeError:
                logging.warning(f"Bad JSON on line {idx}, skipping")
                continue
            taxa = obj.get("taxa", [])
            if "NCBITaxon:9606" not in taxa:
                continue

            uniprot, main, typ, src = self.parse_preferred_name(
                obj.get("preferred_name","")
            )
            row = {
                "EntryID": idx,
                "Type": obj.get("type"),
                "Taxa": "|".join(taxa),
                "uniprot_name": uniprot,
                "PreferredName": main,
                "ProteinType": typ,
                "Source": src
            }
            for ident in obj.get("identifiers", []):
                code = ident.get("i","")
                if ":" in code:
                    pref, suf = code.split(":",1)
                    row.setdefault(pref, []).append(suf)
            # join lists
            for k,v in list(row.items()):
                if isinstance(v, list):
                    row[k] = "|".join(v)
            self.records.append(row)

        self.metadata["record_count_output"] = len(self.records)
        logging.info(f"Parsed {len(self.records)} output records")

    def save_to_csv(self):
        if not self.records:
            logging.error("No records to save, exiting")
            return
        df = pd.DataFrame(self.records)
        # rename for provenance
        rename_map = {
            "EntryID": "NodeNorm_Protein",
            "Type": "biolinkType",
            "PreferredName": "nodenorm_name",
            "UniProtKB": "nodenorm_uniprot_id",
            "ENSEMBL": "nodenorm_ensembl_protein_id",
            "UMLS": "nodenorm_UMLS"
        }
        df.rename(columns=rename_map, inplace=True)
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        df.to_csv(self.output_file, index=False)
        logging.info(f"Saved CSV to {self.output_file}")

        # capture schema + sizes
        self._df = df
        self.metadata["output_file_size_bytes"] = os.path.getsize(self.output_file)
        self.metadata["num_output_columns"] = df.shape[1]
        self.metadata["output_columns"] = df.columns.tolist()

    def save_metadata(self):
        end = datetime.now()
        self.metadata["timestamp"]["end"] = end.isoformat()
        self.metadata["transformation_duration_seconds"] = (end - self._start_time).total_seconds()

        os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
        with open(self.metadata_file, "w") as mf:
            json.dump(self.metadata, mf, indent=2)
        logging.info(f"Metadata written to {self.metadata_file}")

    def run(self):
        self.parse_data()
        self.save_to_csv()
        self.save_metadata()

if __name__=="__main__":
    p = argparse.ArgumentParser(description="Transform NodeNorm protein JSONL to CSV")
    p.add_argument("--config", default="config/targets/targets_config.yaml")
    args = p.parse_args()
    cfg = yaml.safe_load(open(args.config))
    NodeNormProteinTransformer(cfg).run()
