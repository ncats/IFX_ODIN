#!/usr/bin/env python
"""
nodenorm_gene_transform.py - Parse NodeNorm gene dump into structured CSV with JSON cleaning transformations

This script reads a NodeNorm gene file in JSON-lines format, cleans and transforms the data into a structured
DataFrame by flattening the JSON structure, removing known prefixes from identifier values, renaming columns,
and dropping columns that are not required in the final output.
It records detailed metadata (timestamps, hashes, sizes, counts, steps) and writes that to a JSON file.
"""

import os
import json
import yaml
import logging
import argparse
import hashlib
import pandas as pd
from datetime import datetime
from logging.handlers import RotatingFileHandler

# Configure logging to both file and console.
def setup_logging(log_file: str):
    """Initialize a rotating‐file + console logger using the path from config."""
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Prevent duplicate logs by clearing existing handlers
    if root.hasHandlers():
        root.handlers.clear()
    # Rotating file
    fh = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3)
    fh.setFormatter(fmt)
    # Console
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(ch)

def compute_md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
    return h.hexdigest()

class NodeNormGeneTransformer:
    def __init__(self, config):
        c = config['nodenorm_genes']
        self.input_file   = c['raw_file']
        self.output_file  = c['output_file']
        self.metadata_file= c['tf_metadata_file']
        self.log_file      = c['log_file']
        os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
        # Set up logging now
        setup_logging(self.log_file)
        logging.info(f"Logging to {self.log_file}")

    def _flatten_record(self, record):
        flat = {
            'type': record.get('type','').strip(),
            'ic': record.get('ic'),
            'preferred_name': record.get('preferred_name','').strip(),
            'taxa': ",".join(record.get('taxa',[]))
        }
        # identifiers
        ids = record.get('identifiers',[])
        gene_id = None
        id_dict = {}
        for idd in ids:
            i = idd.get('i','').strip()
            if not i: continue
            if i.startswith("NCBIGene:"):
                gene_id = i
            db = i.split(":",1)[0]
            id_dict.setdefault(db,[]).append(i)
        flat['gene_id'] = gene_id or flat['preferred_name']
        for db, vals in id_dict.items():
            flat[db] = "|".join(vals)
        return flat

    def _parse_json_lines(self):
        recs = []
        logging.info(f"Reading JSON-lines from {self.input_file}")
        with open(self.input_file) as f:
            for ln, line in enumerate(f,1):
                line=line.strip()
                if not line: continue
                try:
                    obj=json.loads(line)
                    recs.append(self._flatten_record(obj))
                except json.JSONDecodeError as e:
                    logging.error(f"JSON error line {ln}: {e}")
        logging.info(f"Parsed {len(recs)} total records")
        return recs

    def _clean_dataframe(self, df):
        """
        Clean the DataFrame by:
        - Dropping rows without a valid gene_id.
        - Stripping extra whitespace from string columns.
        - Replacing empty strings with None.
        - Dropping duplicate rows.
        """
        logging.info("Starting cleaning process on DataFrame")
        before = len(df)
        df = df.dropna(subset=['gene_id'])
        # Strip whitespace from object (string) columns and replace empty strings with None.
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.strip().replace(r'^\s*$', None, regex=True)
        df = df.drop_duplicates()
        after = len(df)
        removed = before - after
        logging.info(f"Cleaned DF: {removed} duplicates/empties removed → {after} records")
        return df

    def _strip_prefixes(self, df):
        for prefix in ["NCBIGene","ENSEMBL","HGNC","OMIM","UMLS"]:
            if prefix in df.columns:
                df[prefix] = df[prefix].apply(lambda v: "|".join(
                    [p.split(":",1)[1] if p.startswith(prefix+":") else p for p in str(v).split("|")]
                ) if pd.notna(v) else v)
        return df

    def _rename_columns(self, df):
        mapping = {
            "gene_id":"NodeNorm_Gene",
            "type":"biolinkType",
            "preferred_name":"nodenorm_symbol",
            "NCBIGene":"nodenorm_NCBI_id",
            "ENSEMBL":"nodenorm_ensembl_gene_id",
            "HGNC":"nodenorm_HGNC",
            "UMLS":"nodenorm_UMLS",
            "OMIM":"nodenorm_OMIM"
        }
        return df.rename(columns=mapping)

    def run(self):
        t0 = datetime.now()

        # 1) Parse JSON-lines
        records = self._parse_json_lines()
        raw_count = len(records)

        # 2) Build DataFrame
        df = pd.DataFrame(records)

        # 3) Clean
        cleaned_before = len(df)
        df = self._clean_dataframe(df)
        cleaned_after  = len(df)

        # 4) Strip prefixes
        df = self._strip_prefixes(df)

        # 5) Rename
        df = self._rename_columns(df)
        # re–add HGNC: prefix so downstream provenance sees “HGNC:12345”
        if 'nodenorm_HGNC' in df.columns:
            df['nodenorm_HGNC'] = df['nodenorm_HGNC'].apply(
                lambda v: f"HGNC:{v}" 
                    if pd.notnull(v) and str(v).strip() and not str(v).startswith("HGNC:") 
                    else v
            )

        # 6) Drop unwanted
        df = df.drop(columns=["ic","taxa","NodeNorm_Gene"], errors='ignore')

        # 7) Save CSV
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        df.to_csv(self.output_file, index=False)
        out_count = len(df)

        t1 = datetime.now()
        duration = (t1 - t0).total_seconds()

        # 8) Compute sizes & hashes
        in_size  = os.path.getsize(self.input_file)
        out_size = os.path.getsize(self.output_file)
        in_md5   = compute_md5(self.input_file)
        out_md5  = compute_md5(self.output_file)

        # 9) Write metadata
        meta = {
            "timestamp": {
                "start":  t0.isoformat(),
                "end":    t1.isoformat(),
                "duration_seconds": duration
            },
            "input": {
                "path": self.input_file,
                "size_bytes": in_size,
                "md5":        in_md5,
                "record_count_raw": raw_count
            },
            "cleaning": {
                "before": cleaned_before,
                "after":  cleaned_after
            },
            "output": {
                "path": self.output_file,
                "size_bytes": out_size,
                "md5":         out_md5,
                "record_count": out_count
            },
            "processing_steps": [
                {"step":"parse_json_lines","records":raw_count},
                {"step":"clean_dataframe","before":cleaned_before,"after":cleaned_after},
                {"step":"strip_prefixes"},
                {"step":"rename_columns"},
                {"step":"drop_columns"},
                {"step":"write_csv","records":out_count}
            ]
        }
        with open(self.metadata_file, "w") as mf:
            json.dump(meta, mf, indent=2)
        logging.info(f"Transformation complete: {out_count} records → {self.output_file}")
        logging.info(f"Metadata written to {self.metadata_file}")

if __name__=="__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config/targets/targets_config.yaml")
    args = p.parse_args()
    cfg = yaml.safe_load(open(args.config))
    NodeNormGeneTransformer(cfg).run()
