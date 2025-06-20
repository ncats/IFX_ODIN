# GO Transformer - ODIN style
import os
import logging
import argparse
import pandas as pd
import gzip
import shutil
import yaml
import json
from datetime import datetime


def setup_logging(log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ],
        force=True
    )


class GOTransformer:
    def __init__(self, config):
        self.cfg = config["go"]
        self.qc_mode = config.get("global", {}).get("qc_mode", False)
        setup_logging(self.cfg["log_file"])
        self.metadata = {
            "timestamp": {"start": str(datetime.now())},
            "outputs": []
        }

    def parse_obo(self):
        path = self.cfg["obo_raw"]
        data = []
        current_term = {}
        with open(path, 'r') as f:
            for line in f:
                line = line.strip()
                if line == "[Term]":
                    if current_term and "id" in current_term:
                        data.append(current_term)
                    current_term = {}
                elif line == "[Typedef]" or line == "":
                    continue
                else:
                    key, value = line.split(": ", 1)
                    current_term[key] = value
            if current_term and "id" in current_term:
                data.append(current_term)

        df = pd.DataFrame(data)
        df = df[df['name'] != "term tracker item"]

        columns = ['data-version','id','name','namespace','def','synonym','is_a','alt_id','subset','xref','comment','is_obsolete']
        df = df[[col for col in columns if col in df.columns]]
        df.rename(columns={"id": "go_id"}, inplace=True)
        df.fillna("", inplace=True)

        df.to_csv(self.cfg["obo_cleaned"], index=False)
        logging.info(f"Saved cleaned GO terms → {self.cfg['obo_cleaned']}")
        self.metadata['outputs'].append(self.cfg['obo_cleaned'])

        obsolete_df = df[df['def'].str.contains("OBSOLETE", na=False)]
        obsolete_df.to_csv(self.cfg["obo_obsolete"], index=False)
        logging.info(f"Saved obsolete GO terms → {self.cfg['obo_obsolete']}")
        if self.qc_mode:
            self.metadata['outputs'].append(self.cfg['obo_obsolete'])

        node_df = df[['go_id', 'name', 'namespace', 'def', 'alt_id']].copy()
        node_df.to_csv(self.cfg['go_node'], index=False)
        logging.info(f"Saved GO node file → {self.cfg['go_node']}")
        self.metadata['outputs'].append(self.cfg['go_node'])

    def parse_gaf(self):
        with gzip.open(self.cfg['gaf_raw'], 'rt') as f:
            df = pd.read_csv(f, sep='\t', comment='!', header=None, dtype=str)
        df = df.drop(columns=[df.columns[0], df.columns[-1]])
        df.columns = [
            'UniProtKB', 'symbol', 'qualifier', 'go_id', 'REF', 'evidence_type', 
            'dbxref', 'namespace', 'gene_name', 'altsymbol', 
            'target_type', 'taxon', 'date', 'source', 'part_of'
        ]
        df.rename(columns={'UniProtKB': 'uniprot_id'}, inplace=True)

        mapping = pd.read_csv(self.cfg['protein_mapping'], dtype=str)[['ncats_protein_id', 'uniprot_id']]
        merged = pd.merge(mapping, df, on='uniprot_id', how='left')
        merged = merged[merged['go_id'].notna()]  # 🚫 Drop unmapped
        merged.to_csv(self.cfg['gaf_cleaned'], index=False)
        logging.info(f"Saved GOA → {self.cfg['gaf_cleaned']}")
        self.metadata['outputs'].append(self.cfg['gaf_cleaned'])

    def parse_gene2go(self):
        with gzip.open(self.cfg['gene2go_raw'], 'rt') as f:
            df = pd.read_csv(f, sep='\t', dtype=str)
        df = df[df['#tax_id'] == '9606']
        df.rename(columns={'GeneID': 'NCBI_id', 'GO_ID': 'go_id'}, inplace=True)
        df.drop(columns=['#tax_id'], inplace=True)

        mapping = pd.read_csv(self.cfg['gene_mapping'], dtype=str)[['ncats_gene_id', 'consolidated_NCBI_id']]
       # Rename to match mapping column
        df.rename(columns={"NCBI_id": "consolidated_NCBI_id"}, inplace=True)

        # Merge on consolidated NCBI ID
        merged = pd.merge(df, mapping, on="consolidated_NCBI_id", how="left")
        merged.to_csv(self.cfg['gene2go_cleaned'], index=False)
        logging.info(f"Saved gene2go merged → {self.cfg['gene2go_cleaned']}")
        self.metadata['outputs'].append(self.cfg['gene2go_cleaned'])

        # Split by qualifier if enabled
        if 'Qualifier' in merged.columns and self.qc_mode:
            os.makedirs(self.cfg['gene2go_edges_dir'], exist_ok=True)
            for qualifier in merged['Qualifier'].dropna().unique():
                q_df = merged[merged['Qualifier'] == qualifier]
                q_file = os.path.join(
                    self.cfg['gene2go_edges_dir'],
                    f"gene2go_{qualifier.replace(' ', '_').replace('/', '_').replace(':', '').lower()}.csv"
                )
                q_df.to_csv(q_file, index=False)
                logging.info(f"Saved qualifier edge file → {q_file}")
                self.metadata['outputs'].append(q_file)

    def save_metadata(self):
        self.metadata["timestamp"]["end"] = str(datetime.now())
        meta_file = self.cfg["transform_metadata_file"]
        os.makedirs(os.path.dirname(meta_file), exist_ok=True)
        with open(meta_file, "w") as f:
            json.dump(self.metadata, f, indent=2)
        logging.info(f"Saved transform metadata → {meta_file}")

    def run(self):
        self.parse_obo()
        self.parse_gaf()
        self.parse_gene2go()
        self.save_metadata()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="GO data transformer for ODIN pipeline")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    GOTransformer(config).run()
