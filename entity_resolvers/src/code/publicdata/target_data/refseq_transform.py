#!/usr/bin/env python
"""
refseq_transform.py - Transform, clean, and process RefSeq (and related) data

This script reads the decompressed RefSeq data file (produced by refseq_download.py),
transforms and cleans the data (including renaming columns), and writes the cleaned CSV.
It also saves detailed metadata about the transformation process including:
  - input and output file paths,
  - record counts before/after each step,
  - transformation start time, end time, and duration,
  - list of data sources,
  - processing steps details,
  - and outputs generated.
"""

import os
import yaml
import json
import pandas as pd
import logging
import argparse
from datetime import datetime

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class RefSeqTransformer:
    def __init__(self, full_config):
        # Extract the refseq_data section from the full config.
        self.config = full_config["refseq_data"]
        # Paths
        self.decompressed = self.config["refseq"]["decompressed"]
        self.transformed_data_path = self.config["transformed_data_path"]
        # Metadata file
        self.metadata_file = self.config.get("tf_metadata_file", "tf_refseq_metadata.json")
        # Initialize metadata container
        self.metadata = {
            "timestamp": {"start": datetime.now().isoformat()},
            "data_sources": [],
            "processing_steps": [],
            "outputs": []
        }
        # Record data source
        self.metadata["data_sources"].append({
            "name": "RefSeq gene2refseq",
            "path": self.decompressed,
            "timestamp": datetime.now().isoformat()
        })

    def fetch_and_process_data(self):
        # 1. Load data
        logging.info(f"Loading RefSeq data from {self.decompressed}...")
        start_read = datetime.now()
        try:
            df = pd.read_csv(self.decompressed, sep="\t", dtype=str)
        except Exception as e:
            logging.error(f"Error reading RefSeq data: {e}")
            return None
        end_read = datetime.now()
        records_before = len(df)
        self.metadata["processing_steps"].append({
            "step": "read_tsv",
            "records": records_before,
            "duration_seconds": (end_read - start_read).total_seconds()
        })

        # 2. Rename columns
        logging.info("Renaming columns for RefSeq data...")
        rename_map = {
            '#tax_id': 'refseq_tax_id',
            'GeneID': 'refseq_ncbi_id',
            'Symbol': 'refseq_symbol',
            'status': 'refseq_status',
            'Synonyms': 'refseq_synonyms',
            'dbXrefs': 'refseq_dbxrefs',
            'chromosome': 'refseq_chromosome',
            'map_location': 'refseq_location',
            'description': 'refseq_description',
            'type_of_gene': 'refseq_gene_type',
            'Modification_date': 'refseq_Modification_date',
            'Feature_type': 'refseq_feature_type',
            'RNA_nucleotide_accession.version': 'refseq_rna_id',
            'protein_accession.version': 'refseq_protein_id'
        }
        df.rename(columns=rename_map, inplace=True)
        self.metadata["processing_steps"].append({
            "step": "rename_columns",
            "renamed": list(rename_map.items()),
            "columns_after": df.columns.tolist()
        })

        # 3. Save transformed data
        logging.info(f"Saving transformed RefSeq data to {self.transformed_data_path}...")
        os.makedirs(os.path.dirname(self.transformed_data_path), exist_ok=True)
        try:
            df.to_csv(self.transformed_data_path, index=False)
        except Exception as e:
            logging.error(f"Error saving transformed RefSeq data: {e}")
            return None

        self.metadata["outputs"].append({
            "name": "RefSeq Transformed Data",
            "path": self.transformed_data_path,
            "records": records_before,
            "generated_at": datetime.now().isoformat()
        })

        # === Column-wise diff logic ===
        qc_dir = "src/data/publicdata/target_data/qc"
        os.makedirs(qc_dir, exist_ok=True)
        base = os.path.splitext(os.path.basename(self.transformed_data_path))[0]
        backup_path = os.path.join(qc_dir, f"{base}.backup.csv")
        diff_csv_path = os.path.join(qc_dir, f"{base}_diff.csv")

        if os.path.exists(backup_path):
            try:
                old_df = pd.read_csv(backup_path, dtype=str).fillna("")
                new_df = df.fillna("")

                join_col = "refseq_ncbi_id" if "refseq_ncbi_id" in df.columns else None
                if join_col:
                    old_df.set_index(join_col, inplace=True)
                    new_df.set_index(join_col, inplace=True)

                # Align both index and columns
                common_cols = sorted(set(old_df.columns).intersection(new_df.columns))
                old_df = old_df[common_cols].sort_index()
                new_df = new_df[common_cols].sort_index()

                diff_df = old_df.compare(new_df, keep_shape=False, keep_equal=False)
                if not diff_df.empty:
                    diff_df.to_csv(diff_csv_path)
                    logging.info(f"✅ Cleaned CSV diff written to {diff_csv_path}")
                    self.metadata["outputs"].append({
                        "name": "RefSeq Cleaned Column Diff",
                        "path": diff_csv_path,
                        "generated_at": datetime.now().isoformat()
                    })
                else:
                    logging.info("✅ No differences found in cleaned RefSeq output.")
            except Exception as e:
                logging.warning(f"⚠️ Could not generate column-level diff: {e}")

        # Always update backup
        df.to_csv(backup_path, index=False)

        return df

    def generate_concatenated_files(self, df):
        if not self.config.get('global', {}).get('qc_mode', True):
            logging.info('QC mode disabled, skipping concatenated QC files.')
            return
        # 4. Concatenate RNA IDs
        logging.info("Generating concatenated RNA IDs file...")
        step_start = datetime.now()
        df_rna = df[df['refseq_rna_id'] != '-']
        count_rna_in = len(df_rna)
        rna_grouped = df_rna.groupby('refseq_ncbi_id')['refseq_rna_id']\
            .agg(lambda x: '|'.join(x.dropna().unique())).reset_index()
        rna_output_path = self.config['refseq']['rna_concatenated_path']
        os.makedirs(os.path.dirname(rna_output_path), exist_ok=True)
        rna_grouped.to_csv(rna_output_path, index=False)
        step_end = datetime.now()
        self.metadata["processing_steps"].append({
            "step": "concat_rna",
            "input_records": count_rna_in,
            "output_records": len(rna_grouped),
            "duration_seconds": (step_end - step_start).total_seconds(),
            "output": rna_output_path
        })
        self.metadata["outputs"].append({
            "name": "RefSeq RNA Concatenated",
            "path": rna_output_path,
            "generated_at": datetime.now().isoformat()
        })

        # 5. Concatenate protein IDs
        logging.info("Generating concatenated protein IDs file...")
        step_start = datetime.now()
        df_prot = df[df['refseq_protein_id'] != '-']
        count_prot_in = len(df_prot)
        prot_grouped = df_prot.groupby('refseq_ncbi_id')['refseq_protein_id']\
            .agg(lambda x: ';'.join(x.dropna().unique())).reset_index()
        prot_output_path = self.config['refseq']['protein_concatenated_path']
        os.makedirs(os.path.dirname(prot_output_path), exist_ok=True)
        prot_grouped.to_csv(prot_output_path, index=False)
        step_end = datetime.now()
        self.metadata["processing_steps"].append({
            "step": "concat_protein",
            "input_records": count_prot_in,
            "output_records": len(prot_grouped),
            "duration_seconds": (step_end - step_start).total_seconds(),
            "output": prot_output_path
        })
        self.metadata["outputs"].append({
            "name": "RefSeq Protein Concatenated",
            "path": prot_output_path,
            "generated_at": datetime.now().isoformat()
        })

    def run(self):
        overall_start = datetime.now()
        df = self.fetch_and_process_data()
        if df is None:
            logging.error("Transformation failed; no data.")
            return
        self.generate_concatenated_files(df)
        overall_end = datetime.now()

        # Record final counts
        num_in = len(pd.read_csv(self.decompressed, sep="\t", dtype=str))
        num_out = len(df)
        duration = (overall_end - overall_start).total_seconds()

        # Finalize metadata
        self.metadata['timestamp']['end'] = overall_end.isoformat()
        self.metadata['timestamp']['duration_seconds'] = duration
        self.metadata['num_records_input'] = num_in
        self.metadata['num_records_output'] = num_out

        # Write metadata to file
        with open(self.metadata_file, 'w') as mf:
            json.dump(self.metadata, mf, indent=2)
        logging.info(f"Metadata written to {self.metadata_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform and clean RefSeq data")
    parser.add_argument("--config", type=str, default="config/targets_config.yaml")
    args = parser.parse_args()
    with open(args.config) as f:
        cfg = yaml.safe_load(f)
    RefSeqTransformer(cfg).run()