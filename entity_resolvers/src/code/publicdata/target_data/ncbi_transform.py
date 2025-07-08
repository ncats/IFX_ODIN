#!/usr/bin/env python
"""
ncbi_transform.py - Transform and clean NCBI gene_info data

This script reads the decompressed NCBI gene_info file (produced by ncbi_download.py),
performs cleaning and transformation, and then writes out a cleaned CSV.
It also logs a detailed metadata file that includes timestamps, file paths, record counts,
transformation duration, and a summary of processing steps.
"""

import os
import yaml
import json
import pandas as pd
import logging
import argparse
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class NCBITransformer:
    def __init__(self, full_config):
        self.config = full_config["ncbi_data"]
        # Input file: decompressed file from ncbi_download.py
        self.input_file = self.config.get("decompressed_file", "src/data/publicdata/target_data/raw/ncbi_gene_info.tsv")
        # Output file for the transformed data
        self.output_file = self.config.get("parsed_output", "src/data/publicdata/target_data/cleaned/sources/ncbi_gene_info.csv")
        # Metadata file for transformation details
        self.metadata_file = self.config.get("tf_metadata_file", "tf_ncbi_metadata.json")

    def transform_and_clean_ncbi_data(self, ncbi_df):
        start_time = datetime.now()
        processing_steps = []

        try:
            # Remove any leading '#' from column names (e.g. "#tax_id" becomes "tax_id")
            original_columns = ncbi_df.columns.tolist()
            ncbi_df.columns = [col.lstrip('#') for col in ncbi_df.columns]
            processing_steps.append(f"Removed leading '#' from columns: {original_columns} -> {ncbi_df.columns.tolist()}")

            # Rename columns to standard names
            rename_mapping = {
                'GeneID': 'NCBI_id',
                'Symbol': 'symbol',
                'Synonyms': 'synonyms'
            }
            ncbi_df.rename(columns=rename_mapping, inplace=True)
            processing_steps.append(f"Renamed columns using mapping: {rename_mapping}")

            # Define required columns based on the actual file headers
            required_cols = ['NCBI_id', 'symbol', 'synonyms', 'dbXrefs', 'chromosome',
                             'map_location', 'description', 'type_of_gene', 'Modification_date', 'Feature_type']
            missing = [col for col in required_cols if col not in ncbi_df.columns]
            if missing:
                raise KeyError(f"Missing required columns in input: {missing}")
            processing_steps.append("Validated required columns.")

            ncbi_human_df2 = ncbi_df[required_cols].copy()
            # Clean up the dbXrefs field
            ncbi_human_df2['dbXrefs'] = ncbi_human_df2['dbXrefs'].str.replace('HGNC:HGNC:', 'HGNC:')
            ncbi_human_df2['dbXrefs'] = ncbi_human_df2['dbXrefs'].str.replace('AllianceGenome:HGNC:', 'AG:')
            ncbi_human_df2['dbXrefs'] = ncbi_human_df2['dbXrefs'].str.split('|')
            processing_steps.append("Cleaned up dbXrefs field (replaced prefixes and split on '|').")

            exploded_data_manual = ncbi_human_df2.explode('dbXrefs')
            exploded_data_manual.sort_values('dbXrefs', inplace=True)
            exploded_data_manual.dropna(inplace=True)
            exploded_data_manual['source'] = exploded_data_manual['dbXrefs'].str.split(':').str[0]
            processing_steps.append("Exploded dbXrefs and extracted source information.")

            pivot_data = exploded_data_manual.pivot_table(
                index='NCBI_id',
                columns='source',
                values='dbXrefs',
                aggfunc=lambda x: '|'.join(x)
            )
            pivot_data.reset_index(inplace=True)
            pivot_data.rename(columns={
                "Ensembl": "ensembl_gene_id",
                "HGNC": "hgnc_id",
                "MIM": "mim_id",
                "miRBase": "miR_id",
                "IMGT/GENE-DB": "imgt_id"
            }, inplace=True)
            # Remove the 'Ensembl:' prefix if present
            if 'ensembl_gene_id' in pivot_data.columns:
                pivot_data['ensembl_gene_id'] = pivot_data['ensembl_gene_id'].str.replace('Ensembl:', '')
            processing_steps.append("Pivoted exploded data to obtain dbXrefs by source.")

            ncbi_df_final = pivot_data[['NCBI_id', 'hgnc_id', 'ensembl_gene_id', 'mim_id', 'miR_id', 'imgt_id']]
            merged_data = pd.merge(
                exploded_data_manual[['NCBI_id', 'symbol', 'synonyms', 'chromosome', 'map_location', 'description', 'type_of_gene', 'Modification_date', 'Feature_type']],
                ncbi_df_final,
                how='right',
                on='NCBI_id'
            ).drop_duplicates()
            processing_steps.append("Merged pivoted dbXrefs with gene info.")

            # Split and explode the 'ensembl_gene_id' column if it contains multiple IDs
            if 'ensembl_gene_id' in merged_data.columns:
                merged_data['ensembl_gene_id'] = merged_data['ensembl_gene_id'].str.split('|')
                merged_data = merged_data.explode('ensembl_gene_id')
            processing_steps.append("Split and exploded ensembl_gene_id column.")

            # Report unique Ensembl gene ID count
            unique_ensembl_gene_ids = merged_data['ensembl_gene_id'].nunique() if 'ensembl_gene_id' in merged_data.columns else None
            logging.info(f"Unique ensembl_gene_ids in the NCBI dataset: {unique_ensembl_gene_ids}")

            # Rename columns with the 'ncbi_' prefix
            rename_prefix = {
                'NCBI_id': 'ncbi_NCBI_id',
                'symbol': 'ncbi_symbol',
                'synonyms': 'ncbi_synonyms',
                'chromosome': 'ncbi_chromosome',
                'map_location': 'ncbi_location',
                'description': 'ncbi_description',
                'type_of_gene': 'ncbi_gene_type',
                'Modification_date': 'ncbi_Modification_date',
                'Feature_type': 'ncbi_Feature_type',
                'hgnc_id':'ncbi_hgnc_id',
                'ensembl_gene_id':'ncbi_ensembl_gene_id',
                'mim_id':'ncbi_mim_id', 
                'miR_id':'ncbi_miR_id', 
                'imgt_id':'ncbi_imgt_id'
            }
            merged_data.rename(columns=rename_prefix, inplace=True)
            processing_steps.append(f"Renamed final columns with prefix using mapping: {rename_prefix}")

            end_time = datetime.now()
            logging.info(f"NCBI data transformed in {(end_time - start_time).total_seconds()} seconds")
            return merged_data, start_time, end_time, processing_steps

        except Exception as e:
            logging.error(f"Error during NCBI transformation: {e}")
            raise e

    def run(self):
        try:
            # Read the decompressed NCBI file
            ncbi_df = pd.read_csv(self.input_file, sep="\t", dtype=str)
            num_records_input = len(ncbi_df)
        except Exception as e:
            logging.error(f"Error reading file {self.input_file}: {e}")
            return

        transformed_df, start_time, end_time, processing_steps = self.transform_and_clean_ncbi_data(ncbi_df)
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        transformed_df.to_csv(self.output_file, index=False)
        logging.info(f"Transformed NCBI data saved to: {self.output_file}")

        # === DIFF LOGIC ON CLEANED OUTPUT ===
        qc_dir = "src/data/publicdata/target_data/qc"
        os.makedirs(qc_dir, exist_ok=True)
        base = os.path.splitext(os.path.basename(self.output_file))[0]
        backup_path = os.path.join(qc_dir, f"{base}.backup.csv")
        diff_csv_path = os.path.join(qc_dir, f"{base}_diff.csv")

        if os.path.exists(backup_path):
            try:
                old_df = pd.read_csv(backup_path, dtype=str).fillna("")
                new_df = transformed_df.fillna("")

                join_col = "ncbi_NCBI_id" if "ncbi_NCBI_id" in transformed_df.columns else None
                if join_col:
                    old_df.set_index(join_col, inplace=True)
                    new_df.set_index(join_col, inplace=True)

                # Align columns and index for comparison
                common_cols = sorted(set(old_df.columns).intersection(set(new_df.columns)))
                old_df = old_df[common_cols]
                new_df = new_df[common_cols]
                old_df.sort_index(inplace=True)
                new_df.sort_index(inplace=True)

                diff_df = old_df.compare(new_df, keep_shape=False, keep_equal=False)
                if not diff_df.empty:
                    diff_df.to_csv(diff_csv_path)
                    logging.info(f"✅ Diff written to {diff_csv_path}")
                else:
                    logging.info("✅ No differences found in cleaned NCBI output.")
            except Exception as e:
                logging.warning(f"⚠️ Could not generate diff on cleaned output: {e}")

        # Always update backup for next run
        transformed_df.to_csv(backup_path, index=False)

        # === METADATA ===
        meta = {
            "timestamp": datetime.now().isoformat(),
            "input_file": self.input_file,
            "output_file": self.output_file,
            "num_records_input": num_records_input,
            "num_records_output": len(transformed_df),
            "transformation_start": start_time.isoformat(),
            "transformation_end": end_time.isoformat(),
            "transformation_duration_seconds": (end_time - start_time).total_seconds(),
            "processing_steps": processing_steps
        }
        with open(self.metadata_file, "w") as f:
            json.dump(meta, f, indent=2)
        logging.info(f"Metadata saved to {self.metadata_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform and clean NCBI gene_info data")
    parser.add_argument("--config", type=str, default="config/targets_config.yaml",
                        help="Path to YAML config file")
    args = parser.parse_args()
    
    with open(args.config) as f:
        config = yaml.safe_load(f)
    
    NCBITransformer(config).run()
