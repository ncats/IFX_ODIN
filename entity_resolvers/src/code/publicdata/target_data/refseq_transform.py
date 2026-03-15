#!/usr/bin/env python
"""
refseq_transform.py — Transform, clean, and process RefSeq data.

Core transformation logic restored from the original working pipeline:
  1) Rename columns
  2) Generate concatenated RNA and protein files (grouped by gene)
  3) Entity diff runs on the GENE-LEVEL concatenated data, not the 900k raw rows
"""

import os
import yaml
import json
import pandas as pd
import logging
import argparse
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True,
)


class RefSeqTransformer:
    def __init__(self, full_config):
        self.config = full_config["refseq_data"]
        self.decompressed = self.config["refseq"]["decompressed"]
        self.transformed_data_path = self.config["transformed_data_path"]
        self.metadata_file = self.config.get("tf_metadata_file", "tf_refseq_metadata.json")
        self.entity_diff_file = self.config.get(
            "entity_diff_output",
            "src/data/publicdata/target_data/qc/refseq_entity_diff.qc.json",
        )
        self.archive_dir = self.config.get(
            "transform_archive_dir",
            "src/data/publicdata/target_data/archive/cleaned/refseq",
        )
        self.metadata = {
            "timestamp": {"start": datetime.now().isoformat()},
            "processing_steps": [],
            "outputs": [],
        }

    def fetch_and_process_data(self):
        """Load and rename columns (UNCHANGED from original)."""
        logging.info(f"Loading RefSeq data from {self.decompressed}...")
        df = pd.read_csv(self.decompressed, sep="\t", dtype=str)
        records_before = len(df)

        rename_map = {
            '#tax_id': 'refseq_tax_id', 'GeneID': 'refseq_ncbi_id',
            'Symbol': 'refseq_symbol', 'status': 'refseq_status',
            'Synonyms': 'refseq_synonyms', 'dbXrefs': 'refseq_dbxrefs',
            'chromosome': 'refseq_chromosome', 'map_location': 'refseq_location',
            'description': 'refseq_description', 'type_of_gene': 'refseq_gene_type',
            'Modification_date': 'refseq_Modification_date',
            'Feature_type': 'refseq_feature_type',
            'RNA_nucleotide_accession.version': 'refseq_rna_id',
            'protein_accession.version': 'refseq_protein_id',
        }
        df.rename(columns=rename_map, inplace=True)

        os.makedirs(os.path.dirname(self.transformed_data_path), exist_ok=True)
        df.to_csv(self.transformed_data_path, index=False)

        self.metadata["processing_steps"].append({
            "step": "rename_columns", "records": records_before,
        })
        return df

    def generate_concatenated_files(self, df):
        """Generate gene-level concatenated RNA and protein files (RESTORED from original)."""
        # RNA
        logging.info("Generating concatenated RNA IDs file...")
        df_rna = df[df['refseq_rna_id'] != '-']
        rna_grouped = df_rna.groupby('refseq_ncbi_id')['refseq_rna_id'] \
            .agg(lambda x: '|'.join(x.dropna().unique())).reset_index()
        rna_path = self.config['refseq']['rna_concatenated_path']
        os.makedirs(os.path.dirname(rna_path), exist_ok=True)
        rna_grouped.to_csv(rna_path, index=False)
        logging.info(f"RNA concatenated: {len(rna_grouped)} genes → {rna_path}")

        self.metadata["outputs"].append({
            "name": "RefSeq RNA Concatenated", "path": rna_path, "records": len(rna_grouped),
        })

        # Protein
        logging.info("Generating concatenated protein IDs file...")
        df_prot = df[df['refseq_protein_id'] != '-']
        prot_grouped = df_prot.groupby('refseq_ncbi_id')['refseq_protein_id'] \
            .agg(lambda x: ';'.join(x.dropna().unique())).reset_index()
        prot_path = self.config['refseq']['protein_concatenated_path']
        os.makedirs(os.path.dirname(prot_path), exist_ok=True)
        prot_grouped.to_csv(prot_path, index=False)
        logging.info(f"Protein concatenated: {len(prot_grouped)} genes → {prot_path}")

        self.metadata["outputs"].append({
            "name": "RefSeq Protein Concatenated", "path": prot_path, "records": len(prot_grouped),
        })

        return rna_grouped, prot_grouped

    def compute_entity_diff(self, old_df, new_df):
        """
        Vectorized entity diff on gene-level data.
        Operates on the concatenated RNA file (~20-40k rows), not the raw 900k rows.
        """
        old_df = old_df.fillna("").astype(str)
        new_df = new_df.fillna("").astype(str)

        id_col = "refseq_ncbi_id"
        old_ids = set(old_df[id_col])
        new_ids = set(new_df[id_col])

        added = sorted(new_ids - old_ids)
        removed = sorted(old_ids - new_ids)

        # Vectorized field comparison on common IDs
        common_ids = old_ids & new_ids
        old_common = old_df[old_df[id_col].isin(common_ids)].drop_duplicates(subset=[id_col]).set_index(id_col).sort_index()
        new_common = new_df[new_df[id_col].isin(common_ids)].drop_duplicates(subset=[id_col]).set_index(id_col).sort_index()

        # Align indexes
        shared_idx = old_common.index.intersection(new_common.index)
        shared_cols = [c for c in old_common.columns if c in new_common.columns]

        field_changes = []
        if shared_idx.any() and shared_cols:
            old_aligned = old_common.loc[shared_idx, shared_cols]
            new_aligned = new_common.loc[shared_idx, shared_cols]
            diff_mask = old_aligned != new_aligned

            for col in shared_cols:
                changed_ids = diff_mask.index[diff_mask[col]]
                for gid in changed_ids:
                    field_changes.append({
                        id_col: gid,
                        "field": col,
                        "old": old_aligned.at[gid, col],
                        "new": new_aligned.at[gid, col],
                    })

        return {
            "added_ids": added,
            "removed_ids": removed,
            "field_changes": field_changes[:1000],  # cap to avoid giant JSON
            "n_added_ids": len(added),
            "n_removed_ids": len(removed),
            "n_field_changes": len(field_changes),
        }

    def archive_output(self, df):
        version = datetime.now().strftime("%Y%m%d")
        archive_path = Path(self.archive_dir) / version
        archive_path.mkdir(parents=True, exist_ok=True)
        outfile = archive_path / Path(self.transformed_data_path).name
        df.to_csv(outfile, index=False)
        return str(outfile)

    def run(self):
        df = self.fetch_and_process_data()
        if df is None:
            logging.error("Transformation failed.")
            return

        rna_grouped, prot_grouped = self.generate_concatenated_files(df)

        # Entity diff on the gene-level RNA concatenated file (not the 900k raw rows)
        qc_dir = "src/data/publicdata/target_data/qc"
        os.makedirs(qc_dir, exist_ok=True)
        backup_file = os.path.join(qc_dir, "refseq_rna_concatenated.backup.csv")

        diff_summary = None
        if os.path.exists(backup_file):
            try:
                old_df = pd.read_csv(backup_file, dtype=str)
                diff_summary = self.compute_entity_diff(old_df, rna_grouped)
                os.makedirs(os.path.dirname(self.entity_diff_file), exist_ok=True)
                with open(self.entity_diff_file, "w") as f:
                    json.dump(diff_summary, f, indent=2)
                logging.info(f"Entity diff written → {self.entity_diff_file}")
            except Exception as e:
                logging.warning(f"Could not generate entity diff: {e}")

        # Update backup with current gene-level data
        rna_grouped.to_csv(backup_file, index=False)

        archive_path = self.archive_output(df)

        self.metadata["timestamp"]["end"] = datetime.now().isoformat()
        self.metadata["output_file"] = self.transformed_data_path
        self.metadata["archived_output"] = archive_path
        self.metadata["num_records_input"] = len(df)
        self.metadata["num_records_output"] = len(df)
        self.metadata["summary"] = {
            "final_rows": len(df),
            "gene_count_rna": len(rna_grouped),
            "gene_count_protein": len(prot_grouped),
            "n_added_ids": diff_summary["n_added_ids"] if diff_summary else 0,
            "n_removed_ids": diff_summary["n_removed_ids"] if diff_summary else 0,
            "n_field_changes": diff_summary["n_field_changes"] if diff_summary else 0,
            "entity_diff_file": self.entity_diff_file if diff_summary else None,
        }

        os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
        with open(self.metadata_file, 'w') as mf:
            json.dump(self.metadata, mf, indent=2)
        logging.info("RefSeq transform metadata written")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform RefSeq data")
    parser.add_argument("--config", type=str, default="config/targets_config.yaml")
    args = parser.parse_args()
    with open(args.config) as f:
        cfg = yaml.safe_load(f)
    RefSeqTransformer(cfg).run()