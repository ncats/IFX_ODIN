#!/usr/bin/env python
"""
ensembl_transform.py - Transform, clean, and merge Ensembl BioMart CSV parts
"""

import os
import yaml
import json
import logging
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class EnsemblTransformer:
    def __init__(self, full_cfg):
        cfg                   = full_cfg["ensembl_data"]
        self.inputs           = cfg["output_paths"]["biomart_csvs"]
        self.final_output     = cfg["output_paths"]["final_merged"]
        self.biomart_queries  = cfg.get("biomart_queries", [])
        self.metadata_file    = cfg.get("tf_metadata_file")
        self.metadata         = {
            "timestamp":        {"start": datetime.now().isoformat()},
            "input_files":      self.inputs,
            "biomart_queries":  self.biomart_queries,
            "processing_steps": {},
            "final_output":     self.final_output
        }
        self.logger = logging.getLogger("EnsemblTransformer")

    def merge_dataframes(self, a, b, on, step):
        before_a, before_b = len(a), len(b)
        m = pd.merge(a, b, on=on, how="outer").drop_duplicates()
        self.metadata["processing_steps"][step] = {
            "merge_on":        on,
            "records_a":       before_a,
            "records_b":       before_b,
            "records_merged":  len(m)
        }
        return m

    def concat_if_duplicate(self, df):
        df["ensembl_synonyms"] = (
            df.groupby("ensembl_transcript_id_version")["ensembl_synonyms"]
              .transform(lambda x: "|".join(v for v in x if v and v!="nan"))
        )
        df.drop_duplicates(subset=["ensembl_transcript_id_version"], inplace=True)
        self.metadata["processing_steps"]["concat_synonyms"] = {"records": len(df)}
        return df

    def process_df3(self, df3):
        for col in ["ensembl_refseq_NR","ensembl_refseq_NM","ensembl_refseq_NP"]:
            df3[col] = (
                df3.groupby("ensembl_transcript_id_version")[col]
                   .transform(lambda x: "|".join(x.dropna().unique()))
            )
        df3.drop_duplicates(subset=["ensembl_transcript_id_version"], inplace=True)
        self.metadata["processing_steps"]["process_df3"] = {"records": len(df3)}
        return df3

    def run(self):
        self.logger.info("Loading input CSV parts…")
        df1 = pd.read_csv(self.inputs[0], dtype=str)
        df2 = pd.read_csv(self.inputs[1], dtype=str)
        print("🧪 df2 columns:", df2.columns.tolist())

        df3 = pd.read_csv(self.inputs[2], dtype=str)
        df4 = pd.read_csv(self.inputs[3], dtype=str)
        self.metadata["processing_steps"]["input_counts"] = {
            "df1":len(df1),"df2":len(df2),"df3":len(df3),"df4":len(df4)
        }

        df1.rename(columns={
            "Gene stable ID":"ensembl_gene_id",
            "Gene stable ID version":"ensembl_gene_id_version",
            "Transcript stable ID":"ensembl_transcript_id",
            "Transcript stable ID version":"ensembl_transcript_id_version",
            "Protein stable ID":"ensembl_peptide_id",
            "Protein stable ID version":"ensembl_peptide_id_version",
            "Gene name":"ensembl_symbol",
            "Gene type":"ensembl_gene_type",
            "Ensembl Canonical":"ensembl_canonical",
            "Gene Synonym":"ensembl_synonyms",
            "Transcript support level (TSL)":"ensembl_transcript_tsl",
            "NCBI gene (formerly Entrezgene) ID":"ensembl_NCBI_id",
            "HGNC ID":"ensembl_hgnc_id"
        }, inplace=True)

        df2.rename(columns={
            "Gene stable ID":"ensembl_gene_id",
            "Gene stable ID version":"ensembl_gene_id_version",
            "Transcript stable ID":"ensembl_transcript_id",
            "Transcript stable ID version":"ensembl_transcript_id_version",
            "Protein stable ID":"ensembl_peptide_id",
            "Protein stable ID version":"ensembl_peptide_id_version",
            "UniProtKB/Swiss-Prot ID":"ensembl_uniprot_id",
            "UniProtKB/TrEMBL ID":"ensembl_trembl_id",
            "UniProtKB isoform ID":"ensembl_uniprot_isoform"
        }, inplace=True)

        df3.rename(columns={
            "Gene stable ID":"ensembl_gene_id",
            "Gene stable ID version":"ensembl_gene_id_version",
            "Transcript stable ID":"ensembl_transcript_id",
            "Transcript stable ID version":"ensembl_transcript_id_version",
            "RefSeq match transcript (MANE Select)":"ensembl_refseq_MANEselect",
            "RefSeq mRNA ID":"ensembl_refseq_NM",
            "RefSeq ncRNA ID":"ensembl_refseq_NR",
            "RefSeq peptide ID":"ensembl_refseq_NP"
        }, inplace=True)

        df4.rename(columns={
            "Gene stable ID":"ensembl_gene_id",
            "Gene stable ID version":"ensembl_gene_id_version",
            "Gene description":"ensembl_description",
            "Chromosome/scaffold name":"ensembl_location",
            "Strand":"ensembl_strand",
            "Gene start (bp)":"ensembl_start",
            "Gene end (bp)":"ensembl_end"
        }, inplace=True)

        df2 = df2[["ensembl_transcript_id_version","ensembl_uniprot_id","ensembl_trembl_id","ensembl_uniprot_isoform"]]
        df3 = self.process_df3(df3)
        df4 = df4[["ensembl_gene_id_version","ensembl_description","ensembl_location","ensembl_strand","ensembl_start","ensembl_end"]]
        df4["ensembl_description"] = df4["ensembl_description"].str.split("[").str[0].str.strip()
        df1["ensembl_synonyms"] = df1["ensembl_synonyms"].fillna("").astype(str)
        df1 = self.concat_if_duplicate(df1)

        m12    = self.merge_dataframes(df1, df2, on="ensembl_transcript_id_version", step="merge_df1_df2")
        m123   = self.merge_dataframes(m12, df3, on="ensembl_transcript_id_version", step="merge_with_df3")
        m123   = m123.loc[:, ~m123.columns.str.endswith("_y")].rename(columns=lambda c: c.rstrip("_x"))
        final  = self.merge_dataframes(m123, df4, on="ensembl_gene_id_version", step="merge_with_df4")

        os.makedirs(os.path.dirname(self.final_output), exist_ok=True)
        final.to_csv(self.final_output, index=False)
        self.logger.info("Saved final merged to %s", self.final_output)

        # === DIFF LOGIC ON CLEANED OUTPUT ===
        qc_dir = "src/data/publicdata/target_data/qc"
        os.makedirs(qc_dir, exist_ok=True)
        base = os.path.splitext(os.path.basename(self.final_output))[0]
        backup_path = os.path.join(qc_dir, f"{base}.backup.csv")
        diff_csv_path = os.path.join(qc_dir, f"{base}_diff.csv")

        if os.path.exists(backup_path):
            try:
                old_df = pd.read_csv(backup_path, dtype=str).fillna("")
                new_df = final.fillna("")

                join_col = "ensembl_transcript_id_version" if "ensembl_transcript_id_version" in final.columns else None
                try:
                    old_df.set_index(join_col, inplace=True)
                    new_df.set_index(join_col, inplace=True)
                except Exception:
                    pass  # fallback to index comparison

                diff_df = old_df.compare(new_df, keep_shape=False, keep_equal=False)
                if not diff_df.empty:
                    diff_df.to_csv(diff_csv_path)
                    self.logger.info("✅ Diff written to %s", diff_csv_path)
                else:
                    self.logger.info("✅ No differences found in cleaned output.")
            except Exception as e:
                self.logger.warning("⚠️ Could not generate diff on cleaned output: %s", e)

        # Always update backup for next run
        final.to_csv(backup_path, index=False)

        # === Save metadata ===
        meta = {
            "timestamp": {
                "start": self.metadata["timestamp"]["start"],
                "end":   datetime.now().isoformat()
            },
            "input_files":       self.inputs,
            "biomart_queries":   self.biomart_queries,
            "processing_steps":  self.metadata["processing_steps"],
            "record_counts": {
                "after_df1":      len(df1),
                "after_df2":      len(df2),
                "after_df3":      len(df3),
                "after_df4":      len(df4),
                "after_merge":    len(final)
            },
            "final_output":      self.final_output
        }
        if self.metadata_file:
            os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
            with open(self.metadata_file, "w") as mf:
                json.dump(meta, mf, indent=2)
            self.logger.info("Metadata saved to %s", self.metadata_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform Ensembl BioMart CSVs")
    parser.add_argument("--config", type=str,
                        default="config/targets_config.yaml")
    args = parser.parse_args()
    with open(args.config) as f:
        cfg = yaml.safe_load(f)
    EnsemblTransformer(cfg).run()
