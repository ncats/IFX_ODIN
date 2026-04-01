#!/usr/bin/env python
"""
ensembl_transform.py - Transform, clean, and merge Ensembl BioMart CSV parts.

Core merge logic is UNCHANGED from the original working pipeline.
Diff tracking uses shared entity_diff on the CLEANED output only.
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

from publicdata.target_data.shared.entity_diff import run_entity_diff, archive_cleaned_output
from publicdata.target_data.shared.output_versioning import save_versioned_output
from publicdata.target_data.download_utils import setup_logging

setup_logging()


class EnsemblTransformer:
    def __init__(self, full_cfg):
        cfg = full_cfg["ensembl_data"]
        self.inputs = cfg["output_paths"]["biomart_csvs"]
        self.final_output = cfg["output_paths"]["final_merged"]
        self.metadata_file = cfg.get("tf_metadata_file")
        self.diff_json = cfg.get(
            "entity_diff_output",
            "src/data/publicdata/target_data/qc/ensembl_entity_diff.qc.json",
        )
        self.transform_archive_dir = cfg.get(
            "transform_archive_dir",
            "src/data/publicdata/target_data/archive/cleaned/ensembl",
        )
        self.metadata = {
            "timestamp": {"start": datetime.now().isoformat()},
            "input_files": self.inputs,
            "processing_steps": {},
            "final_output": self.final_output,
            "archived_output": None,
            "summary": {},
        }
        self.logger = logging.getLogger("EnsemblTransformer")

    @staticmethod
    def _first_line(path):
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as handle:
                return handle.readline().strip()
        except Exception:
            return ""

    def _require_columns(self, df, required, path, label):
        missing = [col for col in required if col not in df.columns]
        if missing:
            first_line = self._first_line(path)
            raise RuntimeError(
                f"{label} is missing required columns {missing}. "
                f"Input file: {path}. First line: {first_line[:500]}"
            )

    # ── Merge helpers (UNCHANGED from original) ──────────────────────────

    def merge_dataframes(self, a, b, on, step):
        before_a, before_b = len(a), len(b)
        m = pd.merge(a, b, on=on, how="outer").drop_duplicates()
        self.metadata["processing_steps"][step] = {
            "merge_on": on,
            "records_a": before_a,
            "records_b": before_b,
            "records_merged": len(m),
        }
        return m

    def concat_if_duplicate(self, df):
        df["ensembl_synonyms"] = (
            df.groupby("ensembl_transcript_id_version")["ensembl_synonyms"]
            .transform(lambda x: "|".join(v for v in x if v and v != "nan"))
        )
        df.drop_duplicates(subset=["ensembl_transcript_id_version"], inplace=True)
        self.metadata["processing_steps"]["concat_synonyms"] = {"records": len(df)}
        return df

    def process_df3(self, df3):
        for col in ["ensembl_refseq_NR", "ensembl_refseq_NM", "ensembl_refseq_NP"]:
            df3[col] = (
                df3.groupby("ensembl_transcript_id_version")[col]
                .transform(lambda x: "|".join(x.dropna().unique()))
            )
        df3.drop_duplicates(subset=["ensembl_transcript_id_version"], inplace=True)
        self.metadata["processing_steps"]["process_df3"] = {"records": len(df3)}
        return df3

    # ── Main run ─────────────────────────────────────────────────────────

    def run(self):
        self.logger.info("Loading input CSV parts…")
        df1 = pd.read_csv(self.inputs[0], dtype=str)
        df2 = pd.read_csv(self.inputs[1], dtype=str)
        df3 = pd.read_csv(self.inputs[2], dtype=str)
        df4 = pd.read_csv(self.inputs[3], dtype=str)

        self.metadata["processing_steps"]["input_counts"] = {
            "df1": len(df1), "df2": len(df2), "df3": len(df3), "df4": len(df4),
        }

        # ── Column renames (UNCHANGED) ───────────────────────────────────
        df1.rename(columns={
            "Gene stable ID": "ensembl_gene_id",
            "Gene stable ID version": "ensembl_gene_id_version",
            "Transcript stable ID": "ensembl_transcript_id",
            "Transcript stable ID version": "ensembl_transcript_id_version",
            "Protein stable ID": "ensembl_peptide_id",
            "Protein stable ID version": "ensembl_peptide_id_version",
            "Gene name": "ensembl_symbol",
            "Gene type": "ensembl_gene_type",
            "Ensembl Canonical": "ensembl_canonical",
            "Gene Synonym": "ensembl_synonyms",
            "Transcript support level (TSL)": "ensembl_transcript_tsl",
            "NCBI gene (formerly Entrezgene) ID": "ensembl_NCBI_id",
            "HGNC ID": "ensembl_hgnc_id",
        }, inplace=True)

        df2.rename(columns={
            "Gene stable ID": "ensembl_gene_id",
            "Gene stable ID version": "ensembl_gene_id_version",
            "Transcript stable ID": "ensembl_transcript_id",
            "Transcript stable ID version": "ensembl_transcript_id_version",
            "Protein stable ID": "ensembl_peptide_id",
            "Protein stable ID version": "ensembl_peptide_id_version",
            "UniProtKB/Swiss-Prot ID": "ensembl_uniprot_id",
            "UniProtKB/TrEMBL ID": "ensembl_trembl_id",
            "UniProtKB isoform ID": "ensembl_uniprot_isoform",
        }, inplace=True)

        df3.rename(columns={
            "Gene stable ID": "ensembl_gene_id",
            "Gene stable ID version": "ensembl_gene_id_version",
            "Transcript stable ID": "ensembl_transcript_id",
            "Transcript stable ID version": "ensembl_transcript_id_version",
            "RefSeq match transcript (MANE Select)": "ensembl_refseq_MANEselect",
            "RefSeq mRNA ID": "ensembl_refseq_NM",
            "RefSeq ncRNA ID": "ensembl_refseq_NR",
            "RefSeq peptide ID": "ensembl_refseq_NP",
        }, inplace=True)

        df4.rename(columns={
            "Gene stable ID": "ensembl_gene_id",
            "Gene stable ID version": "ensembl_gene_id_version",
            "Gene description": "ensembl_description",
            "Chromosome/scaffold name": "ensembl_location",
            "Strand": "ensembl_strand",
            "Gene start (bp)": "ensembl_start",
            "Gene end (bp)": "ensembl_end",
        }, inplace=True)

        if "ensembl_synonyms" not in df1.columns:
            df1["ensembl_synonyms"] = ""

        self._require_columns(
            df1,
            [
                "ensembl_gene_id",
                "ensembl_gene_id_version",
                "ensembl_transcript_id",
                "ensembl_transcript_id_version",
                "ensembl_peptide_id",
                "ensembl_peptide_id_version",
                "ensembl_symbol",
                "ensembl_gene_type",
                "ensembl_canonical",
                "ensembl_transcript_tsl",
                "ensembl_NCBI_id",
                "ensembl_hgnc_id",
                "ensembl_synonyms",
            ],
            self.inputs[0],
            "Ensembl BioMart part 1",
        )
        self._require_columns(
            df2,
            [
                "ensembl_transcript_id_version",
                "ensembl_uniprot_id",
                "ensembl_trembl_id",
                "ensembl_uniprot_isoform",
            ],
            self.inputs[1],
            "Ensembl BioMart part 2",
        )
        self._require_columns(
            df3,
            [
                "ensembl_transcript_id_version",
                "ensembl_refseq_MANEselect",
                "ensembl_refseq_NM",
                "ensembl_refseq_NR",
                "ensembl_refseq_NP",
            ],
            self.inputs[2],
            "Ensembl BioMart part 3",
        )
        self._require_columns(
            df4,
            [
                "ensembl_gene_id_version",
                "ensembl_description",
                "ensembl_location",
                "ensembl_strand",
                "ensembl_start",
                "ensembl_end",
            ],
            self.inputs[3],
            "Ensembl BioMart part 4",
        )

        # ── Subsetting and cleaning (UNCHANGED) ─────────────────────────
        df2 = df2[["ensembl_transcript_id_version", "ensembl_uniprot_id",
                    "ensembl_trembl_id", "ensembl_uniprot_isoform"]]
        df3 = self.process_df3(df3)
        df4 = df4[["ensembl_gene_id_version", "ensembl_description",
                    "ensembl_location", "ensembl_strand", "ensembl_start", "ensembl_end"]]
        df4["ensembl_description"] = df4["ensembl_description"].str.split("[").str[0].str.strip()
        df1["ensembl_synonyms"] = df1["ensembl_synonyms"].fillna("").astype(str)
        df1 = self.concat_if_duplicate(df1)

        # ── Merges (UNCHANGED) ───────────────────────────────────────────
        m12 = self.merge_dataframes(df1, df2, on="ensembl_transcript_id_version", step="merge_df1_df2")
        m123 = self.merge_dataframes(m12, df3, on="ensembl_transcript_id_version", step="merge_with_df3")
        m123 = m123.loc[:, ~m123.columns.str.endswith("_y")].rename(columns=lambda c: c.rstrip("_x"))
        final = self.merge_dataframes(m123, df4, on="ensembl_gene_id_version", step="merge_with_df4")

        # ── Write final output ───────────────────────────────────────────
        ver_result = save_versioned_output(
            df=final,
            output_path=self.final_output,
            id_col=None,
            sep=",",
            write_diff=False,
            archive_dir=self.transform_archive_dir,
            output_kind="cleaned_source_table",
        )
        self.logger.info("Saved final merged to %s", self.final_output)

        # ── Entity diff on CLEANED output ────────────────────────────────
        backup_path = Path("src/data/publicdata/target_data/qc") / f"{Path(self.final_output).stem}.backup.csv"
        diff_summary = run_entity_diff(
            new_df=final,
            id_col="ensembl_transcript_id_version",
            backup_path=str(backup_path),
            diff_json_path=self.diff_json,
        )

        # ── Archive cleaned output ───────────────────────────────────────
        archive_path = ver_result["archive_path"] or archive_cleaned_output(
            final, self.transform_archive_dir, Path(self.final_output).name
        )
        self.metadata["archived_output"] = archive_path

        # ── Summary for version manifest ─────────────────────────────────
        self.metadata["summary"] = {
            "final_rows": len(final),
            "n_added_ids": diff_summary["n_added"] if diff_summary else 0,
            "n_removed_ids": diff_summary["n_removed"] if diff_summary else 0,
            "n_field_changes": diff_summary["n_field_changes"] if diff_summary else 0,
            "entity_diff_file": self.diff_json if diff_summary else None,
        }

        # ── Save metadata ────────────────────────────────────────────────
        meta = {
            "timestamp": {
                "start": self.metadata["timestamp"]["start"],
                "end": datetime.now().isoformat(),
            },
            "input_files": self.inputs,
            "processing_steps": self.metadata["processing_steps"],
            "record_counts": {
                "after_df1": len(df1), "after_df2": len(df2),
                "after_df3": len(df3), "after_df4": len(df4),
                "after_merge": len(final),
            },
            "final_output": self.final_output,
            "archived_output": archive_path,
            "output_versioning": ver_result,
            "summary": self.metadata["summary"],
        }
        if self.metadata_file:
            os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
            with open(self.metadata_file, "w") as mf:
                json.dump(meta, mf, indent=2)
            self.logger.info("Metadata saved to %s", self.metadata_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform Ensembl BioMart CSVs")
    parser.add_argument("--config", type=str, default="config/targets_config.yaml")
    args = parser.parse_args()
    with open(args.config) as f:
        cfg = yaml.safe_load(f)
    EnsemblTransformer(cfg).run()
