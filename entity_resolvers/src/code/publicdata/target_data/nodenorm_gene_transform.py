#!/usr/bin/env python3
"""
nodenorm_gene_transform.py — Parse NodeNorm gene dump into structured CSV.

Updates in this version:
  - keeps original flatten/clean/rename behavior
  - removes hard-coded qc paths
  - respects global or section-level qc_mode
  - supports config-driven backup/diff paths when provided
  - carries downloader metadata/version into transform metadata
  - records hashes, sizes, row counts, and optional diff output
"""

import os
import re
import json
import yaml
import argparse
import hashlib
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler

import pandas as pd

from publicdata.target_data.download_utils import setup_logging
from publicdata.target_data.shared.output_versioning import save_versioned_output


def compute_md5(path: str) -> str:
    digest = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(4096), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_mkdir_for_file(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _sanitize_stem(path: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", os.path.splitext(os.path.basename(path))[0])


class NodeNormGeneTransformer:
    def __init__(self, config: dict):
        self.config = config
        c = config["nodenorm_genes"]

        self.input_file = c["raw_file"]
        self.output_file = c["output_file"]
        self.metadata_file = c["tf_metadata_file"]
        self.download_metadata_file = c.get("dl_metadata_file")
        self.transform_archive_dir = c.get("transform_archive_dir")

        self.log_file = c.get("transform_log_file", c.get("log_file"))
        if not self.log_file:
            self.log_file = os.path.join(
                os.path.dirname(self.metadata_file),
                "nodenorm_gene_transform.log",
            )

        self.qc_mode = bool(c.get("qc_mode", config.get("qc_mode", False)))
        self.qc_dir = c.get(
            "qc_dir",
            os.path.join(os.path.dirname(self.output_file), "qc"),
        )

        base = _sanitize_stem(self.output_file)
        self.backup_file = c.get("backup_file", os.path.join(self.qc_dir, f"{base}.backup.csv"))
        self.diff_file = c.get("diff_file", os.path.join(self.qc_dir, f"{base}.diff.csv"))

        _safe_mkdir_for_file(self.metadata_file)
        setup_logging(self.log_file)
        logging.info("Logging to %s", self.log_file)

    def _load_download_metadata(self) -> dict:
        if not self.download_metadata_file or not os.path.exists(self.download_metadata_file):
            return {}
        try:
            with open(self.download_metadata_file, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception as exc:
            logging.warning(
                "Could not load downloader metadata %s: %s",
                self.download_metadata_file,
                exc,
            )
            return {}

    def _flatten_record(self, record: dict) -> dict:
        flat = {
            "type": record.get("type", "").strip(),
            "ic": record.get("ic"),
            "preferred_name": record.get("preferred_name", "").strip(),
            "taxa": ",".join(record.get("taxa", [])),
        }

        identifiers = record.get("identifiers", [])
        gene_id = None
        id_dict: dict[str, list[str]] = {}

        for identifier in identifiers:
            value = identifier.get("i", "").strip()
            if not value:
                continue
            if value.startswith("NCBIGene:"):
                gene_id = value
            db = value.split(":", 1)[0]
            id_dict.setdefault(db, []).append(value)

        flat["gene_id"] = gene_id or flat["preferred_name"]
        for db, values in id_dict.items():
            flat[db] = "|".join(values)
        return flat

    def _parse_json_lines(self) -> list[dict]:
        records: list[dict] = []
        logging.info("Reading JSON-lines from %s", self.input_file)
        with open(self.input_file, "r", encoding="utf-8") as fh:
            for line_number, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    records.append(self._flatten_record(obj))
                except json.JSONDecodeError as exc:
                    logging.error("JSON error line %s: %s", line_number, exc)
        logging.info("Parsed %s total records", len(records))
        return records

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Starting cleaning process on DataFrame")
        before = len(df)
        df = df.dropna(subset=["gene_id"])
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].str.strip().replace(r"^\s*$", None, regex=True)
        df = df.drop_duplicates()
        after = len(df)
        logging.info("Cleaned DF: %s duplicates/empties removed -> %s records", before - after, after)
        return df

    def _strip_prefixes(self, df: pd.DataFrame) -> pd.DataFrame:
        for prefix in ["NCBIGene", "ENSEMBL", "HGNC", "OMIM", "UMLS"]:
            if prefix in df.columns:
                df[prefix] = df[prefix].apply(
                    lambda value: "|".join(
                        [
                            part.split(":", 1)[1] if part.startswith(prefix + ":") else part
                            for part in str(value).split("|")
                        ]
                    )
                    if pd.notna(value)
                    else value
                )
        return df

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        mapping = {
            "gene_id": "NodeNorm_Gene",
            "type": "biolinkType",
            "preferred_name": "nodenorm_symbol",
            "NCBIGene": "nodenorm_NCBI_id",
            "ENSEMBL": "nodenorm_ensembl_gene_id",
            "HGNC": "nodenorm_HGNC",
            "UMLS": "nodenorm_UMLS",
            "OMIM": "nodenorm_OMIM",
        }
        return df.rename(columns=mapping)

    def _generate_diff(self, new_df: pd.DataFrame) -> str | None:
        if not self.qc_mode:
            logging.info("qc_mode is False; skipping backup/diff generation.")
            return None

        os.makedirs(self.qc_dir, exist_ok=True)
        diff_generated = None

        try:
            if os.path.exists(self.backup_file):
                old_df = pd.read_csv(self.backup_file, dtype=str).fillna("")
                compare_df = new_df.fillna("")

                join_col = "nodenorm_NCBI_id" if "nodenorm_NCBI_id" in compare_df.columns else None
                if join_col and join_col in old_df.columns:
                    old_df = old_df.set_index(join_col)
                    compare_df = compare_df.set_index(join_col)

                common_cols = sorted(set(old_df.columns).intersection(set(compare_df.columns)))
                if common_cols:
                    old_df = old_df[common_cols].sort_index()
                    compare_df = compare_df[common_cols].sort_index()
                    diff_df = old_df.compare(compare_df, keep_shape=False, keep_equal=False)
                    if not diff_df.empty:
                        _safe_mkdir_for_file(self.diff_file)
                        diff_df.to_csv(self.diff_file)
                        logging.info("Column diff written to %s", self.diff_file)
                        diff_generated = self.diff_file
                    else:
                        logging.info("No differences found from previous NodeNorm gene output.")
        except Exception as exc:
            logging.warning("Failed to generate diff: %s", exc)

        try:
            _safe_mkdir_for_file(self.backup_file)
            new_df.to_csv(self.backup_file, index=False)
        except Exception as exc:
            logging.warning("Failed to update backup file %s: %s", self.backup_file, exc)

        return diff_generated

    def run(self) -> None:
        start = datetime.now()

        records = self._parse_json_lines()
        raw_count = len(records)

        df = pd.DataFrame(records)
        cleaned_before = len(df)
        df = self._clean_dataframe(df)
        cleaned_after = len(df)

        df = self._strip_prefixes(df)
        df = self._rename_columns(df)

        if "nodenorm_HGNC" in df.columns:
            df["nodenorm_HGNC"] = df["nodenorm_HGNC"].apply(
                lambda value: f"HGNC:{value}"
                if pd.notnull(value)
                and str(value).strip()
                and not str(value).startswith("HGNC:")
                else value
            )

        df = df.drop(columns=["ic", "taxa", "NodeNorm_Gene"], errors="ignore")

        _safe_mkdir_for_file(self.output_file)
        ver_result = save_versioned_output(
            df=df,
            output_path=self.output_file,
            id_col="nodenorm_NCBI_id",
            sep=",",
            write_diff=False,
            archive_dir=self.transform_archive_dir,
            output_kind="cleaned_source_table",
        )
        out_count = len(df)
        logging.info("Saved %s gene records to %s", out_count, self.output_file)

        diff_generated = self._generate_diff(df)

        end = datetime.now()
        download_meta = self._load_download_metadata()

        meta = {
            "timestamp": {
                "start": start.isoformat(),
                "end": end.isoformat(),
                "duration_seconds": (end - start).total_seconds(),
            },
            "source": {
                "name": "NodeNorm",
                "version": download_meta.get("version"),
                "release_date": download_meta.get("release_date"),
                "download_metadata_file": self.download_metadata_file,
                "download_status": download_meta.get("status"),
            },
            "input": {
                "path": self.input_file,
                "size_bytes": os.path.getsize(self.input_file),
                "md5": compute_md5(self.input_file),
                "record_count_raw": raw_count,
            },
            "cleaning": {
                "before": cleaned_before,
                "after": cleaned_after,
            },
            "output": {
                "path": self.output_file,
                "size_bytes": os.path.getsize(self.output_file),
                "md5": compute_md5(self.output_file),
                "record_count": out_count,
                "columns": df.columns.tolist(),
                "output_versioning": ver_result,
            },
            "qc": {
                "qc_mode": self.qc_mode,
                "backup_file": self.backup_file if self.qc_mode else None,
                "diff_file": diff_generated,
            },
            "processing_steps": [
                {"step": "parse_json_lines", "records": raw_count},
                {"step": "clean_dataframe", "before": cleaned_before, "after": cleaned_after},
                {"step": "strip_prefixes"},
                {"step": "rename_columns"},
                {"step": "drop_columns"},
                {"step": "write_csv", "records": out_count},
            ],
        }

        _safe_mkdir_for_file(self.metadata_file)
        with open(self.metadata_file, "w", encoding="utf-8") as fh:
            json.dump(meta, fh, indent=2)

        logging.info("Transformation complete: %s records -> %s", out_count, self.output_file)
        logging.info("Metadata written to %s", self.metadata_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/targets_config.yaml")
    args = parser.parse_args()
    with open(args.config, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    NodeNormGeneTransformer(cfg).run()
