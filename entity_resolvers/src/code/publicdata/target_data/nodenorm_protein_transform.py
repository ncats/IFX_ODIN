#!/usr/bin/env python3
"""
nodenorm_protein_transform.py — Parse NodeNorm protein dump into structured CSV.

Updates in this version:
  - keeps original parsing / preferred-name extraction behavior
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


class NodeNormProteinTransformer:
    def __init__(self, cfg: dict):
        self.config = cfg
        c = cfg["nodenorm_proteins"]

        self.input_file = c["raw_file"]
        self.output_file = c["output_file"]
        self.metadata_file = c.get(
            "tf_metadata_file",
            os.path.join(os.path.dirname(self.output_file), "tf_nodenorm_proteins_metadata.json"),
        )
        self.download_metadata_file = c.get("dl_metadata_file")
        self.transform_archive_dir = c.get("transform_archive_dir")

        self.log_file = c.get("transform_log_file", c.get("log_file"))
        if not self.log_file:
            self.log_file = os.path.join(
                os.path.dirname(self.metadata_file),
                "nodenorm_protein_transform.log",
            )

        self.qc_mode = bool(c.get("qc_mode", cfg.get("qc_mode", False)))
        self.qc_dir = c.get(
            "qc_dir",
            os.path.join(os.path.dirname(self.output_file), "qc"),
        )

        base = _sanitize_stem(self.output_file)
        self.backup_file = c.get("backup_file", os.path.join(self.qc_dir, f"{base}.backup.csv"))
        self.diff_file = c.get("diff_file", os.path.join(self.qc_dir, f"{base}.diff.csv"))

        setup_logging(self.log_file)

        self.records: list[dict] = []
        self._df: pd.DataFrame | None = None

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

    def pop_parentheses(self, text: str):
        text = text.strip()
        if text.endswith(")"):
            index = text.rfind("(")
            if index != -1:
                return text[index + 1 : -1].strip(), text[:index].strip()
        return None, text

    def parse_preferred_name(self, raw: str):
        parts = raw.split(" ", 1)
        if len(parts) < 2:
            return raw, raw, None, None
        uniprot, remainder = parts[0], parts[1]
        source, remainder = self.pop_parentheses(remainder)
        protein_type, remainder = self.pop_parentheses(remainder)
        return uniprot, remainder.strip(), protein_type, source

    def _parse_json_lines(self) -> list[str]:
        logging.info("Reading JSON-lines from %s", self.input_file)
        lines: list[str] = []
        with open(self.input_file, "r", encoding="utf-8") as fh:
            for line in fh:
                if '"NCBITaxon:9606"' in line:
                    lines.append(line)
        logging.info("Found %s human entries", len(lines))
        return lines

    def parse_data(self) -> tuple[int, int]:
        lines = self._parse_json_lines()
        input_count = len(lines)

        for idx, line in enumerate(lines, 1):
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                logging.warning("Bad JSON on line %s, skipping", idx)
                continue

            taxa = obj.get("taxa", [])
            if "NCBITaxon:9606" not in taxa:
                continue

            uniprot, main_name, protein_type, source = self.parse_preferred_name(
                obj.get("preferred_name", "")
            )
            row = {
                "EntryID": idx,
                "Type": obj.get("type"),
                "Taxa": "|".join(taxa),
                "uniprot_name": uniprot,
                "PreferredName": main_name,
                "ProteinType": protein_type,
                "Source": source,
            }

            for identifier in obj.get("identifiers", []):
                code = identifier.get("i", "")
                if ":" in code:
                    prefix, suffix = code.split(":", 1)
                    row.setdefault(prefix, []).append(suffix)

            for key, value in list(row.items()):
                if isinstance(value, list):
                    row[key] = "|".join(value)

            self.records.append(row)

        output_count = len(self.records)
        logging.info("Parsed %s output records", output_count)
        return input_count, output_count

    def save_to_csv(self) -> pd.DataFrame:
        if not self.records:
            raise ValueError("No records to save.")

        df = pd.DataFrame(self.records)
        rename_map = {
            "EntryID": "NodeNorm_Protein",
            "Type": "biolinkType",
            "PreferredName": "nodenorm_name",
            "UniProtKB": "nodenorm_uniprot_id",
            "ENSEMBL": "nodenorm_ensembl_protein_id",
            "UMLS": "nodenorm_UMLS",
        }
        df.rename(columns=rename_map, inplace=True)

        _safe_mkdir_for_file(self.output_file)
        df.to_csv(self.output_file, index=False)
        logging.info("Saved CSV to %s", self.output_file)

        self._df = df
        return df

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

                join_col = (
                    "nodenorm_uniprot_id"
                    if "nodenorm_uniprot_id" in compare_df.columns and "nodenorm_uniprot_id" in old_df.columns
                    else None
                )
                if join_col:
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
                        logging.info("No differences found in NodeNorm protein output.")
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
        input_count, output_count = self.parse_data()
        df = self.save_to_csv()
        ver_result = save_versioned_output(
            df=df,
            output_path=self.output_file,
            id_col="nodenorm_uniprot_id",
            sep=",",
            write_diff=False,
            archive_dir=self.transform_archive_dir,
            output_kind="cleaned_source_table",
        )
        diff_generated = self._generate_diff(df)
        end = datetime.now()

        download_meta = self._load_download_metadata()

        metadata = {
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
                "record_count_raw": input_count,
            },
            "output": {
                "path": self.output_file,
                "size_bytes": os.path.getsize(self.output_file),
                "md5": compute_md5(self.output_file),
                "record_count": output_count,
                "num_output_columns": df.shape[1],
                "output_columns": df.columns.tolist(),
                "output_versioning": ver_result,
            },
            "qc": {
                "qc_mode": self.qc_mode,
                "backup_file": self.backup_file if self.qc_mode else None,
                "diff_file": diff_generated,
            },
            "processing_steps": [
                {"step": "read_human_json_lines", "records": input_count},
                {"step": "parse_json_records", "records": output_count},
                {"step": "rename_columns"},
                {"step": "write_csv", "records": output_count},
            ],
        }

        _safe_mkdir_for_file(self.metadata_file)
        with open(self.metadata_file, "w", encoding="utf-8") as fh:
            json.dump(metadata, fh, indent=2)
        logging.info("Metadata written to %s", self.metadata_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform NodeNorm protein JSONL to CSV")
    parser.add_argument("--config", default="config/targets_config.yaml")
    args = parser.parse_args()
    with open(args.config, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    NodeNormProteinTransformer(cfg).run()
