import os
import yaml
import json
import logging
import pandas as pd
from pathlib import Path
from pronto import Ontology
from datetime import datetime
from logging.handlers import RotatingFileHandler
import argparse

try:
    from publicdata.disease_data.transform_diff_utils import compute_dataframe_diff, write_diff_json
except ImportError:
    from transform_diff_utils import compute_dataframe_diff, write_diff_json


def setup_logging(log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=2)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(message)s"))

    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, stream_handler],
        force=True
    )


def explode_and_pivot(df, id_col, xref_col):
    df = df.copy()
    df[xref_col] = df[xref_col].fillna("").astype(str)
    df[xref_col] = df[xref_col].apply(lambda x: x.split("|") if x else [])
    exploded = df[[id_col, xref_col]].explode(xref_col)

    exploded["source"] = exploded[xref_col].apply(
        lambda x: x.split(":")[0].upper() if isinstance(x, str) and ":" in x else None
    )

    pivoted = exploded.pivot_table(
        index=id_col,
        columns="source",
        values=xref_col,
        aggfunc=lambda x: "|".join(x.dropna().astype(str).unique())
    ).reset_index()

    return pd.merge(df, pivoted, on=id_col, how="left")


def clean_pipe_column(series, replace_prefix=None, new_prefix=None, strip_prefix=False):
    series = series.replace("nan", pd.NA).fillna("")

    def _clean(val):
        if not isinstance(val, str):
            return ""
        vals = []
        for v in val.split("|"):
            v = v.strip()
            if not v:
                continue
            if replace_prefix and new_prefix:
                v = v.replace(replace_prefix, new_prefix)
            elif strip_prefix:
                if ":" in v:
                    v = v.split(":", 1)[1]
            vals.append(v)
        vals = sorted(set(vals))
        return "|".join(vals)

    series = series.apply(_clean)
    return series.replace("", pd.NA)


class DOIDTransformer:
    def __init__(self, full_config):
        self.cfg = full_config["doid"]
        self.input_file = Path(self.cfg["raw_file"])
        self.output_file = Path(self.cfg["cleaned_file"])
        self.meta_file = Path(self.cfg["transform_metadata_file"])
        self.log_file = Path(self.cfg["log_file"])
        self.qc_mode = self.cfg.get("qc_mode", full_config.get("global", {}).get("qc_mode", True))

        self.obsolete_output_file = Path(
            self.cfg.get(
                "obsolete_output_file",
                str(self.output_file.parent / "doid_obsolete_ids.csv")
            )
        )

        setup_logging(self.log_file)

    def _extract_term_record(self, term):
        synonyms = "|".join(sorted(set(
            s.description.strip()
            for s in term.synonyms
            if getattr(s, "description", None)
        ))) if term.synonyms else ""

        definition = str(term.definition).strip() if term.definition else ""
        xrefs = "|".join(sorted(set(
            str(x.id).strip()
            for x in term.xrefs
            if getattr(x, "id", None)
        ))) if term.xrefs else ""

        return {
            "DOID": term.id,
            "preferred_label": term.name,
            "definition": definition,
            "synonyms": synonyms,
            "database_cross_reference": xrefs
        }

    def _finalize_df(self, df):
        if df.empty:
            return df

        df = df.copy()
        df = df[df["DOID"].str.startswith("DOID:")].drop_duplicates()

        df = explode_and_pivot(df, "DOID", "database_cross_reference")
        df.rename(columns={col: f"doid_{col}" for col in df.columns}, inplace=True)

        if "doid_database_cross_reference" in df.columns:
            df.drop(columns=["doid_database_cross_reference"], inplace=True)

        if "doid_UMLS_CUI" in df.columns:
            df["doid_UMLS_CUI"] = clean_pipe_column(df["doid_UMLS_CUI"], strip_prefix=True)

        if "doid_SNOMEDCT_US_2023_03_01" in df.columns:
            df["doid_SNOMEDCT_US_2023_03_01"] = clean_pipe_column(
                df["doid_SNOMEDCT_US_2023_03_01"],
                replace_prefix="SNOMEDCT_US_2023_03_01:",
                new_prefix="SNOMEDCT:"
            )
            df.rename(columns={"doid_SNOMEDCT_US_2023_03_01": "doid_SNOMEDCT"}, inplace=True)

        if "doid_MIM" in df.columns:
            df["doid_MIM"] = clean_pipe_column(df["doid_MIM"], replace_prefix="MIM:", new_prefix="OMIM:")

        if "doid_ICD9CM" in df.columns:
            df["doid_ICD9CM"] = clean_pipe_column(df["doid_ICD9CM"], replace_prefix="ICD9CM:", new_prefix="ICD9:")

        if "doid_NCI" in df.columns:
            df["doid_NCI"] = clean_pipe_column(df["doid_NCI"], replace_prefix="NCI:", new_prefix="NCIT:")

        snomed_cols = [
            "doid_SNOMEDCT_US_2020_03_01", "doid_SNOMEDCT_US_2020_09_01",
            "doid_SNOMEDCT_US_2021_07_31", "doid_SNOMEDCT_US_2021_09_01",
            "doid_SNOMEDCT_US_2022_03_01", "doid_SNOMEDCT_US_2022_07_31",
            "doid_SNOMEDCT_US_2023_09_01", "doid_SNOMEDCT_US_2023_10_01",
            "doid_SNOMEDCT_US_2023_11_01", "doid_SNOMEDCT_US_2024_03_01",
            "doid_SNOMEDCT_US_2025_04_25", "doid_SNOMEDCT_US_2025_05_01"
        ]
        df.drop(columns=[col for col in snomed_cols if col in df.columns], inplace=True)

        return df

    def transform(self):
        if not self.input_file.exists():
            logging.error(f"Missing file: {self.input_file}")
            return

        ontology = Ontology(self.input_file)

        active_records = []
        obsolete_records = []

        for term in ontology.terms():
            if not str(term.id).startswith("DOID:"):
                continue

            record = self._extract_term_record(term)

            if getattr(term, "obsolete", False):
                obsolete_records.append(record)
            else:
                active_records.append(record)

        active_df = self._finalize_df(pd.DataFrame(active_records))
        obsolete_df = self._finalize_df(pd.DataFrame(obsolete_records))

        prev_active = self.output_file.with_suffix(".previous.csv")
        prev_obsolete = self.obsolete_output_file.with_suffix(".previous.csv")

        if prev_active.exists():
            old_df = pd.read_csv(prev_active, dtype=str)
            diff = compute_dataframe_diff(
                old_df,
                active_df,
                id_col="doid_DOID",
                label_col="doid_preferred_label",
                compare_cols=[c for c in active_df.columns if c.startswith("doid_") and c not in ["doid_DOID", "doid_preferred_label"]]
            )
            write_diff_json(diff, self.output_file.with_name("doid_changes.qc.json"))

        if prev_obsolete.exists():
            old_obs = pd.read_csv(prev_obsolete, dtype=str)
            obs_diff = compute_dataframe_diff(
                old_obs,
                obsolete_df,
                id_col="doid_DOID",
                label_col="doid_preferred_label"
            )
            write_diff_json(obs_diff, self.obsolete_output_file.with_name("doid_obsolete_changes.qc.json"))

        os.makedirs(self.output_file.parent, exist_ok=True)
        os.makedirs(self.obsolete_output_file.parent, exist_ok=True)
        os.makedirs(self.meta_file.parent, exist_ok=True)

        active_df.to_csv(self.output_file, index=False)
        obsolete_df.to_csv(self.obsolete_output_file, index=False)
        active_df.to_csv(prev_active, index=False)
        obsolete_df.to_csv(prev_obsolete, index=False)

        metadata = {
            "timestamp": datetime.now().isoformat(),
            "input_file": str(self.input_file),
            "output_file": str(self.output_file),
            "obsolete_output_file": str(self.obsolete_output_file),
            "active_record_count": len(active_df),
            "obsolete_record_count": len(obsolete_df)
        }
        with open(self.meta_file, "w") as f:
            json.dump(metadata, f, indent=2)

    def run(self):
        self.transform()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="YAML config path")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    DOIDTransformer(config).run()