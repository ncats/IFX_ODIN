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

def setup_logging(log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    
    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=2)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(message)s"))  # cleaner for terminal

    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, stream_handler],
        force=True
    )

def explode_and_pivot(df, id_col, xref_col):
    df[xref_col] = df[xref_col].fillna('').astype(str)
    df[xref_col] = df[xref_col].apply(lambda x: x.split('|') if x else [])
    exploded = df[[id_col, xref_col]].explode(xref_col)
    exploded['source'] = exploded[xref_col].apply(
        lambda x: x.split(':')[0].upper() if isinstance(x, str) and ':' in x else None
    )
    pivoted = exploded.pivot_table(
        index=id_col,
        columns='source',
        values=xref_col,
        aggfunc=lambda x: '|'.join(x.dropna().unique())
    ).reset_index()
    return pd.merge(df, pivoted, on=id_col, how='left')

class DOIDTransformer:
    def __init__(self, full_config):
        self.cfg = full_config["doid"]
        self.input_file = Path(self.cfg["raw_file"])
        self.output_file = Path(self.cfg["cleaned_file"])
        self.meta_file = Path(self.cfg["transform_metadata_file"])
        self.log_file = Path(self.cfg["log_file"])
        self.qc_mode = self.cfg.get("qc_mode", full_config.get("global", {}).get("qc_mode", True))
        setup_logging(self.log_file)

    def transform(self):
        if not self.input_file.exists():
            logging.error(f"Missing file: {self.input_file}")
            return

        logging.info(f"🔄 Parsing DOID: {self.input_file}")
        ontology = Ontology(self.input_file)
        records = []

        for term in ontology.terms():
            if term.id.startswith("DOID:"):
                synonyms = "|".join(s.description for s in term.synonyms) if term.synonyms else ""
                definition = str(term.definition) if term.definition else ""
                xrefs = "|".join(str(x.id) for x in term.xrefs) if term.xrefs else ""
                records.append({
                    "DOID": term.id,
                    "preferred_label": term.name,
                    "definition": definition,
                    "synonyms": synonyms,
                    "database_cross_reference": xrefs
                })

        df = pd.DataFrame(records)
        df = df[df["DOID"].str.startswith("DOID:")]
        df = df.drop_duplicates()

        # Explode + Pivot xrefs into columns
        df = explode_and_pivot(df, "DOID", "database_cross_reference")

        # Add doid_ prefix to all columns except DOID
        df.rename(columns={col: f"doid_{col}" for col in df.columns}, inplace=True)
        df.drop(columns=["doid_database_cross_reference"], inplace=True)
        # Clean UMLS_CUI: remove 'UMLS_CUI:' prefix and fix literal 'nan'
        if "doid_UMLS_CUI" in df.columns:
            df["doid_UMLS_CUI"] = df["doid_UMLS_CUI"].replace("nan", pd.NA)
            df["doid_UMLS_CUI"] = df["doid_UMLS_CUI"].fillna("")
            df["doid_UMLS_CUI"] = df["doid_UMLS_CUI"].apply(
                lambda x: "|".join([v.replace("UMLS_CUI:", "") for v in x.split("|") if v.strip()]) if isinstance(x, str) else ""
            )
            df["doid_UMLS_CUI"] = df["doid_UMLS_CUI"].replace("", pd.NA)

        # Clean SNOMEDCT: remove long prefix 'SNOMEDCT_US_2023_03_01:' → 'SNOMEDCT:'
        if "doid_SNOMEDCT_US_2023_03_01" in df.columns:
            df["doid_SNOMEDCT_US_2023_03_01"] = df["doid_SNOMEDCT_US_2023_03_01"].replace("nan", pd.NA)
            df["doid_SNOMEDCT_US_2023_03_01"] = df["doid_SNOMEDCT_US_2023_03_01"].fillna("")
            df["doid_SNOMEDCT_US_2023_03_01"] = df["doid_SNOMEDCT_US_2023_03_01"].apply(
                lambda x: "|".join([v.replace("SNOMEDCT_US_2023_03_01:", "SNOMEDCT:") for v in x.split("|") if v.strip()]) if isinstance(x, str) else ""
            )
            df["doid_SNOMEDCT_US_2023_03_01"] = df["doid_SNOMEDCT_US_2023_03_01"].replace("", pd.NA)
            df.rename(columns={"doid_SNOMEDCT_US_2023_03_01": "doid_SNOMEDCT"}, inplace=True)
        # Clean MIM: replace "MIM:" with "OMIM:"
        if "doid_MIM" in df.columns:
            df["doid_MIM"] = df["doid_MIM"].replace("nan", pd.NA)
            df["doid_MIM"] = df["doid_MIM"].fillna("")
            df["doid_MIM"] = df["doid_MIM"].apply(
                lambda x: "|".join([v.replace("MIM:", "OMIM:") for v in x.split("|") if v.strip()]) if isinstance(x, str) else ""
            )
            df["doid_MIM"] = df["doid_MIM"].replace("", pd.NA)
        # Clean ICD9CM: replace "ICD9CM:" with "ICD9:"
        if "doid_ICD9CM" in df.columns:
            df["doid_ICD9CM"] = df["doid_ICD9CM"].replace("nan", pd.NA)
            df["doid_ICD9CM"] = df["doid_ICD9CM"].fillna("")
            df["doid_ICD9CM"] = df["doid_ICD9CM"].apply(
                lambda x: "|".join([v.replace("ICD9CM:", "ICD9:") for v in x.split("|") if v.strip()]) if isinstance(x, str) else ""
            )
            df["doid_ICD9CM"] = df["doid_ICD9CM"].replace("", pd.NA)

        # List of SNOMEDCT columns to drop
        snomed_cols = [
            "doid_SNOMEDCT_US_2020_03_01", "doid_SNOMEDCT_US_2020_09_01", "doid_SNOMEDCT_US_2021_07_31",
            "doid_SNOMEDCT_US_2021_09_01", "doid_SNOMEDCT_US_2022_03_01", "doid_SNOMEDCT_US_2022_07_31",
            "doid_SNOMEDCT_US_2023_09_01", "doid_SNOMEDCT_US_2023_10_01", "doid_SNOMEDCT_US_2023_11_01",
            "doid_SNOMEDCT_US_2024_03_01", "doid_SNOMEDCT_US_2025_04_25", "doid_SNOMEDCT_US_2025_05_01"
        ]

        # Drop the columns if they exist in the dataframe
        df.drop(columns=[col for col in snomed_cols if col in df.columns], inplace=True)

        os.makedirs(self.output_file.parent, exist_ok=True)
        df.to_csv(self.output_file, index=False)
        logging.info(f"✅ Cleaned DOID saved → {self.output_file}")

        metadata = {
            "timestamp": datetime.now().isoformat(),
            "input_file": str(self.input_file),
            "output_file": str(self.output_file),
            "record_count": len(df)
        }
        with open(self.meta_file, "w") as f:
            json.dump(metadata, f, indent=2)
        logging.info(f"📜 Metadata written → {self.meta_file}")

    def run(self):
        self.transform()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="YAML config path")
    args = parser.parse_args()
    with open(args.config) as f:
        config = yaml.safe_load(f)
    DOIDTransformer(config).run()
