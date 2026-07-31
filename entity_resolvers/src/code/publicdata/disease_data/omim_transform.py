#!/usr/bin/env python
# omim_transform.py - Extracts structured data from OMIM using mimTitles.txt and the API with QC diffs

import os
import json
import yaml
import logging
import argparse
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
import time

try:
    from publicdata.disease_data.transform_diff_utils import compute_dataframe_diff, write_diff_json
except ImportError:
    from transform_diff_utils import compute_dataframe_diff, write_diff_json


def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode="a"),
            logging.StreamHandler()
        ],
        force=True
    )


class OMIMTransformer:
    def __init__(self, full_config):
        self.cfg = full_config["omim"]
        setup_logging(self.cfg["log_file"])

        self.input_file = Path(self.cfg["input_file"])
        self.cleaned_genes_file = Path(self.cfg["cleaned_genes_file"])
        self.cleaned_diseases_file = Path(self.cfg["cleaned_diseases_file"])
        self.cleaned_obsolete_file = Path(self.cfg["cleaned_obsolete_file"])
        self.output_file = Path(self.cfg["output_file"])
        self.transformed_file = Path(self.cfg["transformed_file"])
        self.metadata_file = Path(self.cfg["transform_metadata_file"])
        self.api_key = os.environ.get("OMIM_API_KEY")

        self.failed_mims_file = self.transformed_file.with_name("OMIM_failed_mims.csv")

        os.makedirs(self.output_file.parent, exist_ok=True)
        os.makedirs(self.transformed_file.parent, exist_ok=True)
        os.makedirs(self.metadata_file.parent, exist_ok=True)

    def load_and_clean_titles(self):
        if not self.input_file.exists():
            logging.error(f"Input file not found: {self.input_file}")
            return None

        df = pd.read_csv(
            self.input_file,
            sep="\t",
            skiprows=2,
            dtype=str,
            encoding="utf-8-sig"
        )

        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace(";", "")
            .str.replace("(", "")
            .str.replace(")", "")
            .str.replace("[^a-z0-9_]", "", regex=True)
        )

        if "mim_number" not in df.columns:
            logging.error("❌ 'mim_number' column missing after cleaning. Aborting.")
            return None

        prefix_col = None
        for candidate in ["prefix", "_prefix"]:
            if candidate in df.columns:
                prefix_col = candidate
                break

        if prefix_col is None:
            logging.error("❌ No prefix column found ('prefix' or '_prefix').")
            return None

        df[prefix_col] = df[prefix_col].fillna("").astype(str).str.strip()
        df["omim_prefix"] = df[prefix_col].str.title()

        df["mim_number"] = df["mim_number"].fillna("").astype(str).str.strip()
        df = df[df["mim_number"] != ""].copy()
        df["mim_number"] = "OMIM:" + df["mim_number"]

        return df

    def save_cleaned_titles(self, df):
        df = df.rename(columns={
            "mim_number": "omim_OMIM",
            "preferred_title_symbol": "omim_preferred_label",
            "alternative_titles_symbols": "omim_alternative_labels",
            "included_titles_symbols": "omim_included_labels"
        })

        genes_df = df[df["omim_prefix"].isin(["Asterisk", "Plus"])].copy()
        diseases_df = df[df["omim_prefix"].isin(["Percent", "Number Sign", ""])].copy()
        obsolete_df = df[df["omim_prefix"] == "Caret"].copy()

        prev_disease = self.cleaned_diseases_file.with_suffix(".previous.csv")
        prev_obsolete = self.cleaned_obsolete_file.with_suffix(".previous.csv")

        if prev_disease.exists():
            old_df = pd.read_csv(prev_disease, dtype=str)
            diff = compute_dataframe_diff(
                old_df,
                diseases_df,
                id_col="omim_OMIM",
                label_col="omim_preferred_label",
                compare_cols=[c for c in diseases_df.columns if c.startswith("omim_") and c not in ["omim_OMIM", "omim_preferred_label"]]
            )
            write_diff_json(diff, self.cleaned_diseases_file.with_name("omim_changes.qc.json"))

        if prev_obsolete.exists():
            old_obs = pd.read_csv(prev_obsolete, dtype=str)
            obs_diff = compute_dataframe_diff(
                old_obs,
                obsolete_df,
                id_col="omim_OMIM",
                label_col="omim_preferred_label"
            )
            write_diff_json(obs_diff, self.cleaned_obsolete_file.with_name("omim_obsolete_changes.qc.json"))

        self.cleaned_genes_file.parent.mkdir(parents=True, exist_ok=True)
        genes_df.to_csv(self.cleaned_genes_file, index=False)
        diseases_df.to_csv(self.cleaned_diseases_file, index=False)
        obsolete_df.to_csv(self.cleaned_obsolete_file, index=False)

        diseases_df.to_csv(prev_disease, index=False)
        obsolete_df.to_csv(prev_obsolete, index=False)

        return genes_df, diseases_df, obsolete_df

    def prompt_skip_api(self, total_to_process):
        print(f"🧬 Found {total_to_process:,} disease MIMs to process.")
        choice = input("🔁 Resume OMIM API querying now? (Y/n): ").strip().lower()
        return not (choice == "n")

    def fetch_omim_entry(self, mim_number_prefixed: str):
        mim_numeric = str(mim_number_prefixed).strip().replace("OMIM:", "")

        params = {
            "mimNumber": mim_numeric,
            "include": (
                "titles,"
                "clinicalSynopsis,"
                "phenotypeMap,"
                "externalLinks,"
                "textSectionList,"
                "allelicVariant,"
                "geneMap,"
                "referenceList,"
                "exists"
            ),
            "format": "json",
            "apiKey": self.api_key
        }

        max_retries = 5
        retry_delay = 10

        for _ in range(max_retries):
            try:
                response = requests.get(
                    "https://api.omim.org/api/entry",
                    params=params,
                    timeout=20
                )

                if response.status_code == 200:
                    return response.json()

                elif response.status_code == 429:
                    wait_time = int(response.headers.get("Retry-After", retry_delay))
                    time.sleep(wait_time)
                    continue

                elif response.status_code == 400:
                    return {"omim": {"error": {"errorMessage": "HTTP 400"}}}

                else:
                    return {"omim": {"error": {"errorMessage": f"HTTP {response.status_code}"}}}

            except requests.exceptions.RequestException:
                time.sleep(retry_delay)

        return {"omim": {"error": {"errorMessage": "Max retries exceeded"}}}

    def extract_fields(self, mim_number_prefixed: str, entry_json: dict):
        mim_pref = str(mim_number_prefixed).strip()
        if not mim_pref.startswith("OMIM:"):
            mim_pref = "OMIM:" + mim_pref.replace("OMIM:", "")

        try:
            omim_block = entry_json.get("omim", {})
            entry_list = omim_block.get("entryList", [])

            if not entry_list:
                return None

            entry = entry_list[0].get("entry", {})
            return {
                "MIM_Number": mim_pref,
                "Preferred_Title": entry.get("titles", {}).get("preferredTitle", ""),
                "Clinical_Synopsis": json.dumps(entry.get("clinicalSynopsis", {})),
                "Phenotype_Maps": json.dumps(entry.get("phenotypeMapList", [])),
                "Text_Sections": json.dumps(entry.get("textSectionList", [])),
                "External_Links": json.dumps(entry.get("externalLinks", {})),
                "Allelic_Variants": json.dumps(entry.get("allelicVariantList", [])),
                "Gene_Map": json.dumps(entry.get("geneMap", {})),
                "Reference_List": json.dumps(entry.get("referenceList", [])),
                "Clinical_Synopsis_Exists": entry.get("clinicalSynopsisExists", False),
                "Phenotype_Map_Exists": entry.get("phenotypeMapExists", False),
                "Gene_Map_Exists": entry.get("geneMapExists", False),
                "Allelic_Variant_Exists": entry.get("allelicVariantExists", False),
                "Phenotypic_Series_Exists": entry.get("phenotypicSeriesExists", False),
            }
        except Exception:
            return None

    def process_mims(self, mim_list, already_processed, resume_records):
        records = resume_records.copy()
        failed = []
        total = len(mim_list)

        for count, mim in enumerate(mim_list, start=1):
            try:
                entry = self.fetch_omim_entry(mim)
                if entry:
                    data = self.extract_fields(mim, entry)
                    if data:
                        records.append(data)
                    else:
                        failed.append({"MIM_Number": mim, "reason": "No entryList / extraction failed"})
                else:
                    failed.append({"MIM_Number": mim, "reason": "No response"})
            except Exception as e:
                failed.append({"MIM_Number": mim, "reason": str(e)})

            if count % 100 == 0:
                pd.DataFrame(records).to_csv(self.transformed_file, index=False)
                if failed:
                    pd.DataFrame(failed).to_csv(self.failed_mims_file, index=False)
            time.sleep(1)

        return records, failed

    def run(self):
        if not self.api_key:
            logging.error("❌ OMIM_API_KEY environment variable is not set.")
            return

        df = self.load_and_clean_titles()
        if df is None:
            return

        _, diseases_df, obsolete_df = self.save_cleaned_titles(df)

        if "omim_OMIM" not in diseases_df.columns:
            logging.error("❌ 'omim_OMIM' column missing in disease rows.")
            return

        all_mims = set(diseases_df["omim_OMIM"].dropna().astype(str).str.strip().unique())

        processed = set()
        if self.transformed_file.exists():
            existing_df = pd.read_csv(self.transformed_file, dtype=str)

            if "MIM_Number" not in existing_df.columns:
                records = []
            else:
                existing_df["MIM_Number"] = existing_df["MIM_Number"].astype(str).str.strip()
                processed_all = set(existing_df["MIM_Number"].dropna())
                processed = processed_all.intersection(all_mims)
                records = existing_df[existing_df["MIM_Number"].isin(all_mims)].to_dict(orient="records")
        else:
            records = []

        to_process = sorted([m for m in all_mims if m not in processed])

        if len(to_process) == 0:
            pd.DataFrame(records).to_csv(self.transformed_file, index=False)
        else:
            if not self.prompt_skip_api(len(to_process)):
                return
            records, failed = self.process_mims(to_process, processed, records)
            pd.DataFrame(records).to_csv(self.transformed_file, index=False)

            if failed:
                pd.DataFrame(failed).to_csv(self.failed_mims_file, index=False)
            elif self.failed_mims_file.exists():
                self.failed_mims_file.unlink()

        metadata = {
            "timestamp": datetime.now().isoformat(),
            "records_transformed": len(pd.read_csv(self.transformed_file, dtype=str)) if self.transformed_file.exists() else 0,
            "input_file": str(self.input_file),
            "cleaned_genes_file": str(self.cleaned_genes_file),
            "cleaned_diseases_file": str(self.cleaned_diseases_file),
            "cleaned_obsolete_file": str(self.cleaned_obsolete_file),
            "output_file": str(self.transformed_file),
            "failed_mims_file": str(self.failed_mims_file),
        }
        with open(self.metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OMIM Transformer")
    parser.add_argument("--config", required=True, help="YAML config path")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    OMIMTransformer(cfg).run()