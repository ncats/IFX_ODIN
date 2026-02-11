#!/usr/bin/env python
# omim_transform.py - Extracts structured data from OMIM using mimTitles.txt and the API

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
        self.output_file = Path(self.cfg["output_file"])  # optional raw API json
        self.transformed_file = Path(self.cfg["transformed_file"])
        self.metadata_file = Path(self.cfg["transform_metadata_file"])
        self.api_key = os.environ.get("OMIM_API_KEY")

        os.makedirs(self.output_file.parent, exist_ok=True)
        os.makedirs(self.transformed_file.parent, exist_ok=True)
        os.makedirs(self.metadata_file.parent, exist_ok=True)

    def load_and_clean_titles(self):
        if not self.input_file.exists():
            logging.error(f"Input file not found: {self.input_file}")
            return None

        df = pd.read_csv(self.input_file, sep="\t", skiprows=2, dtype=str, encoding="utf-8-sig")
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_") \
            .str.replace(";", "").str.replace("(", "").str.replace(")", "") \
            .str.replace("[^a-z0-9_]", "", regex=True)

        if "prefix" in df.columns:
            df = df[df["prefix"].str.upper().ne("CARET") & df["prefix"].notna()]

        if "mim_number" not in df.columns:
            logging.error("❌ 'mim_number' column missing after cleaning. Aborting.")
            return None

        # ✅ Add "OMIM:" prefix to all values in the mim_number column
        df["mim_number"] = df["mim_number"].fillna("").astype(str).str.strip()
        df["mim_number"] = "OMIM:" + df["mim_number"]

        return df

    def save_cleaned_titles(self, df):
        # Normalize and rename columns for provenance
        df["_prefix"] = df["_prefix"].fillna("").str.strip().str.title()

        df = df.rename(columns={
            "_prefix": "omim_prefix",
            "mim_number": "omim_OMIM",
            "preferred_title_symbol": "omim_preferred_label",
            "alternative_titles_symbols": "omim_alternative_labels",
            "included_titles_symbols": "omim_included_labels"
        })

        # Subsets
        genes_df = df[df["omim_prefix"].isin(["Asterisk", "Plus"])]
        diseases_df = df[df["omim_prefix"].isin(["Percent", "Number Sign", ""])]
        obsolete_df = df[df["omim_prefix"] == "Caret"]

        # File paths from config
        genes_path = Path(self.cfg["cleaned_genes_file"])
        diseases_path = Path(self.cfg["cleaned_diseases_file"])
        obsolete_path = Path(self.cfg["cleaned_obsolete_file"])

        # Save files
        genes_path.parent.mkdir(parents=True, exist_ok=True)
        genes_df.to_csv(genes_path, index=False)
        logging.info(f"🧬 Gene entries saved → {genes_path} ({genes_df.shape[0]} rows)")

        diseases_path.parent.mkdir(parents=True, exist_ok=True)
        diseases_df.to_csv(diseases_path, index=False)
        logging.info(f"🦠 Disease entries saved → {diseases_path} ({diseases_df.shape[0]} rows)")

        obsolete_path.parent.mkdir(parents=True, exist_ok=True)
        obsolete_df.to_csv(obsolete_path, index=False)
        logging.info(f"🗑️ Obsolete entries saved → {obsolete_path} ({obsolete_df.shape[0]} rows)")

    def prompt_skip_api(self, total_to_process):
        print(f"🧬 Found {total_to_process:,} disease MIMs to process.")
        choice = input("🔁 Resume OMIM API querying now? (Y/n): ").strip().lower()
        return not (choice == "n")

    def fetch_omim_entry(self, mim_number):
        params = {
            "mimNumber": mim_number,
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

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get("https://api.omim.org/api/entry", params=params, timeout=10)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    wait_time = int(response.headers.get("Retry-After", retry_delay))
                    logging.warning(f"Rate limit hit for {mim_number}. Waiting {wait_time} sec...")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Failed to fetch {mim_number}: HTTP {response.status_code}")
                    break
            except requests.exceptions.RequestException as e:
                logging.warning(f"Error fetching {mim_number}: {e}")
                time.sleep(retry_delay)

        return None

    def extract_fields(self, mim_number, entry_json):
        try:
            entry = entry_json["omim"]["entryList"][0]["entry"]
            return {
                "MIM_Number": mim_number,
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
        except Exception as e:
            logging.warning(f"⚠️ Failed to extract for {mim_number}: {e}")
            return None

    def process_mims(self, mim_list, already_processed, resume_records):
        records = resume_records.copy()
        total = len(mim_list)

        for count, mim in enumerate(mim_list, start=1):
            try:
                entry = self.fetch_omim_entry(mim)
                if entry:
                    data = self.extract_fields(mim, entry)
                    if data:
                        records.append(data)
            except Exception as e:
                logging.warning(f"⚠️ Error processing {mim}: {e}")

            if count % 100 == 0:
                pd.DataFrame(records).to_csv(self.transformed_file, index=False)
                logging.info(f"Processed {len(already_processed) + count} entries...")
            elif count % 10 == 0:
                logging.info(f"→ Working on MIM {mim} ({count} of {total})")

            time.sleep(1)

        return records

    def run(self):
        df = self.load_and_clean_titles()
        if df is None:
            return

        # Split full file into gene/disease/obsolete files
        self.save_cleaned_titles(df)

        # ✅ Only query disease MIMs from OMIM_diseases.csv
        disease_df = pd.read_csv(self.cleaned_diseases_file, dtype=str)
        if "omim_OMIM" not in disease_df.columns:
            logging.error("❌ 'omim_OMIM' column missing in cleaned_diseases_file.")
            return

        all_mims = disease_df["omim_OMIM"].dropna().unique()

        # Resume logic if final output already exists
        processed = set()
        if self.transformed_file.exists():
            existing_df = pd.read_csv(self.transformed_file, dtype=str)
            processed = set(existing_df["MIM_Number"].dropna())
            records = existing_df.to_dict(orient="records")
        else:
            records = []

        to_process = [m for m in all_mims if m not in processed]
        logging.info(f"Total Disease MIMs: {len(all_mims)} | Already processed: {len(processed)} | Remaining: {len(to_process)}")

        # 🔁 Ask user if they want to proceed with API calls
        if not self.prompt_skip_api(len(to_process)):
            logging.info("⏭️ Skipping OMIM API query step as requested.")
            return

        records = self.process_mims(to_process, processed, records)

        df_out = pd.DataFrame(records)
        df_out.to_csv(self.transformed_file, index=False)
        logging.info(f"✅ Final data written → {self.transformed_file}")

        meta = {
            "timestamp": datetime.now().isoformat(),
            "records_transformed": len(df_out),
            "input_file": str(self.input_file),
            "cleaned_genes_file": str(self.cleaned_genes_file),
            "cleaned_diseases_file": str(self.cleaned_diseases_file),
            "output_file": str(self.transformed_file),
        }
        with open(self.metadata_file, "w") as f:
            json.dump(meta, f, indent=2)
        logging.info(f"📝 Metadata saved → {self.metadata_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OMIM Transformer")
    parser.add_argument("--config", required=True, help="YAML config path")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)
    OMIMTransformer(cfg).run()
