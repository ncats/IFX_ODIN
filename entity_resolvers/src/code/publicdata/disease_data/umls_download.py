import requests
import logging
import os
import json
import argparse
import yaml
import pandas as pd
from datetime import datetime

def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    handlers = [
        logging.FileHandler(log_file, mode="a"),
        logging.StreamHandler()
    ]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
        force=True
    )

class UMLSDownloader:
    def __init__(self, full_config):
        self.full_config = full_config
        self.cfg = full_config["umls"]
        self.qc_mode = full_config.get("global", {}).get("qc_mode", False)
        setup_logging(os.path.abspath(self.cfg["log_file"]))

        self.api_key = os.getenv("UMLS_API_KEY")
        if not self.api_key:
            logging.error("UMLS API key not found in environment variables.")
            raise ValueError("API key is missing.")

        self.base_url = "https://uts-ws.nlm.nih.gov/rest"
        self.version = "current"

        self.raw_file = self.cfg["raw_file"]
        self.cleaned_file = self.cfg["cleaned_file"]
        self.dl_metadata_file = self.cfg["dl_metadata_file"]
        self.transform_metadata_file = self.cfg["transform_metadata_file"]

    def search_disease_cuis(self):
        results = []
        page = 1
        seen = set()

        while True:
            params = {
                "apiKey": self.api_key,
                "string": "disease",
                "pageNumber": page
            }
            url = f"{self.base_url}/search/{self.version}"
            response = requests.get(url, params=params)

            if response.status_code != 200:
                logging.error(f"Failed to retrieve CUIs on page {page}")
                break

            data = response.json()
            page_results = data.get("result", {}).get("results", [])
            if not page_results:
                break

            for item in page_results:
                cui = item.get("ui")
                if cui and cui.startswith("C") and cui not in seen:
                    name = item.get("name", "")
                    concept_url = f"{self.base_url}/content/{self.version}/CUI/{cui}"
                    detail_resp = requests.get(concept_url, params={"apiKey": self.api_key})
                    if detail_resp.status_code == 200:
                        detail_data = detail_resp.json()
                        sem_types = detail_data.get("result", {}).get("semanticTypes", [])
                        if any("Disease or Syndrome" in st.get("name", "") for st in sem_types):
                            results.append({"ui": cui, "name": name})
                            seen.add(cui)
                            logging.info(f"✔ Found disease: {name} ({cui})")
                            if len(results) % 500 == 0:
                                self._autosave(results)
                    else:
                        logging.warning(f"Could not fetch detail for {cui}")

            logging.info(f"Page {page}: {len(page_results)} results, {len(results)} total disease CUIs")
            page += 1
            if page > 100:
                break

        return results

    def _autosave(self, records):
        os.makedirs(os.path.dirname(self.raw_file), exist_ok=True)
        with open(self.raw_file, "w") as f:
            json.dump(records, f, indent=2)
        logging.info(f"💾 Autosaved {len(records)} CUIs → {self.raw_file}")

    def resolve_related_id_to_cui(self, url):
        response = requests.get(url, params={"apiKey": self.api_key})
        if response.status_code != 200:
            return None, None

        result = response.json().get("result", {})
        return result.get("ui"), f"{result.get('rootSource')}:{result.get('code')}" if result.get("rootSource") and result.get("code") else None

    def fetch_relations(self, cui):
        url = f"{self.base_url}/content/{self.version}/CUI/{cui}/relations"
        params = {"apiKey": self.api_key}
        response = requests.get(url, params=params)

        if response.status_code != 200:
            logging.warning(f"Failed to fetch relations for CUI {cui}")
            return [], [], [], []

        data = response.json()
        relations = data.get("result", [])

        parents, children = [], []
        parent_xrefs, child_xrefs = [], []

        for r in relations:
            label = r.get("relationLabel")
            related_url = r.get("relatedId")
            if not related_url:
                continue

            related_cui, xref = self.resolve_related_id_to_cui(related_url)
            if label == "PAR":
                if related_cui:
                    parents.append(related_cui)
                if xref:
                    parent_xrefs.append(xref)
            elif label == "CHD":
                if related_cui:
                    children.append(related_cui)
                if xref:
                    child_xrefs.append(xref)

        return parents, children, parent_xrefs, child_xrefs

    def run(self):
        cuis = self.search_disease_cuis()
        records = []

        for item in cuis:
            cui = item["ui"]
            name = item["name"]
            logging.info(f"🔄 Processing {cui} - {name}")
            parents, children, parent_xrefs, child_xrefs = self.fetch_relations(cui)

            records.append({
                "cui": cui,
                "name": name,
                "parents": "|".join(parents),
                "children": "|".join(children),
                "parent_xrefs": "|".join(parent_xrefs),
                "child_xrefs": "|".join(child_xrefs)
            })

        # Save final JSON
        os.makedirs(os.path.dirname(self.raw_file), exist_ok=True)
        with open(self.raw_file, "w") as f:
            json.dump(records, f, indent=2)

        # Save final CSV
        df = pd.DataFrame(records)
        os.makedirs(os.path.dirname(self.cleaned_file), exist_ok=True)
        df.to_csv(self.cleaned_file, index=False)

        # Metadata
        metadata = {
            "timestamp": datetime.now().isoformat(),
            "total_records": len(records),
            "raw_file": self.raw_file,
            "cleaned_file": self.cleaned_file
        }
        os.makedirs(os.path.dirname(self.dl_metadata_file), exist_ok=True)
        with open(self.dl_metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)
        with open(self.transform_metadata_file, "w") as f:
            json.dump(metadata, f, indent=2)

        logging.info(f"✅ Saved {len(records)} entries to {self.cleaned_file}")
        logging.info(f"📝 Metadata written to {self.dl_metadata_file} and {self.transform_metadata_file}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Download UMLS disease CUIs with relationships and xrefs")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    with open(args.config) as f:
        full_config = yaml.safe_load(f)

    UMLSDownloader(full_config).run()
