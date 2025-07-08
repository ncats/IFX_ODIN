import os
import yaml
import json
import logging
import argparse
import hashlib
import requests
from datetime import datetime

GITHUB_API_URL = "https://api.github.com/repos/obophenotype/human-phenotype-ontology/releases/latest"

class HPOPhenotypeDownloader:
    def __init__(self, config):
        self.cfg = config["phenotype"]["hpo"]
        self.qc_mode = config.get("global", {}).get("qc_mode", False)
        self.raw_dir = self.cfg["raw_dir"]
        self.log_file = self.cfg.get("log_file", "hpo_download.log")
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)

        self.download_tasks = {
            "hp.obo": self.cfg["download_url_obo"],
            "hp.owl": self.cfg["download_url_owl"],
            "phenotype.hpoa": self.cfg["download_url_phenotype_hpoa"],
            "genes_to_phenotype.txt": self.cfg["download_url_genes_to_phenotype"],
            "phenotype_to_genes.txt": self.cfg["download_url_phenotype_to_genes"]
        }

        self.metadata = {
            "timestamp": {"start": str(datetime.now())},
            "steps": [],
            "outputs": [],
            "version": None,
            "github_release_tag": None
        }

    def run(self):
        self.fetch_latest_github_release()
        for filename, url in self.download_tasks.items():
            dest_path = os.path.join(self.raw_dir, filename)
            self.download_file(url, dest_path)

        self.extract_version(os.path.join(self.raw_dir, "phenotype.hpoa"))
        self.metadata["timestamp"]["end"] = str(datetime.now())
        self.save_metadata()

    def fetch_latest_github_release(self):
        try:
            response = requests.get(GITHUB_API_URL, timeout=20)
            response.raise_for_status()
            tag = response.json().get("tag_name")
            self.metadata["github_release_tag"] = tag
            logging.info(f"🔖 Latest GitHub release: {tag}")
        except Exception as e:
            logging.warning(f"⚠️ Could not fetch GitHub release tag: {e}")

    def download_file(self, url, dest_path):
        logging.info(f"🌐 Downloading {url} → {dest_path}")
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            new_hash = hashlib.md5(response.content).hexdigest()

            if os.path.exists(dest_path):
                with open(dest_path, "rb") as f:
                    old_hash = hashlib.md5(f.read()).hexdigest()
                if new_hash == old_hash:
                    logging.info(f"🔁 No update needed for {dest_path} (hash unchanged)")
                    self.metadata["steps"].append(f"No update: {url}")
                    return

            with open(dest_path, "wb") as f:
                f.write(response.content)
            self.metadata["steps"].append(f"Downloaded {url} to {dest_path}")
            self.metadata["outputs"].append(dest_path)
            logging.info(f"✅ Downloaded {dest_path} (hash: {new_hash})")

        except Exception as e:
            logging.error(f"❌ Failed to download {url}: {e}")
            self.metadata["steps"].append(f"Failed to download {url}: {str(e)}")

    def extract_version(self, hpoa_path):
        try:
            with open(hpoa_path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("#date:"):
                        self.metadata["version"] = line.strip().split(":", 1)[1].strip()
                        logging.info(f"📅 Extracted HPO version date: {self.metadata['version']}")
                        return
            logging.warning("⚠️ No version found in phenotype.hpoa")
        except Exception as e:
            logging.warning(f"⚠️ Could not parse version info from phenotype.hpoa: {e}")

    def save_metadata(self):
        meta_path = self.cfg["metadata_file"]
        os.makedirs(os.path.dirname(meta_path), exist_ok=True)
        with open(meta_path, "w") as f:
            json.dump(self.metadata, f, indent=2)
        logging.info(f"🧾 Metadata saved to: {meta_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HPO phenotype downloader for TargetGraph/ODIN pipeline")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    log_path = config["phenotype"]["hpo"].get("log_file", "hpo_download.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path, mode='a')
        ]
    )

    HPOPhenotypeDownloader(config).run()