# omim_download.py - Downloads static OMIM files using an API key and YAML config with diff tracking

import os
import logging
import requests
from pathlib import Path
import yaml
import argparse
import difflib
from datetime import datetime

DOWNLOADS = {
    "mimTitles.txt": "mimTitles",
    "genemap2.txt": "genemap2",
    "morbidmap.txt": "morbidmap",
    "mim2gene.txt": "mim2gene"  # public, no key needed
}

class OMIMDownloader:
    def __init__(self, config):
        self.cfg = config["omim"]
        self.output_dir = Path(self.cfg["raw_dir"])
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.api_key = os.environ.get("OMIM_API_KEY") or input("🔑 Enter your OMIM API key: ").strip()
        self.urls = self.build_urls()
        self.metadata_file = Path(self.cfg["dl_metadata_file"])
        self.log_file = Path(self.cfg["log_file"])
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(self.log_file, mode="a"),
                logging.StreamHandler()
            ]
        )

    def build_urls(self):
        base = f"https://data.omim.org/downloads/{self.api_key}"
        return {
            "mimTitles.txt": f"{base}/mimTitles.txt",
            "genemap2.txt": f"{base}/genemap2.txt",
            "morbidmap.txt": f"{base}/morbidmap.txt",
            "mim2gene.txt": "https://omim.org/static/omim/data/mim2gene.txt"
        }

    def save_diff_if_changed(self, new_content, dest):
        if dest.exists():
            old_content = dest.read_text()
            if old_content != new_content:
                diff_file = dest.with_suffix(dest.suffix + ".diff.txt")
                diff = difflib.unified_diff(
                    old_content.splitlines(),
                    new_content.splitlines(),
                    fromfile="previous",
                    tofile="new",
                    lineterm=""
                )
                diff_text = "\n".join(diff)
                diff_file.write_text(diff_text)
                logging.info(f"🔁 Changes detected in {dest.name}, diff saved to {diff_file.name}")
            else:
                logging.info(f"🟢 No changes detected in {dest.name}")
        dest.write_text(new_content)

    def download_files(self):
        for filename, url in self.urls.items():
            dest = self.output_dir / filename
            logging.info(f"⬇️  Downloading {filename}...")
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                self.save_diff_if_changed(r.text, dest)
                logging.info(f"✅ Saved to {dest}")
            else:
                logging.error(f"❌ Failed to download {filename} — HTTP {r.status_code}")

        meta = {
            "timestamp": datetime.now().isoformat(),
            "output_dir": str(self.output_dir),
            "files": list(self.urls.keys())
        }
        self.metadata_file.parent.mkdir(parents=True, exist_ok=True)
        self.metadata_file.write_text(yaml.dump(meta))

    def run(self):
        self.download_files()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    with open(args.config) as f:
        full_config = yaml.safe_load(f)

    downloader = OMIMDownloader(full_config)
    downloader.run()
