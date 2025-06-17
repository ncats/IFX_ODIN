#!/usr/bin/env python

"""
doid_download.py — Download HumanDO.obo (DOID) ontology file from public source
with rotating logs, metadata tracking, and diff comparison.
"""

import os
import json
import yaml
import hashlib
import requests
import logging
from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler

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

def compute_sha256(file_path):
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()

class DOIDDownloader:
    def __init__(self, full_config):
        self.cfg = full_config["doid"]
        self.qc_mode = self.cfg.get("qc_mode", full_config.get("global", {}).get("qc_mode", True))

        self.url = self.cfg["download_url"]
        self.output_path = Path(self.cfg["raw_file"])
        self.meta_path = Path(self.cfg["dl_metadata_file"])
        self.log_path = Path(self.cfg["log_file"])
        self.diff_path = self.output_path.with_suffix(".diff.txt")

        setup_logging(self.log_path)

    def download(self):
        logging.info(f"⬇️ Fetching from {self.url}")
        os.makedirs(self.output_path.parent, exist_ok=True)

        response = requests.get(self.url, timeout=60)
        response.raise_for_status()
        new_content = response.content

        # Write to temp for diff
        tmp_path = self.output_path.with_suffix(".tmp")
        with open(tmp_path, "wb") as f:
            f.write(new_content)
        logging.info(f"🧪 Downloaded to temp: {tmp_path}")

        # Diff check
        if self.output_path.exists():
            old_hash = compute_sha256(self.output_path)
            new_hash = compute_sha256(tmp_path)
            if old_hash == new_hash:
                logging.info("✅ No changes detected (hash match). Keeping existing file.")
                os.remove(tmp_path)
                return
            else:
                if self.qc_mode:
                    with open(self.diff_path, "w") as f:
                        f.write(f"OLD HASH: {old_hash}\nNEW HASH: {new_hash}\n")
                    logging.info(f"🔍 Hash changed. Diff saved to: {self.diff_path}")
        else:
            logging.info("🆕 No prior file detected. Saving as new.")

        # Move temp to final
        os.replace(tmp_path, self.output_path)
        logging.info(f"✅ Saved final OBO → {self.output_path}")

        # Metadata
        metadata = {
            "download_url": self.url,
            "saved_to": str(self.output_path),
            "timestamp": datetime.now().isoformat(),
            "filesize_bytes": os.path.getsize(self.output_path),
            "sha256": compute_sha256(self.output_path)
        }
        os.makedirs(self.meta_path.parent, exist_ok=True)
        with open(self.meta_path, "w") as f:
            json.dump(metadata, f, indent=2)
        logging.info(f"📝 Metadata written → {self.meta_path}")

    def run(self):
        self.download()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML config")
    args = parser.parse_args()

    with open(args.config) as f:
        full_config = yaml.safe_load(f)
    DOIDDownloader(full_config).run()
