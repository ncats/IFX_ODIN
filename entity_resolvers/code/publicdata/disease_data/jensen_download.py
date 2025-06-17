# jensen_download.py - Download Jensen Disease Text Mining Scores with logging and diff tracking

import os
import sys
import yaml
import json
import logging
import argparse
import hashlib
import difflib
import shutil
import requests
from datetime import datetime
from pathlib import Path

def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    handlers = [
        logging.FileHandler(log_file, mode="a"),
        logging.StreamHandler(sys.stdout),
    ]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
        force=True,
    )

class JensenDiseaseDownloader:
    def __init__(self, full_config):
        self.cfg = full_config["jensen"]
        self.qc_mode = self.cfg.get("qc_mode", full_config.get("global", {}).get("qc_mode", True))
        setup_logging(self.cfg["log_file"])

        self.download_url = self.cfg["download_url"]
        self.tsv_file = Path(self.cfg["raw_file"])
        self.metadata_file = Path(self.cfg["dl_metadata_file"])

        os.makedirs(self.tsv_file.parent, exist_ok=True)
        os.makedirs(self.metadata_file.parent, exist_ok=True)

    def compute_hash(self, file_path):
        h = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                h.update(chunk)
        return h.hexdigest()

    def run(self):
        logging.info(f"⬇️  Downloading Jensen Disease TSV from {self.download_url}")
        response = requests.get(self.download_url, stream=True)
        response.raise_for_status()

        temp_file = self.tsv_file.with_suffix(".temp.tsv")
        with open(temp_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logging.info(f"✅ Temp download saved to {temp_file}")

        diff_txt = self.tsv_file.with_suffix(".diff.txt")
        diff_html = self.tsv_file.with_suffix(".diff.html")
        backup_file = self.tsv_file.with_suffix(".backup.tsv")

        if self.tsv_file.exists():
            if self.compute_hash(self.tsv_file) != self.compute_hash(temp_file):
                shutil.copy2(self.tsv_file, backup_file)

                with open(backup_file, "r", encoding="utf-8", errors="ignore") as old_f, \
                     open(temp_file, "r", encoding="utf-8", errors="ignore") as new_f:
                    old_lines = old_f.readlines()
                    new_lines = new_f.readlines()

                full_diff = list(difflib.unified_diff(old_lines, new_lines, fromfile="old", tofile="new"))
                with open(diff_txt, "w") as dt:
                    dt.writelines(full_diff[:100])
                with open(diff_html, "w") as dh:
                    dh.write(difflib.HtmlDiff().make_file(old_lines, new_lines, fromdesc="Old", todesc="New", context=True))

                os.replace(temp_file, self.tsv_file)
                logging.info(f"📄 Diff saved: {diff_txt}, {diff_html}")

                if not self.qc_mode:
                    for f in [backup_file, diff_txt, diff_html]:
                        if f.exists():
                            f.unlink()
            else:
                logging.info("No changes detected. Keeping existing TSV file.")
                temp_file.unlink()
        else:
            os.replace(temp_file, self.tsv_file)

        meta = {
            "timestamp": datetime.now().isoformat(),
            "download_url": self.download_url,
            "tsv_file": str(self.tsv_file),
            "diff_file_text": str(diff_txt) if diff_txt.exists() else None,
            "diff_file_html": str(diff_html) if diff_html.exists() else None
        }
        with open(self.metadata_file, "w") as f:
            json.dump(meta, f, indent=2)
        logging.info(f"📝 Metadata saved → {self.metadata_file}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Download Jensen Disease TSV file")
    parser.add_argument("--config", required=True, help="Path to config YAML")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    JensenDiseaseDownloader(cfg).run()
