#!/usr/bin/env python

# orphanet_download.py - Downloads OWL and XML files from Orphanet with diff tracking

import os
import sys
import json
import logging
import argparse
import shutil
import difflib
import requests
import yaml
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

class OrphanetDownloader:
    def __init__(self, full_config):
        self.cfg = full_config["orphanet"]
        self.qc_mode = full_config.get("global", {}).get("qc_mode", False)

        setup_logging(os.path.abspath(self.cfg["log_file"]))

        self.owl_url = self.cfg["owl_url"]
        self.owl_file = Path(self.cfg["owl_file"])

        self.xml_url = self.cfg["xml_url"]
        self.xml_file = Path(self.cfg["xml_file"])

        self.metadata_file = Path(self.cfg["dl_metadata_file"])

        os.makedirs(self.owl_file.parent, exist_ok=True)
        os.makedirs(self.xml_file.parent, exist_ok=True)
        os.makedirs(self.metadata_file.parent, exist_ok=True)

    def download_with_diff(self, url, dest):
        temp_file = dest.with_suffix(dest.suffix + ".tmp")
        backup_file = dest.with_name(dest.stem + ".backup")
        diff_txt = dest.with_name(dest.stem + ".diff.txt")
        diff_html = dest.with_name(dest.stem + ".diff.html")

        logging.info(f"⬇️  Attempting download from: {url}")
        response = requests.get(url, stream=True)

        if response.status_code != 200:
            logging.error(f"❌ Failed to download {url} (HTTP {response.status_code})")
            if "ordo" in url.lower():
                logging.warning(
                    "⚠️  The hardcoded ORDO OWL version URL may be outdated.\n"
                    "   👉 Please check https://www.orphadata.com/ordo/ for the latest OWL file link."
                )
            elif "en_product6" in url.lower():
                logging.warning(
                    "⚠️  The hardcoded XML gene association URL may have changed.\n"
                    "   👉 Please verify the XML link via https://www.orphadata.com/data/xml/"
                )
            raise RuntimeError(f"Download failed: HTTP {response.status_code}")

        with open(temp_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)

        if dest.exists():
            with open(dest, "rb") as f1, open(temp_file, "rb") as f2:
                if f1.read() == f2.read():
                    os.remove(temp_file)
                    logging.info(f"✅ No change: {dest}")
                    return "skipped", None, None

            shutil.copy2(dest, backup_file)

            with open(backup_file, "r", encoding="utf-8", errors="ignore") as old_f, \
                open(temp_file, "r", encoding="utf-8", errors="ignore") as new_f:

                old_lines = old_f.readlines()
                new_lines = new_f.readlines()

            with open(diff_txt, "w") as f:
                f.writelines(difflib.unified_diff(old_lines, new_lines, fromfile="old", tofile="new"))

            with open(diff_html, "w") as f:
                f.write(difflib.HtmlDiff().make_file(old_lines, new_lines, "Old", "New"))

            os.replace(temp_file, dest)
            logging.info(f"📝 Updated: {dest}")
            return "updated", str(diff_txt), str(diff_html)

        os.replace(temp_file, dest)
        logging.info(f"✅ New file saved: {dest}")
        return "new", None, None

    def run(self):
        owl_status, owl_diff_txt, owl_diff_html = self.download_with_diff(self.owl_url, self.owl_file)
        xml_status, xml_diff_txt, xml_diff_html = self.download_with_diff(self.xml_url, self.xml_file)

        if not self.qc_mode:
            for f in [self.owl_file.with_suffix(".backup"), owl_diff_txt, owl_diff_html,
                      self.xml_file.with_suffix(".backup"), xml_diff_txt, xml_diff_html]:
                if f and os.path.exists(f):
                    os.remove(f)

        meta = {
            "timestamp": datetime.now().isoformat(),
            "owl_file": str(self.owl_file),
            "xml_file": str(self.xml_file),
            "owl_status": owl_status,
            "xml_status": xml_status,
            "owl_diff_txt": owl_diff_txt,
            "owl_diff_html": owl_diff_html,
            "xml_diff_txt": xml_diff_txt,
            "xml_diff_html": xml_diff_html,
        }
        with open(self.metadata_file, "w") as f:
            json.dump(meta, f, indent=2)
        logging.info(f"📝 Metadata saved → {self.metadata_file}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Download Orphanet OWL and XML files")
    parser.add_argument("--config", required=True, help="YAML config path")
    args = parser.parse_args()
    cfg = yaml.safe_load(open(args.config))
    OrphanetDownloader(cfg).run()
