#!/usr/bin/env python
import os
import urllib.request
import logging
import yaml
from datetime import datetime
import json
import hashlib
import difflib

def compute_md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def generate_diff(old_path, new_path, diff_dir, label):
    os.makedirs(diff_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.basename(new_path)
    diff_txt = os.path.join(diff_dir, f"{label}_{base}_diff_{timestamp}.txt")
    diff_html = os.path.join(diff_dir, f"{label}_{base}_diff_{timestamp}.html")

    with open(old_path, "r", encoding="utf-8", errors="ignore") as f:
        old_lines = f.readlines()
    with open(new_path, "r", encoding="utf-8", errors="ignore") as f:
        new_lines = f.readlines()

    diff = list(difflib.unified_diff(old_lines, new_lines, fromfile="old", tofile="new"))
    with open(diff_txt, "w") as f:
        f.writelines(diff)

    html_diff = difflib.HtmlDiff().make_file(old_lines, new_lines, fromdesc="Old", todesc="New", context=True)
    with open(diff_html, "w") as f:
        f.write(html_diff)

    return diff_txt, diff_html

class PantherDownloader:
    def __init__(self, config):
        self.cfg = config["pathways"]["panther"]
        self.metadata = {
            "timestamp": {"start": str(datetime.now())},
            "downloads": []
        }
        self.qc_dir = self.cfg.get("qc_dir", "src/data/publicdata/pathway_data/qc")
        os.makedirs(self.qc_dir, exist_ok=True)

    def download_file(self, url, dest_path, label):
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        tmp_path = dest_path + ".tmp"
        logging.info(f"⬇️ Checking and downloading {label} from {url} ...")
        try:
            urllib.request.urlretrieve(url, tmp_path)
            new_md5 = compute_md5(tmp_path)

            if os.path.exists(dest_path):
                old_md5 = compute_md5(dest_path)
                if old_md5 == new_md5:
                    logging.info(f"⚖️ No changes detected for {label}; skipping overwrite.")
                    os.remove(tmp_path)
                    return
                else:
                    logging.info(f"🔁 Changes detected for {label}; generating diff...")
                    diff_txt, diff_html = generate_diff(dest_path, tmp_path, self.qc_dir, label)
                    logging.info(f"📝 Diff written: {diff_txt}, {diff_html}")

            os.replace(tmp_path, dest_path)
            logging.info(f"✅ Updated and saved to {dest_path}")

            self.metadata["downloads"].append({
                "url": url,
                "label": label,
                "path": dest_path,
                "md5": new_md5,
                "downloaded_at": str(datetime.now())
            })

        except Exception as e:
            logging.error(f"❌ Failed to download {url}: {e}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    def run(self):
        for key, entry in self.cfg["files"].items():
            self.download_file(entry["url"], entry["raw_path"], key)

        self.metadata["timestamp"]["end"] = str(datetime.now())
        with open(self.cfg["dl_metadata_file"], "w") as f:
            json.dump(self.metadata, f, indent=2)
        logging.info(f"📝 Download metadata saved to {self.cfg['dl_metadata_file']}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    PantherDownloader(config).run()
