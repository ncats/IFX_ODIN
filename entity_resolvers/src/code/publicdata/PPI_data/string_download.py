#!/usr/bin/env python
"""
string_download.py - Download STRING PPI data with update detection,
diff generation, and structured metadata.
"""

import os
import gzip
import shutil
import yaml
import json
import logging
import hashlib
import difflib
import argparse
import requests
from datetime import datetime
from tqdm import tqdm
from requests.exceptions import HTTPError

def setup_logging(config):
    log_file = config.get("log_file", "string_download.log")
    handlers = [logging.StreamHandler()]
    directory = os.path.dirname(log_file)
    if directory:
        os.makedirs(directory, exist_ok=True)
    handlers.insert(0, logging.FileHandler(log_file))
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s",
                        handlers=handlers)

class StringPPIDownloader:
    def __init__(self, full_config):
        self.cfg = full_config["ppi"]["string"]
        self.qc_mode = full_config.get("global", {}).get("qc_mode", False)
        setup_logging(self.cfg)

        self.url = self.cfg["download_url"]
        self.output_path = self.cfg["raw_file"] + ".gz"
        self.decompressed_path = self.cfg["raw_file"]
        self.meta_file = self.cfg.get("dl_metadata_file", "metadata/string_dl_metadata.json")
        self.diff_file = self.cfg.get("diff_file", "diffs/string_download.diff.txt")

    def compute_md5(self, filepath):
        h = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                h.update(chunk)
        return h.hexdigest()

    def download_and_extract(self):
        tmp_file = self.output_path + ".tmp"
        update_detected = False
        old_md5, new_md5 = None, None

        logging.info(f"Downloading STRING PPI from {self.url}")
        try:
            with requests.get(self.url, stream=True) as r:
                r.raise_for_status()
                total = int(r.headers.get("content-length", 0))
                with open(tmp_file, "wb") as f, tqdm(total=total, unit="iB", unit_scale=True) as bar:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                        bar.update(len(chunk))
        except HTTPError as e:
            logging.error(f"HTTP error: {e}")
            return False

        if os.path.exists(self.output_path):
            old_md5 = self.compute_md5(self.output_path)
            new_md5 = self.compute_md5(tmp_file)
            if old_md5 == new_md5:
                logging.info("No update detected. Removing temp file.")
                os.remove(tmp_file)
                return True
            else:
                logging.info("Update detected.")
                update_detected = True
                shutil.move(self.output_path, self.output_path + ".backup")
        else:
            new_md5 = self.compute_md5(tmp_file)
            update_detected = True

        shutil.move(tmp_file, self.output_path)
        logging.info(f"Saved compressed file to {self.output_path}")

        logging.info(f"Decompressing to {self.decompressed_path}")
        with gzip.open(self.output_path, "rt") as f_in, open(self.decompressed_path, "w") as f_out:
            shutil.copyfileobj(f_in, f_out)

        diff_summary = ""
        text_diff_path = self.diff_file.replace(".txt", f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
        html_diff_path = text_diff_path.replace(".txt", ".html")

        if update_detected and os.path.exists(self.decompressed_path + ".backup"):
            logging.info("Generating diff...")
            try:
                with open(self.decompressed_path + ".backup", "r", encoding="utf-8", errors="ignore") as old, \
                     open(self.decompressed_path, "r", encoding="utf-8", errors="ignore") as new:
                    old_lines = old.readlines()
                    new_lines = new.readlines()

                diff = list(difflib.unified_diff(old_lines, new_lines,
                                                 fromfile="old", tofile="new"))
                with open(text_diff_path, "w") as f:
                    f.write("".join(diff))
                logging.info(f"Diff written to {text_diff_path}")
                diff_summary = "".join(diff[:500])

                html = difflib.HtmlDiff().make_file(old_lines, new_lines, "old", "new", context=True, numlines=0)
                with open(html_diff_path, "w") as f:
                    f.write(html)
                logging.info(f"HTML diff written to {html_diff_path}")
            except Exception as e:
                logging.error(f"Failed to generate diff: {e}")
        else:
            logging.info("No previous decompressed file found to diff against.")

        metadata = {
            "download_url": self.url,
            "downloaded_at": str(datetime.now()),
            "output_path": self.output_path,
            "decompressed_path": self.decompressed_path,
            "update_detected": update_detected,
            "old_md5": old_md5,
            "new_md5": new_md5,
            "diff_file_text": text_diff_path if update_detected else None,
            "diff_file_html": html_diff_path if update_detected else None,
            "diff_summary": diff_summary
        }

        os.makedirs(os.path.dirname(self.meta_file), exist_ok=True)
        with open(self.meta_file, "w") as f:
            json.dump(metadata, f, indent=2)

        logging.info(f"Metadata written to {self.meta_file}")
        return True

    def run(self):
        return self.download_and_extract()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    downloader = StringPPIDownloader(config)
    downloader.run()
