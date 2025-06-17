#!/usr/bin/env python
"""
hgnc_download.py - Download HGNC data with update detection, detailed metadata tracking,
and log file creation for HGNC.
"""

import os
import yaml
import logging
import argparse
from datetime import datetime
import requests
from requests.exceptions import HTTPError
from tqdm import tqdm
import hashlib
import difflib
import json
import shutil
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def setup_logging(config):
    # Use a default log file name if not provided
    log_file = config.get("log_file", "hgnc_download.log")
    handlers = [logging.StreamHandler()]
    # Only try to create directories if log_file is non-empty and has a directory part.
    directory = os.path.dirname(log_file)
    if directory:
        os.makedirs(directory, exist_ok=True)
    # Add a file handler only if log_file is provided (even if it's just a file name)
    if log_file:
        handlers.insert(0, logging.FileHandler(log_file))
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s",
                        handlers=handlers)

class HGNCDownloader:
    def __init__(self, full_config):
        self.config = full_config["hgnc_data"]
        setup_logging(self.config)
        self.url = self.config["download_url"]
        self.output_path = self.config["output_path"]  # e.g., path to the downloaded file
        self.meta_file = self.config.get("dl_metadata_file", "dl_hgnc_metadata.json")
        # diff_file key will be looked up here; if not present it uses "hgnc_diff.txt" by default
        self.base_diff_file = self.config.get("diff_file", "hgnc_diff.txt")
    
    def compute_hash(self, file_path):
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def download(self):
        temp_file = self.output_path + ".temp"
        update_detected = False
        old_md5 = None
        new_md5 = None
        total_size = 0

        try:
            logging.info(f"Downloading HGNC data from {self.url} to {temp_file}...")
            with requests.get(self.url, stream=True, verify=False) as response:
                response.raise_for_status()
                total_size = int(response.headers.get("content-length", 0))
                block_size = 1024
                tqdm_bar = tqdm(total=total_size, unit="iB", unit_scale=True)
                with open(temp_file, "wb") as f:
                    for chunk in response.iter_content(block_size):
                        f.write(chunk)
                        tqdm_bar.update(len(chunk))
                tqdm_bar.close()
        except HTTPError as e:
            logging.error(f"Error downloading HGNC data: {e}")
            return False

        if os.path.exists(self.output_path):
            old_hash = self.compute_hash(self.output_path)
            new_hash = self.compute_hash(temp_file)
            if old_hash == new_hash:
                logging.info("No updates detected for HGNC file. Removing temporary file.")
                os.remove(temp_file)
            else:
                logging.info("Update detected for HGNC file.")
                backup_file = self.output_path + ".backup"
                shutil.copy2(self.output_path, backup_file)
                logging.info(f"Backed up old HGNC file to {backup_file}")
                os.replace(temp_file, self.output_path)
                update_detected = True
                old_md5 = old_hash
                new_md5 = new_hash
        else:
            logging.info("HGNC output file does not exist. Creating new file.")
            os.replace(temp_file, self.output_path)
            update_detected = True
            new_md5 = self.compute_hash(self.output_path)

        # -------------------- Diff File Creation --------------------
        diff_txt, diff_html = None, None
        backup_file = self.output_path + ".backup"
        if update_detected and os.path.exists(backup_file):
            try:
                with open(backup_file, "r", encoding="utf-8", errors="ignore") as old_f:
                    old_lines = old_f.readlines()
                with open(self.output_path, "r", encoding="utf-8", errors="ignore") as new_f:
                    new_lines = new_f.readlines()
                # Generate plain-text diff
                diff = list(difflib.unified_diff(old_lines, new_lines,
                                                 fromfile="old_version", tofile="new_version"))
                diff_text = "".join(diff)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                base_name = os.path.splitext(os.path.basename(self.output_path))[0]
                diff_txt = f"{base_name}_diff_{timestamp}.txt"
                diff_html = f"{base_name}_diff_{timestamp}.html"
                with open(diff_txt, "w", encoding="utf-8") as f:
                    f.write(diff_text)
                # Generate HTML diff with only changed rows (minimal context)
                html_diff = difflib.HtmlDiff().make_file(
                    old_lines, new_lines, fromdesc="Old Version", todesc="New Version",
                    context=True, numlines=0
                )
                with open(diff_html, "w", encoding="utf-8") as f:
                    f.write(html_diff)
                logging.info(f"Diff generated: {diff_txt}, {diff_html}")
            except Exception as e:
                logging.error(f"Error generating diff for HGNC data: {e}")
                diff_txt, diff_html = None, None
        else:
            logging.info("No previous file backup available for diff generation.")
        # ------------------ End Diff File Creation ------------------

        # Build and write detailed metadata
        metadata = {
            "download_url": self.url,
            "downloaded_at": datetime.now().isoformat(),
            "total_size_bytes": total_size,
            "output_path": self.output_path,
            "update_detected": update_detected,
            "old_md5": old_md5,
            "new_md5": new_md5,
            "diff_file_text": diff_txt,
            "diff_file_html": diff_html
        }
        with open(self.meta_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)
        logging.info(f"HGNC download metadata written to {self.meta_file}")
        return True

    def run(self):
        self.download()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and update HGNC data")
    parser.add_argument("--config", type=str, default="config/targets/targets_config.yaml")
    args = parser.parse_args()
    with open(args.config) as f:
        config = yaml.safe_load(f)
    HGNCDownloader(config).run()
