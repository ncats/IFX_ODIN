#!/usr/bin/env python
"""
hgnc_download.py - Download HGNC data with update detection.

Uses the original working download logic.
NO raw-file diffs — version tracking happens on cleaned output in the transformer.
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
import json
import shutil
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def setup_logging(config):
    log_file = config.get("download_log_file") or config.get("log_file", "hgnc_download.log")
    handlers = [logging.StreamHandler()]
    directory = os.path.dirname(log_file)
    if directory:
        os.makedirs(directory, exist_ok=True)
    if log_file:
        handlers.insert(0, logging.FileHandler(log_file))
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s",
                        handlers=handlers, force=True)


class HGNCDownloader:
    def __init__(self, full_config):
        self.config = full_config["hgnc_data"]
        setup_logging(self.config)
        self.url = self.config["download_url"]
        self.output_path = self.config["output_path"]
        self.meta_file = self.config.get("dl_metadata_file", "dl_hgnc_metadata.json")

        # Load previous metadata to compare versions
        self.old_meta = {}
        if os.path.exists(self.meta_file):
            try:
                with open(self.meta_file) as f:
                    self.old_meta = json.load(f)
            except Exception:
                pass

    def compute_hash(self, file_path):
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def detect_hgnc_version(self):
        try:
            from email.utils import parsedate_to_datetime
            logging.info("Checking Last-Modified header for HGNC file...")
            response = requests.head(self.url, timeout=10)
            last_modified = response.headers.get("Last-Modified")
            if last_modified:
                dt = parsedate_to_datetime(last_modified)
                version = dt.strftime("%Y-%m-%d")
                logging.info(f"HGNC version from Last-Modified: {version}")
                return version
        except Exception as e:
            logging.warning(f"Failed to retrieve HGNC Last-Modified header: {e}")
        return "unknown"

    def download(self):
        # Version-based skip
        current_version = self.detect_hgnc_version()
        previous_version = self.old_meta.get("source_version")
        if (current_version and current_version != "unknown"
                and previous_version == current_version
                and os.path.exists(self.output_path)):
            logging.info(
                f"HGNC version unchanged ({current_version}) and file present — skipping download."
            )
            metadata = {
                "source_name": "HGNC",
                "source_version": current_version,
                "url": self.url,
                "download_start": datetime.now().isoformat(),
                "download_end": datetime.now().isoformat(),
                "updated": False,
                "status": "no_change",
                "output_path": self.output_path,
            }
            os.makedirs(os.path.dirname(self.meta_file), exist_ok=True)
            with open(self.meta_file, "w") as f:
                json.dump(metadata, f, indent=2)
            return True

        temp_file = self.output_path + ".temp"
        update_detected = False
        old_md5 = None
        new_md5 = None
        total_size = 0

        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)

        try:
            logging.info(f"Downloading HGNC data from {self.url}...")
            with requests.get(self.url, stream=True, verify=False) as response:
                response.raise_for_status()
                total_size = int(response.headers.get("content-length", 0))
                tqdm_bar = tqdm(total=total_size, unit="iB", unit_scale=True)
                with open(temp_file, "wb") as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
                        tqdm_bar.update(len(chunk))
                tqdm_bar.close()
        except HTTPError as e:
            logging.error(f"Error downloading HGNC data: {e}")
            return False

        if os.path.exists(self.output_path):
            old_md5 = self.compute_hash(self.output_path)
            new_md5 = self.compute_hash(temp_file)
            if old_md5 == new_md5:
                logging.info("No updates detected for HGNC file. Removing temporary file.")
                os.remove(temp_file)
            else:
                logging.info("Update detected for HGNC file.")
                os.replace(temp_file, self.output_path)
                update_detected = True
        else:
            logging.info("HGNC output file does not exist. Creating new file.")
            os.replace(temp_file, self.output_path)
            update_detected = True
            new_md5 = self.compute_hash(self.output_path)

        # Standardized metadata for version manifest
        metadata = {
            "source_name": "HGNC",
            "source_version": current_version,
            "url": self.url,
            "download_start": datetime.now().isoformat(),
            "download_end": datetime.now().isoformat(),
            "total_size_bytes": total_size,
            "output_path": self.output_path,
            "updated": update_detected,
            "status": "updated" if update_detected else "no_change",
            "old_md5": old_md5,
            "new_md5": new_md5,
        }
        os.makedirs(os.path.dirname(self.meta_file), exist_ok=True)
        with open(self.meta_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)
        logging.info(f"HGNC download metadata written to {self.meta_file}")
        return True

    def run(self):
        self.download()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and update HGNC data")
    parser.add_argument("--config", type=str, default="config/targets_config.yaml")
    args = parser.parse_args()
    with open(args.config) as f:
        config = yaml.safe_load(f)
    HGNCDownloader(config).run()