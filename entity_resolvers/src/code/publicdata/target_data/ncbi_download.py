#!/usr/bin/env python
"""
ncbi_download.py - Download NCBI gene_info.gz with update detection.

Uses the original working download logic.
NO raw-file diffs — version tracking happens on cleaned output in the transformer.
"""

import os
import gzip
import shutil
import yaml
import logging
import argparse
from datetime import datetime
import requests
from requests.exceptions import HTTPError
from tqdm import tqdm
import hashlib
import json


def setup_logging(config):
    log_file = config.get("download_log_file") or config.get("log_file", "ncbi_download.log")
    handlers = [logging.StreamHandler()]
    directory = os.path.dirname(log_file)
    if directory:
        os.makedirs(directory, exist_ok=True)
    if log_file:
        handlers.insert(0, logging.FileHandler(log_file))
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s",
                        handlers=handlers, force=True)


class NCBIDownloader:
    def __init__(self, full_config):
        self.config = full_config["ncbi_data"]
        setup_logging(self.config)
        self.url = self.config["download_url"]
        self.output_path = self.config["output_path"]
        self.decompressed_file = self.config.get(
            "decompressed_file",
            "src/data/publicdata/target_data/raw/ncbi_gene_info.tsv"
        )
        self.meta_file = self.config.get("dl_metadata_file", "dl_ncbi_metadata.json")

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

    def detect_ncbi_version(self):
        import email.utils
        try:
            logging.info("Checking Last-Modified header for NCBI gene_info.gz...")
            response = requests.head(self.url, timeout=10)
            last_modified = response.headers.get("Last-Modified")
            if last_modified:
                dt = email.utils.parsedate_to_datetime(last_modified)
                version = dt.strftime("%Y-%m-%d")
                logging.info(f"NCBI gene_info version (Last-Modified): {version}")
                return version
        except Exception as e:
            logging.warning(f"Failed to detect NCBI version: {e}")
        return "unknown"

    def download_and_extract(self):
        # Version-based skip: check if source version is unchanged
        current_version = self.detect_ncbi_version()
        previous_version = self.old_meta.get("source_version")
        if (current_version and current_version != "unknown"
                and previous_version == current_version
                and os.path.exists(self.output_path)
                and os.path.exists(self.decompressed_file)):
            logging.info(
                f"NCBI version unchanged ({current_version}) and files present — skipping download."
            )
            metadata = {
                "source_name": "NCBI Gene Info",
                "source_version": current_version,
                "url": self.url,
                "download_start": datetime.now().isoformat(),
                "download_end": datetime.now().isoformat(),
                "updated": False,
                "status": "no_change",
                "output_path": self.output_path,
                "decompressed_file": self.decompressed_file,
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
            logging.info(f"Downloading {self.url}...")
            with requests.get(self.url, stream=True) as response:
                response.raise_for_status()
                total_size = int(response.headers.get("content-length", 0))
                tqdm_bar = tqdm(total=total_size, unit="iB", unit_scale=True)
                with open(temp_file, "wb") as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
                        tqdm_bar.update(len(chunk))
                tqdm_bar.close()
        except HTTPError as e:
            logging.error(f"Error downloading NCBI data: {e}")
            return False

        if os.path.exists(self.output_path):
            old_md5 = self.compute_hash(self.output_path)
            new_md5 = self.compute_hash(temp_file)
            if old_md5 == new_md5:
                logging.info(f"No updates detected. Removing temporary file.")
                os.remove(temp_file)
            else:
                logging.info(f"Update detected. Replacing file.")
                os.replace(temp_file, self.output_path)
                update_detected = True
        else:
            logging.info(f"{self.output_path} does not exist. Creating new file.")
            os.replace(temp_file, self.output_path)
            update_detected = True
            new_md5 = self.compute_hash(self.output_path)

        # Decompress
        logging.info(f"Decompressing {self.output_path} to {self.decompressed_file}...")
        os.makedirs(os.path.dirname(self.decompressed_file), exist_ok=True)
        with gzip.open(self.output_path, "rb") as f_in:
            with open(self.decompressed_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        compressed_size = os.path.getsize(self.output_path) if os.path.exists(self.output_path) else None
        decompressed_size = os.path.getsize(self.decompressed_file) if os.path.exists(self.decompressed_file) else None

        # Standardized metadata for version manifest
        metadata = {
            "source_name": "NCBI Gene Info",
            "source_version": current_version,
            "url": self.url,
            "download_start": datetime.now().isoformat(),
            "download_end": datetime.now().isoformat(),
            "total_size_bytes": total_size,
            "output_path": self.output_path,
            "compressed_size_bytes": compressed_size,
            "decompressed_file": self.decompressed_file,
            "decompressed_size_bytes": decompressed_size,
            "updated": update_detected,
            "status": "updated" if update_detected else "no_change",
            "old_md5": old_md5,
            "new_md5": new_md5,
        }
        os.makedirs(os.path.dirname(self.meta_file), exist_ok=True)
        with open(self.meta_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)
        logging.info(f"NCBI download metadata written to {self.meta_file}")
        return True

    def run(self):
        self.download_and_extract()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and decompress NCBI gene_info data")
    parser.add_argument("--config", type=str, default="config/targets_config.yaml")
    args = parser.parse_args()
    with open(args.config) as f:
        config = yaml.safe_load(f)
    NCBIDownloader(config).run()