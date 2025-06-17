#!/usr/bin/env python
"""
ncbi_download.py - Download NCBI gene_info.gz with update detection,
detailed metadata logging, MD5 hash checking, and diff generation.
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
import difflib
import json

def setup_logging(config):
    # Get log file from config or use a default file name.
    log_file = config.get("log_file", "ncbi_download.log")
    handlers = [logging.StreamHandler()]
    # Create the directory for the log file if a directory is specified.
    directory = os.path.dirname(log_file)
    if directory:
        os.makedirs(directory, exist_ok=True)
    # Add file handler
    if log_file:
        handlers.insert(0, logging.FileHandler(log_file))
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s",
                        handlers=handlers)

class NCBIDownloader:
    def __init__(self, full_config):
        self.config = full_config["ncbi_data"]
        # Initialize logging with the provided config
        setup_logging(self.config)
        self.url = self.config["download_url"]
        self.output_path = self.config["output_path"]  # Compressed file location
        self.decompressed_file = self.config.get("decompressed_file",
                                                 "src/data/publicdata/target_data/raw/ncbi_gene_info.tsv")
        self.meta_file = self.config.get("dl_metadata_file", "dl_ncbi_metadata.json")
        self.base_diff_file = self.config.get("diff_file", "ncbi_diff.txt")
    
    def compute_hash(self, file_path):
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def download_and_extract(self):
        temp_file = self.output_path + ".temp"
        update_detected = False
        old_md5 = None
        new_md5 = None
        total_size = 0
        
        # Download to a temporary file
        try:
            logging.info(f"Downloading {self.url} to {temp_file}...")
            with requests.get(self.url, stream=True) as response:
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
            logging.error(f"Error downloading NCBI data: {e}")
            return False
        
        # Check if the compressed file already exists and compare MD5 hashes
        if os.path.exists(self.output_path):
            old_hash = self.compute_hash(self.output_path)
            new_hash = self.compute_hash(temp_file)
            if old_hash == new_hash:
                logging.info(f"No updates detected for {self.output_path}. Removing temporary file.")
                os.remove(temp_file)
            else:
                logging.info(f"Update detected for {self.output_path}. Replacing file.")
                # Backup existing decompressed file for diff generation if available
                if os.path.exists(self.decompressed_file):
                    backup_decompressed = self.decompressed_file + ".backup"
                    shutil.copy2(self.decompressed_file, backup_decompressed)
                    logging.info(f"Backed up old decompressed file to {backup_decompressed}")
                os.replace(temp_file, self.output_path)
                update_detected = True
                old_md5 = old_hash
                new_md5 = new_hash
        else:
            logging.info(f"{self.output_path} does not exist. Creating new file.")
            os.replace(temp_file, self.output_path)
            update_detected = True
            new_md5 = self.compute_hash(self.output_path)
        
        # Decompress the downloaded file
        logging.info(f"Decompressing {self.output_path} to {self.decompressed_file}...")
        with gzip.open(self.output_path, "rb") as f_in:
            with open(self.decompressed_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Capture file sizes
        compressed_size = os.path.getsize(self.output_path) if os.path.exists(self.output_path) else None
        decompressed_size = os.path.getsize(self.decompressed_file) if os.path.exists(self.decompressed_file) else None
        
        # Create versioned diff filenames using current timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        versioned_diff_file = f"{os.path.splitext(self.base_diff_file)[0]}_{timestamp}.txt"
        versioned_html_diff_file = f"{os.path.splitext(self.base_diff_file)[0]}_{timestamp}.html"
        
        # Generate diff if update detected and a backup of the previous decompressed file exists
        diff_generated = False
        diff_summary = ""
        html_diff_generated = False
        backup_decompressed = self.decompressed_file + ".backup"
        if update_detected and os.path.exists(backup_decompressed):
            try:
                with open(backup_decompressed, "r", encoding="utf-8", errors="ignore") as old_file, \
                     open(self.decompressed_file, "r", encoding="utf-8", errors="ignore") as new_file:
                    old_lines = old_file.readlines()
                    new_lines = new_file.readlines()
                    
                    # Generate plain text diff
                    diff = list(difflib.unified_diff(old_lines, new_lines,
                                                     fromfile="old_version", tofile="new_version"))
                    diff_text = "".join(diff)
                    with open(versioned_diff_file, "w", encoding="utf-8") as dfile:
                        dfile.write(diff_text)
                    diff_generated = True
                    diff_summary = diff_text[:500]  # Store a summary (first 500 characters)
                    logging.info(f"Diff generated and written to {versioned_diff_file}")
                    
                    # Generate an HTML diff that only shows changed rows (minimal context)
                    html_diff = difflib.HtmlDiff().make_file(
                        old_lines, new_lines, fromdesc="Old Version", todesc="New Version",
                        context=True, numlines=0
                    )
                    with open(versioned_html_diff_file, "w", encoding="utf-8") as html_file:
                        html_file.write(html_diff)
                    html_diff_generated = True
                    logging.info(f"HTML diff generated and written to {versioned_html_diff_file}")
            except Exception as e:
                logging.error(f"Error generating diff: {e}")
        else:
            logging.info("No previous decompressed file available for diff generation.")
        
        # Build detailed metadata
        metadata = {
            "download_url": self.url,
            "downloaded_at": datetime.now().isoformat(),
            "total_size_bytes": total_size,
            "output_path": self.output_path,
            "compressed_size_bytes": compressed_size,
            "decompressed_file": self.decompressed_file,
            "decompressed_size_bytes": decompressed_size,
            "update_detected": update_detected,
            "old_md5": old_md5,
            "new_md5": new_md5,
            "diff_file_text": versioned_diff_file if diff_generated else None,
            "diff_file_html": versioned_html_diff_file if html_diff_generated else None,
            "diff_summary": diff_summary
        }
        with open(self.meta_file, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        logging.info(f"NCBI download metadata written to {self.meta_file}")
        return True
    
    def run(self):
        self.download_and_extract()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and decompress NCBI gene_info data")
    parser.add_argument("--config", type=str, default="config/targets/targets_config.yaml")
    args = parser.parse_args()
    
    with open(args.config) as f:
        config = yaml.safe_load(f)
    
    NCBIDownloader(config).run()
