#!/usr/bin/env python
"""
pathwaycommons_download.py - Download PathwayCommons files with update detection,
diff generation, logging, metadata tracking, and optional xref enrichment.
"""

import os
import yaml
import json
import shutil
import logging
import argparse
import requests
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from bs4 import BeautifulSoup
import hashlib
import difflib
import urllib3
import gzip

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def setup_logging(config):
    log_file = config.get("log_file", "pathwaycommons_download.log")
    handlers = [logging.StreamHandler()]
    directory = os.path.dirname(log_file)
    if directory:
        os.makedirs(directory, exist_ok=True)
    if log_file:
        handlers.insert(0, logging.FileHandler(log_file))
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s",
                        handlers=handlers)

class PathwayCommonsDownloader:
    def __init__(self, full_config):
        self.config = full_config["pathways"]["pathwaycommons"]
        self.full_config = full_config
        setup_logging(self.config)
        self.base_url = self.config["base_url"]
        self.raw_dir = self.config["raw_dir"]
        self.meta_file = self.config["metadata_file"]
        self.qc_dir = self.config.get("qc_dir", "qc/")
        self.xref_output = self.config.get("xref_output_file", os.path.join(self.raw_dir, "xref_mappings.csv"))
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.qc_dir, exist_ok=True)

    def get_latest_version(self):
        try:
            url = "https://download.baderlab.org/PathwayCommons/PC2/"
            html = requests.get(url).text
            soup = BeautifulSoup(html, "html.parser")

            version_info = {}
            for line in soup.get_text().splitlines():
                if line.startswith("v") and "/" in line:
                    parts = line.split()
                    version = parts[0].strip("/")
                    if version[1:].isdigit():
                        last_modified = " ".join(parts[1:4]) if len(parts) >= 4 else "Unknown"
                        version_info[version] = last_modified

            if not version_info:
                logging.warning("\u26a0\ufe0f No valid versions found on PathwayCommons site.")
                return None

            sorted_versions = sorted(version_info.keys(), key=lambda v: int(v[1:]))
            latest_version = sorted_versions[-1]
            last_modified = version_info[latest_version]

            logging.info(f"🧪 Latest PathwayCommons version: {latest_version} (last modified: {last_modified})")
            return latest_version
        except Exception as e:
            logging.error(f"Error checking PathwayCommons version: {e}")
            return None

    def compute_md5(self, path):
        hash_md5 = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def extract_human_pathway_ids(self):
        pathway_file = os.path.join(self.raw_dir, "pathways.txt.gz")
        if not os.path.exists(pathway_file):
            logging.warning("\u26a0\ufe0f Pathway file not found: pathways.txt.gz")
            return []

        ids = []
        with gzip.open(pathway_file, "rt", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if "Homo sapiens" in line:
                    parts = line.strip().split("\t")
                    if parts:
                        ids.append(parts[0])
        logging.info(f"\u2705 Found {len(ids)} human pathways in pathways.txt.gz")
        return ids

    def enrich_with_xrefs(self, pathway_ids):
        rows = []
        for pid in tqdm(pathway_ids, desc="Fetching xrefs"):
            try:
                r = requests.get(
                    "https://api.pathwaycommons.org/pc2/get",
                    params={"uri": pid, "format": "JSON-LD"},
                    timeout=20
                )
                r.raise_for_status()
                data = r.json()
                for item in data.get("@graph", []):
                    if item.get("@type") == "Pathway":
                        xrefs = item.get("xref", [])
                        for x in xrefs:
                            db = x.get("db")
                            xid = x.get("id")
                            if db and xid:
                                rows.append({
                                    "pathway_uri": pid,
                                    "xref_db": db,
                                    "xref_id": xid
                                })
            except Exception as e:
                logging.warning(f"Could not retrieve xrefs for {pid}: {e}")

        df = pd.DataFrame(rows)
        sep = "\t" if self.xref_output.endswith(".tsv") else ","
        df.to_csv(self.xref_output, index=False, sep=sep)
        logging.info(f"\u2705 Saved xref mappings to {self.xref_output}")

    def download_all(self):
        latest_version = self.get_latest_version()
        if latest_version and not self.base_url.endswith(latest_version + "/"):
            logging.warning(f"\u26a0\ufe0f Config points to {self.base_url} but latest is {latest_version}.")

        metadata = {
            "downloaded_at": str(datetime.now()),
            "base_url": self.base_url,
            "files": {}
        }

        for fname, props in self.config.get("files", {}).items():
            url = self.base_url + fname
            dest = os.path.join(self.raw_dir, fname)
            temp = dest + ".temp"
            logging.info(f"⬇ Downloading {url} to {temp}...")
            try:
                with requests.get(url, stream=True, verify=False) as r:
                    r.raise_for_status()
                    total = int(r.headers.get("content-length", 0))
                    with open(temp, "wb") as f, tqdm(total=total, unit="B", unit_scale=True) as pbar:
                        for chunk in r.iter_content(1024):
                            f.write(chunk)
                            pbar.update(len(chunk))
            except Exception as e:
                logging.error(f"Failed to download {url}: {e}")
                continue

            update = False
            old_md5, new_md5 = None, self.compute_md5(temp)
            if os.path.exists(dest):
                old_md5 = self.compute_md5(dest)
                if old_md5 == new_md5:
                    os.remove(temp)
                    logging.info(f"No update for {fname}.")
                else:
                    update = True
                    backup = dest + ".backup"
                    shutil.copy2(dest, backup)
                    os.replace(temp, dest)
                    logging.info(f"Updated {fname}, backup saved.")
            else:
                os.replace(temp, dest)
                update = True

            diff_txt = None
            if update and os.path.exists(dest + ".backup"):
                try:
                    with open(dest + ".backup", "r", encoding="utf-8", errors="ignore") as f1:
                        old = f1.readlines()
                    with open(dest, "r", encoding="utf-8", errors="ignore") as f2:
                        new = f2.readlines()
                    diff = list(difflib.unified_diff(old, new, fromfile="old", tofile="new"))
                    diff_txt = os.path.join(self.qc_dir, f"{fname}.diff.txt")
                    with open(diff_txt, "w", encoding="utf-8") as f:
                        f.write("".join(diff))
                    logging.info(f"Diff written to {diff_txt}")
                except Exception as e:
                    logging.warning(f"Could not create diff for {fname}: {e}")

            metadata["files"][fname] = {
                "url": url,
                "output_path": dest,
                "update_detected": update,
                "old_md5": old_md5,
                "new_md5": new_md5,
                "diff_file": diff_txt
            }

        with open(self.meta_file, "w") as f:
            json.dump(metadata, f, indent=2)
        logging.info(f"\u2705 Metadata written to {self.meta_file}")

        if self.config.get("fetch_xrefs", False):
            human_ids = self.extract_human_pathway_ids()
            if human_ids:
                self.enrich_with_xrefs(human_ids)

    def run(self):
        self.download_all()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download PathwayCommons data")
    parser.add_argument("--config", type=str, default="config/pathways_config.yaml")
    args = parser.parse_args()
    with open(args.config) as f:
        config = yaml.safe_load(f)
    PathwayCommonsDownloader(config).run()
