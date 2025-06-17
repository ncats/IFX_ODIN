#!/usr/bin/env python
"""
nodenorm_disease_download.py - Fetch NodeNorm Disease.txt dump from latest version

This script:
  1. Finds the latest version folder on the NodeNorm server.
  2. HEAD-checks the Disease.txt file for changes.
  3. Downloads only if changed, streaming progress.
  4. Writes detailed metadata and (optionally) a diff of the old vs new file.
"""

import os
import re
import json
import yaml
import argparse
import hashlib
import difflib
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from logging.handlers import RotatingFileHandler
import logging

def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if root.hasHandlers():
        root.handlers.clear()
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh = RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=3)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(sh)

def get_latest_version(base_url):
    logging.info(f"Checking for latest version at {base_url}")
    try:
        r = requests.get(base_url)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        dirs = [a["href"].rstrip("/") for a in soup.select("a[href$='/']")]
        vers = [d for d in dirs if re.match(r"^\d{4}[A-Za-z]{3}\d{2}$", d)]
        if not vers:
            logging.info("No version directories found.")
            return None
        latest = sorted(vers)[-1]
        logging.info(f"Latest version detected: {latest}")
        return latest
    except Exception as e:
        logging.warning(f"Couldn't fetch versions from {base_url}: {e}")
        return None

class NodeNormDiseaseDownloader:
    def __init__(self, cfg):
        c = cfg["nodenorm"]
        setup_logging(c["log_file"])
        self.base_url_template = c["url_base"]
        self.raw_file = c["raw_file"]
        self.meta_file = c["dl_metadata_file"]
        self.diff_base = c["diff_file"]

        # load old metadata
        if os.path.exists(self.meta_file):
            with open(self.meta_file) as f:
                self.old_meta = json.load(f)
        else:
            self.old_meta = {"files": {}}

        self.new_meta = {
            "downloaded_at": datetime.now().isoformat(),
            "files": {},
            "raw_file": self.raw_file,
            "diff_txt": None
        }

    def _head(self, url):
        try:
            h = requests.head(url)
            h.raise_for_status()
            return {
                "Last-Modified": h.headers.get("Last-Modified"),
                "Content-Length": h.headers.get("Content-Length")
            }
        except:
            return {}

    def _download(self, url, dst):
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            seen = 0
            with open(dst, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
                    seen += len(chunk)
                    if total:
                        pct = seen / total * 100
                        print(f"\r{dst}: {pct:.1f}% ", end="", flush=True)
            print()

    def run(self):
        ver = get_latest_version(self.base_url_template)
        if ver:
            url = f"{self.base_url_template}{ver}/compendia/Disease.txt"
            logging.info(f"Using versioned URL: {url}")
        else:
            url = f"{self.base_url_template}Disease.txt"
            logging.info("Falling back to default URL")

        fname = os.path.basename(url)
        hdr = self._head(url)
        self.new_meta["files"][url] = hdr
        old_hdr = self.old_meta.get("files", {}).get(url, {})
        if hdr.get("Last-Modified") and hdr["Last-Modified"] == old_hdr.get("Last-Modified"):
            logging.info(f"Skipping unchanged {url}")
            return

        logging.info(f"Downloading {url}")
        self._download(url, fname)

        # Write raw file
        os.makedirs(os.path.dirname(self.raw_file), exist_ok=True)
        prev = None
        if os.path.exists(self.raw_file):
            prev = self.raw_file + ".backup"
            os.replace(self.raw_file, prev)

        os.replace(fname, self.raw_file)
        logging.info(f"Wrote raw file to {self.raw_file}")

        if prev:
            old = open(prev).readlines()
            new = open(self.raw_file).readlines()
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            diff_txt = f"{os.path.splitext(self.diff_base)[0]}_{ts}.txt"
            with open(diff_txt, "w") as d:
                d.write("".join(difflib.unified_diff(old, new)))
            logging.info(f"Diff written to {diff_txt}")
            self.new_meta["diff_txt"] = diff_txt

        with open(self.meta_file, "w") as m:
            json.dump(self.new_meta, m, indent=2)
        logging.info(f"Metadata saved to {self.meta_file}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config/diseases/diseases_config.yaml")
    args = p.parse_args()
    cfg = yaml.safe_load(open(args.config))
    NodeNormDiseaseDownloader(cfg).run()
