#!/usr/bin/env python3
"""
nodenorm_gene_download.py — Fetch and filter human gene records from NodeNorm dumps.

The download URL is set manually in targets_config.yaml via url_base.
Examples:
  url_base: "https://stars.renci.org/var/babel_outputs/latest/compendia/"
  url_base: "https://stars.renci.org/var/babel_outputs/2025nov19/compendia/"

Skip logic:
  1) Fetch the directory listing at url_base — parse date + size for Gene.txt files
  2) Compare against stored metadata from last run
  3) Only download files whose date or size changed
  4) Filter for NCBITaxon:9606 and write JSONL
"""

import os
import re
import json
import yaml
import argparse
import requests
from datetime import datetime
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


def parse_directory_listing(url, filename_prefix):
    """
    Parse an Apache/nginx directory listing page and extract file info.

    Example line:
      <a href="Gene.txt">Gene.txt</a>   03-Apr-2025 05:32   12976948673

    Returns dict: {"Gene.txt": {"date": "03-Apr-2025 05:32", "size": "12976948673"}, ...}
    """
    logging.info(f"Fetching directory listing from {url}")
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()

        file_info = {}
        for line in r.text.splitlines():
            m = re.search(
                rf'href="({re.escape(filename_prefix)}[^"]*)"[^>]*>'
                rf'[^<]*</a>\s+'
                rf'(\d{{2}}-[A-Za-z]{{3}}-\d{{4}}\s+\d{{2}}:\d{{2}})\s+'
                rf'(\d+)',
                line
            )
            if m:
                fname = m.group(1)
                date_str = m.group(2)
                size_str = m.group(3)
                file_info[fname] = {"date": date_str, "size": size_str}

        logging.info(f"Found {len(file_info)} {filename_prefix}* file(s) in listing")
        return file_info
    except Exception as e:
        logging.warning(f"Could not parse directory listing: {e}")
        return {}


def extract_version_from_url(url):
    """
    Extract version string from url_base path.
    e.g. ".../2025nov19/compendia/" → "2025nov19"
         ".../latest/compendia/"   → "latest"
    """
    parts = [p for p in url.rstrip("/").split("/") if p]
    # Walk backwards to find "compendia", then the part before it is the version
    for i, p in enumerate(parts):
        if p == "compendia" and i > 0:
            return parts[i - 1]
    return "unknown"


class NodeNormGeneDownloader:
    def __init__(self, cfg):
        c = cfg["nodenorm_genes"]
        setup_logging(c.get("download_log_file") or c.get("log_file", "nodenorm_gene_download.log"))

        self.base_url = c["url_base"].rstrip("/") + "/"
        self.version = extract_version_from_url(self.base_url)
        self.raw_jsonl = c["raw_file"]
        self.meta_file = c["dl_metadata_file"]

        # Load previous metadata
        self.old_meta = {}
        if os.path.exists(self.meta_file):
            try:
                with open(self.meta_file) as f:
                    self.old_meta = json.load(f)
            except Exception:
                pass

        self.new_meta = {
            "source_name": "NodeNorm Gene",
            "source_version": self.version,
            "url_base": self.base_url,
            "download_start": datetime.now().isoformat(),
            "download_end": None,
            "status": "unknown",
            "updated": False,
            "url": None,
            "files": {},
            "outputs": [],
        }

    def _download(self, url, dst):
        dst_dir = os.path.dirname(dst)
        if dst_dir:
            os.makedirs(dst_dir, exist_ok=True)
        with requests.get(url, stream=True, timeout=600) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            seen = 0
            with open(dst, "wb") as f:
                for chunk in r.iter_content(65536):
                    if not chunk:
                        continue
                    f.write(chunk)
                    seen += len(chunk)
                    if total:
                        print(f"\r{os.path.basename(dst)}: {seen/total*100:.1f}% ", end="", flush=True)
        if total:
            print()

    def _write_meta_and_return(self, status):
        self.new_meta["status"] = status
        self.new_meta["download_end"] = datetime.now().isoformat()
        os.makedirs(os.path.dirname(self.meta_file), exist_ok=True)
        with open(self.meta_file, "w") as m:
            json.dump(self.new_meta, m, indent=2)
        logging.info(f"Metadata → {self.meta_file}")

    def run(self):
        logging.info(f"NodeNorm Gene — using url_base: {self.base_url} (version: {self.version})")
        self.new_meta["url"] = self.base_url + "Gene.txt"
        have_raw = os.path.exists(self.raw_jsonl)

        # ── Step 1: Check if url_base changed since last run ─────────────
        old_url_base = self.old_meta.get("url_base")
        if old_url_base and old_url_base != self.base_url:
            logging.info(f"url_base changed: {old_url_base} → {self.base_url} — will re-download.")

        # ── Step 2: Parse directory listing for date/size ────────────────
        remote_files = parse_directory_listing(self.base_url, "Gene.txt")
        self.new_meta["files"] = remote_files

        if not remote_files:
            logging.error("No Gene.txt files found in directory listing. Aborting.")
            self._write_meta_and_return("error")
            return

        # ── Step 3: Compare against previous listing ─────────────────────
        old_files = self.old_meta.get("files", {})
        files_to_download = []

        for fname, info in remote_files.items():
            old_info = old_files.get(fname, {})
            same_date = info.get("date") == old_info.get("date")
            same_size = info.get("size") == old_info.get("size")
            same_base = (old_url_base == self.base_url) if old_url_base else True

            if same_date and same_size and same_base:
                logging.info(f"Unchanged: {fname} ({info['date']}, {info['size']} bytes)")
            else:
                if old_info and same_base:
                    logging.info(f"Changed: {fname} — date: {old_info.get('date')} → {info.get('date')}, "
                                 f"size: {old_info.get('size')} → {info.get('size')}")
                else:
                    logging.info(f"New/changed source: {fname} ({info['date']}, {info['size']} bytes)")
                files_to_download.append(fname)

        # If nothing changed AND we have the JSONL, skip
        if not files_to_download and have_raw:
            logging.info("All Gene.txt files unchanged and JSONL exists — skipping download.")
            self._write_meta_and_return("no_change")
            return

        if not files_to_download and not have_raw:
            logging.info(f"Files unchanged but JSONL missing at {self.raw_jsonl} — re-downloading all.")
            files_to_download = list(remote_files.keys())

        # ── Step 4: Download changed files, filter for human ─────────────
        raw_lines = []
        for fname in files_to_download:
            url = self.base_url + fname
            tmp_name = fname
            logging.info(f"Downloading {url} ({remote_files[fname]['size']} bytes)")
            self._download(url, tmp_name)

            with open(tmp_name, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    if '"NCBITaxon:9606"' in line:
                        raw_lines.append(line)
            try:
                os.remove(tmp_name)
            except OSError:
                pass

        # ── Step 5: Write JSONL ──────────────────────────────────────────
        if not raw_lines:
            logging.info("No new human lines found.")
            self._write_meta_and_return("no_change")
        else:
            os.makedirs(os.path.dirname(self.raw_jsonl), exist_ok=True)
            with open(self.raw_jsonl, "w", encoding="utf-8") as f:
                f.write("".join(raw_lines))
            logging.info(f"Wrote filtered JSONL: {self.raw_jsonl} ({len(raw_lines)} human records)")
            self.new_meta["updated"] = True
            self.new_meta["outputs"].append({"path": self.raw_jsonl, "records": len(raw_lines)})
            self._write_meta_and_return("updated")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config/targets_config.yaml")
    args = p.parse_args()
    cfg = yaml.safe_load(open(args.config))
    NodeNormGeneDownloader(cfg).run()