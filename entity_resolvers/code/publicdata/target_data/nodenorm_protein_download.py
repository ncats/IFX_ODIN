#!/usr/bin/env python
"""
nodenorm_protein_download.py - Fetch and filter human protein records from NodeNorm dumps

This script:
  1. Finds the latest version folder on the NodeNorm server.
  2. HEAD‑checks each part (or auto‑discovers chunks) for Last-Modified to skip unchanged.
  3. Streams each changed chunk, filters for "NCBITaxon:9606" into a raw JSONL.
  4. Generates a unified‑diff of old vs new JSONL.
  5. Logs to console + rotating file, and writes detailed metadata including version.
"""
import os, re, json, yaml, argparse, difflib, requests
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from logging.handlers import RotatingFileHandler
import logging

def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Prevent duplicate logs by clearing existing handlers
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
        r = requests.get(base_url); r.raise_for_status()
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

class NodeNormProteinDownloader:
    def __init__(self, cfg):
        c = cfg["nodenorm_proteins"]
        logf = c.get("log_file", "nodenorm_protein_download.log")
        setup_logging(logf)

        self.base_url_template = c["url_base"]     # e.g. https://stars.renci.org/var/babel_outputs/
        self.parts             = c.get("file_range")# optional [start,end]
        self.raw_jsonl         = c["raw_file"]      # e.g. .../nodenorm_proteins.jsonl
        self.download_dir = c.get("download_dir", "src/data/publicdata/target_data/raw/nodenorm_chunks")
        os.makedirs(self.download_dir, exist_ok=True)
        self.meta_file         = c["dl_metadata_file"]   # e.g. .../nodenorm_proteins_metadata.json
        self.diff_base         = c["diff_file"]     # e.g. .../nodenorm_proteins_diff.txt

        # load old metadata
        if os.path.exists(self.meta_file):
            try:
                self.old_meta = json.load(open(self.meta_file))
            except Exception:
                self.old_meta = {}
        else:
            self.old_meta = {}

        # we'll record version + files + diff
        self.new_meta = {
            "downloaded_at": datetime.now().isoformat(),
            "version": None,
            "files": {},
            "raw_jsonl": self.raw_jsonl,
            "diff_txt": None
        }

    def _head(self, url):
        try:
            h = requests.head(url); h.raise_for_status()
            return {
                "Last-Modified": h.headers.get("Last-Modified"),
                "Content-Length": h.headers.get("Content-Length")
            }
        except Exception:
            return {}

    def _download(self, url, dst):
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length",0))
            seen = 0
            with open(dst, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
                    seen += len(chunk)
                    if total:
                        pct = seen/total*100
                        print(f"\r{dst}: {pct:.1f}% ", end="", flush=True)
            print()

    def run(self):
        # 1) determine version
        ver = get_latest_version(self.base_url_template)
        self.new_meta["version"] = ver
        prev_ver = self.old_meta.get("version")

        # if we already have this version and JSONL exists → skip entirely
        if ver and prev_ver == ver and os.path.exists(self.raw_jsonl):
            logging.info(f"Already at latest version {ver}, skipping download.")
            return

        # 2) build compendia URL
        if ver:
            comp_url = urljoin(self.base_url_template, f"{ver}/compendia/")
        else:
            comp_url = urljoin(self.base_url_template, "compendia/")
        logging.info(f"Using compendia URL: {comp_url}")

        # 3) pick parts
        if self.parts:
            indices = list(range(self.parts[0], self.parts[1] + 1))
        else:
            # auto‑discover Protein.txt.<NN>
            resp = requests.get(comp_url); resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            indices = sorted(
                int(a["href"].split("Protein.txt.")[-1].rstrip("/"))
                for a in soup.select("a[href^='Protein.txt.']")
            )
            logging.info(f"Auto‑detected parts: {indices}")

        # 4) download + filter
        base_file = comp_url + "Protein.txt"
        raw_lines = []
        for i in indices:
            url = f"{base_file}.{i:02d}"
            hdr = self._head(url)
            self.new_meta["files"][url] = hdr
            if hdr.get("Last-Modified") == self.old_meta.get("files",{}).get(url,{}).get("Last-Modified"):
                logging.info(f"Skipping unchanged {url}")
                continue

            fname = os.path.join(self.download_dir, os.path.basename(url))
            logging.info(f"Downloading chunk {i:02d} → {fname}")
            self._download(url, fname)
            with open(fname) as f:
                for line in f:
                    if '"NCBITaxon:9606"' in line:
                        raw_lines.append(line)
            logging.info(f"Retained raw chunk file: {fname}")

        if not raw_lines:
            logging.info("No new lines; exiting.")
            return

        # 5) write JSONL + diff
        os.makedirs(os.path.dirname(self.raw_jsonl), exist_ok=True)
        prev = None
        if os.path.exists(self.raw_jsonl):
            prev = self.raw_jsonl + ".backup"
            os.replace(self.raw_jsonl, prev)

        with open(self.raw_jsonl, "w") as f:
            f.write("".join(raw_lines))
        logging.info(f"Wrote filtered JSONL: {self.raw_jsonl}")

        if prev:
            old = open(prev).readlines()
            new = open(self.raw_jsonl).readlines()
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            diff_txt = f"{os.path.splitext(self.diff_base)[0]}_{ts}.txt"
            with open(diff_txt, "w") as d:
                d.write("".join(difflib.unified_diff(old, new)))
            logging.info(f"JSONL diff → {diff_txt}")
            self.new_meta["diff_txt"] = diff_txt

        # 6) write metadata
        with open(self.meta_file, "w") as m:
            json.dump(self.new_meta, m, indent=2)
        logging.info(f"Metadata → {self.meta_file}")

if __name__=="__main__":
    p = argparse.ArgumentParser(description="Download & filter NodeNorm Protein data")
    p.add_argument("--config", default="config/targets/targets_config.yaml")
    args = p.parse_args()
    cfg = yaml.safe_load(open(args.config))
    NodeNormProteinDownloader(cfg).run()
