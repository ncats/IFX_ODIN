#!/usr/bin/env python
"""
refseq_download.py - Download RefSeq / Ensembl / UniProt collaborations,
with update detection, human filtering, and logging.

NO raw-file diffs — version tracking happens on cleaned output in the transformer.
"""

import os
import gzip
import shutil
import json
import hashlib
import logging
import argparse
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from tqdm import tqdm
from requests.exceptions import HTTPError
import yaml


def setup_logging(log_file):
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        handlers = [logging.StreamHandler(), logging.FileHandler(log_file)]
    else:
        handlers = [logging.StreamHandler()]
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s",
                        handlers=handlers, force=True)


class RefSeqDownloader:
    def __init__(self, full_cfg):
        cfg = full_cfg["refseq_data"]
        self.refs = cfg["refseq"]
        self.ensembl = cfg["ensembl"]
        self.uniprot = cfg["uniprot"]
        self.log_file = cfg.get("download_log_file") or cfg.get("log_file")
        self.meta_file = cfg.get("dl_metadata_file",
                                 "src/data/publicdata/target_data/metadata/dl_refSeq_metadata.json")
        self.meta = {
            "source_name": "RefSeq (NCBI)",
            "downloads": [],
            "download_start": datetime.now().isoformat(),
        }
        setup_logging(self.log_file)

        # Load previous metadata to compare versions
        self.old_meta = {}
        if os.path.exists(self.meta_file):
            try:
                with open(self.meta_file) as f:
                    self.old_meta = json.load(f)
            except Exception:
                pass

    def _all_decompressed_exist(self):
        return all(os.path.exists(p) for p in [
            self.refs["decompressed"],
            self.ensembl["decompressed"],
            self.uniprot["decompressed"],
        ])

    def _compute_hash(self, path):
        h = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                h.update(chunk)
        return h.hexdigest()

    def _headers_and_version(self, url):
        import email.utils
        try:
            r = requests.head(url, timeout=10)
            last_mod = r.headers.get("Last-Modified")
            version = "unknown"
            if last_mod:
                dt = email.utils.parsedate_to_datetime(last_mod)
                version = dt.strftime("%Y-%m-%d")
            return version, {"Last-Modified": last_mod, "ETag": r.headers.get("ETag")}
        except Exception as e:
            logging.warning(f"HEAD failed for {url}: {e}")
            return "unknown", {}

    def _download_and_replace(self, url, out_path):
        tmp = out_path + ".tmp"
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        logging.info(f"Downloading {url}")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            with tqdm(total=total, unit="iB", unit_scale=True, desc=os.path.basename(out_path)) as bar:
                with open(tmp, "wb") as f:
                    for ch in r.iter_content(1024):
                        f.write(ch)
                        bar.update(len(ch))

        updated, old_md5, new_md5 = False, None, None
        if os.path.exists(out_path):
            old_md5 = self._compute_hash(out_path)
            new_md5 = self._compute_hash(tmp)
            if old_md5 != new_md5:
                os.replace(tmp, out_path)
                updated = True
            else:
                os.remove(tmp)
        else:
            os.replace(tmp, out_path)
            new_md5 = self._compute_hash(out_path)
            updated = True
        return updated, old_md5, new_md5

    def fetch_and_process_refseq(self):
        logging.info("=== REFSEQ gene2refseq.gz ===")
        upd, old, new = self._download_and_replace(self.refs["download_url"], self.refs["path"])

        os.makedirs(os.path.dirname(self.refs["decompressed"]), exist_ok=True)
        with gzip.open(self.refs["path"], "rb") as zin, open(self.refs["decompressed"], "wb") as zout:
            shutil.copyfileobj(zin, zout)

        df = pd.read_csv(self.refs["decompressed"], sep="\t", dtype=str)
        if "#tax_id" in df.columns:
            before = len(df)
            df = df[df["#tax_id"] == "9606"]
            logging.info(f"Human filter: {before} → {len(df)} rows")
            df.to_csv(self.refs["decompressed"], sep="\t", index=False)

        version, hdrs = self._headers_and_version(self.refs["download_url"])
        self.meta["downloads"].append({
            "name": "gene2refseq", "url": self.refs["download_url"],
            "version": version, "updated": upd, "old_hash": old, "new_hash": new,
        })

    def fetch_and_process_ensembl(self):
        logging.info("=== REFSEQ gene2ensembl.gz ===")
        upd, old, new = self._download_and_replace(self.ensembl["url"], self.ensembl["path"])

        raw = gzip.decompress(open(self.ensembl["path"], "rb").read()).decode("utf-8")
        df = pd.read_csv(StringIO(raw), sep="\t", dtype=str)
        if "#tax_id" in df.columns:
            before = len(df)
            df = df[df["#tax_id"] == "9606"]
            logging.info(f"Human filter: {before} → {len(df)} rows")

        os.makedirs(os.path.dirname(self.ensembl["decompressed"]), exist_ok=True)
        df.to_csv(self.ensembl["decompressed"], index=False)

        version, hdrs = self._headers_and_version(self.ensembl["url"])
        self.meta["downloads"].append({
            "name": "gene2ensembl", "url": self.ensembl["url"],
            "version": version, "updated": upd, "old_hash": old, "new_hash": new,
        })

    def fetch_and_process_uniprot(self):
        logging.info("=== REFSEQ gene_refseq_uniprotkb_collab.gz ===")
        upd, old, new = self._download_and_replace(self.uniprot["url"], self.uniprot["path"])

        total, rows, hdr = 0, [], None
        with gzip.open(self.uniprot["path"], "rt") as zin:
            for line in zin:
                if line.startswith("#"):
                    hdr = line.strip().split("\t")
                    continue
                total += 1
                cols = line.strip().split("\t")
                if cols[2] == "9606":
                    rows.append(cols)
        logging.info(f"Parsed {total} rows, {len(rows)} human entries")

        os.makedirs(os.path.dirname(self.uniprot["decompressed"]), exist_ok=True)
        pd.DataFrame(rows, columns=hdr).to_csv(self.uniprot["decompressed"], index=False)

        version, hdrs = self._headers_and_version(self.uniprot["url"])
        self.meta["downloads"].append({
            "name": "gene_refseq_uniprotkb", "url": self.uniprot["url"],
            "version": version, "updated": upd, "old_hash": old, "new_hash": new,
        })

    def run(self):
        # Version-based skip: check all 3 source files
        current_version, _ = self._headers_and_version(self.refs["download_url"])
        previous_version = self.old_meta.get("source_version")
        if (current_version and current_version != "unknown"
                and previous_version == current_version
                and self._all_decompressed_exist()):
            logging.info(
                f"RefSeq version unchanged ({current_version}) and all files present — skipping download."
            )
            self.meta["download_end"] = datetime.now().isoformat()
            self.meta["source_version"] = current_version
            self.meta["updated"] = False
            self.meta["status"] = "no_change"
            os.makedirs(os.path.dirname(self.meta_file), exist_ok=True)
            with open(self.meta_file, "w") as f:
                json.dump(self.meta, f, indent=2)
            return

        self.fetch_and_process_refseq()
        self.fetch_and_process_ensembl()
        self.fetch_and_process_uniprot()

        self.meta["download_end"] = datetime.now().isoformat()
        self.meta["updated"] = any(d["updated"] for d in self.meta["downloads"])
        self.meta["status"] = "updated" if self.meta["updated"] else "no_change"
        # Use the newest version across all three files
        versions = [d["version"] for d in self.meta["downloads"] if d["version"] != "unknown"]
        self.meta["source_version"] = max(versions) if versions else "unknown"

        os.makedirs(os.path.dirname(self.meta_file), exist_ok=True)
        with open(self.meta_file, "w") as f:
            json.dump(self.meta, f, indent=2)
        logging.info("RefSeq downloads complete.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config/targets_config.yaml")
    args = p.parse_args()
    cfg = yaml.safe_load(open(args.config))
    RefSeqDownloader(cfg).run()