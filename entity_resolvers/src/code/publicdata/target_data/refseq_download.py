#!/usr/bin/env python
"""
refseq_download.py - Download RefSeq / Ensembl / UniProt collaborations,
with update detection, zero-context diffs, human filtering, and logging.
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
import subprocess
from io import StringIO
from datetime import datetime
from tqdm import tqdm
from requests.exceptions import HTTPError
import yaml

def setup_logging(log_file):
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        handlers = [logging.StreamHandler(), logging.FileHandler(log_file)]
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=handlers,
            force=True
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

class RefSeqDownloader:
    def __init__(self, full_cfg):
        cfg = full_cfg["refseq_data"]
        self.refs      = cfg["refseq"]
        self.ensembl   = cfg["ensembl"]
        self.uniprot   = cfg["uniprot"]
        self.diff_base = cfg.get("diff_file")
        self.log_file  = cfg.get("log_file")
        self.meta      = {"downloads": [], "timestamp": {"start": datetime.now().isoformat()}}
        setup_logging(self.log_file)

    def _compute_hash(self, path):
        h = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                h.update(chunk)
        return h.hexdigest()

    def _download_and_replace(self, url, out_path):
        tmp = out_path + ".tmp"
        logging.info(f"Downloading {url} → {tmp}")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total = int(r.headers.get("content-length",0))
            with tqdm(total=total, unit="iB", unit_scale=True, desc=os.path.basename(out_path)) as bar:
                with open(tmp, "wb") as f:
                    for ch in r.iter_content(1024):
                        f.write(ch)
                        bar.update(len(ch))
        updated = False
        old_md5 = new_md5 = None
        if os.path.exists(out_path):
            old_md5 = self._compute_hash(out_path)
            new_md5 = self._compute_hash(tmp)
            if old_md5 != new_md5:
                logging.info("  → update detected, replacing")
                shutil.copy2(out_path, out_path + ".backup")
                os.replace(tmp, out_path)
                updated = True
            else:
                logging.info("  → no update detected")
                os.remove(tmp)
        else:
            logging.info("  → initial download")
            os.replace(tmp, out_path)
            new_md5 = self._compute_hash(out_path)
            updated = True
        return updated, old_md5, new_md5

    def _make_diff(self, old_file, new_file):
        """
        Run 'diff -U0' to capture only changed hunks, write to timestamped .txt
        """
        if not os.path.exists(old_file):
            return None
        ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = os.path.splitext(self.diff_base)[0]
        txt  = f"{base}_{ts}.txt"
        cmd  = ["diff", "-U", "0", old_file, new_file]
        try:
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, check=False)
        except FileNotFoundError:
            logging.error("`diff` not found; skipping diff")
            return None
        if not result.stdout:
            logging.info("  → no differences found")
            return None
        with open(txt, "w") as out:
            out.write(result.stdout)
        logging.info(f"  → diff saved (zero-context) → {txt}")
        return txt

    def fetch_and_process_refseq(self):
        logging.info("=== REFSEQ NCBI-CREF (gene2refseq.gz) ===")
        upd, old, new = self._download_and_replace(self.refs["download_url"], self.refs["path"])
        # decompress to .decompressed
        bak = self.refs["decompressed"] + ".backup"
        if upd and os.path.exists(self.refs["decompressed"]):
            shutil.copy2(self.refs["decompressed"], bak)
        with gzip.open(self.refs["path"], "rb") as zin, open(self.refs["decompressed"], "wb") as zout:
            shutil.copyfileobj(zin, zout)
        if upd:
            self._make_diff(bak, self.refs["decompressed"])

        # filter human
        df = pd.read_csv(self.refs["decompressed"], sep="\t", dtype=str)
        before = len(df)
        logging.info(f"Filtering RefSeq for human (#tax_id==9606), initial rows: {before}")
        if "#tax_id" in df.columns:
            df = df[df["#tax_id"] == "9606"]
            after = len(df)
            dropped = before - after
            logging.info(f"  → human filter applied, kept {after} rows, dropped {dropped}")
            df.to_csv(self.refs["decompressed"], sep="\t", index=False)
        else:
            logging.warning("  → no '#tax_id' column found, skipping human filter")

        self.meta["downloads"].append({
            "name": "refseq",
            "updated": upd,
            "old_md5": old,
            "new_md5": new,
            "decompressed": self.refs["decompressed"]
        })

    def fetch_and_process_ensembl(self):
        logging.info("=== REFSEQ ENSEMBL-XREF (gene2ensembl.gz) ===")
        upd, old, new = self._download_and_replace(self.ensembl["url"], self.ensembl["path"])
        bak = self.ensembl["decompressed"] + ".backup"
        if upd and os.path.exists(self.ensembl["decompressed"]):
            shutil.copy2(self.ensembl["decompressed"], bak)

        raw = gzip.decompress(open(self.ensembl["path"], "rb").read()).decode("utf-8")
        df  = pd.read_csv(StringIO(raw), sep="\t", dtype=str)
        before = len(df)
        logging.info(f"Filtering Ensembl‐Xref for human (#tax_id==9606), initial rows: {before}")
        if "#tax_id" in df.columns:
            df = df[df["#tax_id"] == "9606"]
            after = len(df)
            dropped = before - after
            logging.info(f"  → human filter applied, kept {after} rows, dropped {dropped}")
        else:
            logging.warning("  → no '#tax_id' column found, skipping human filter")

        df.to_csv(self.ensembl["decompressed"], index=False)
        if upd:
            self._make_diff(bak, self.ensembl["decompressed"])

        self.meta["downloads"].append({
            "name": "ensembl",
            "updated": upd,
            "old_md5": old,
            "new_md5": new,
            "decompressed": self.ensembl["decompressed"]
        })

    def fetch_and_process_uniprot(self):
        logging.info("=== REFSEQ UNIPROT-XREF (gene_refseq_uniprotkb_collab.gz) ===")
        upd, old, new = self._download_and_replace(self.uniprot["url"], self.uniprot["path"])
        bak = self.uniprot["decompressed"] + ".backup"
        if upd and os.path.exists(self.uniprot["decompressed"]):
            shutil.copy2(self.uniprot["decompressed"], bak)

        total = 0
        rows, hdr = [], None
        with gzip.open(self.uniprot["path"], "rt") as zin:
            for line in zin:
                if line.startswith("#"):
                    hdr = line.strip().split("\t")
                    continue
                total += 1
                cols = line.strip().split("\t")
                if cols[2] == "9606":
                    rows.append(cols)
        logging.info(f"Parsed {total} UniProt‐collab rows, selecting {len(rows)} human entries")

        df = pd.DataFrame(rows, columns=hdr)
        df.to_csv(self.uniprot["decompressed"], index=False)
        logging.info(f"  → wrote filtered UniProt file ({len(rows)} rows)")

        if upd:
            self._make_diff(bak, self.uniprot["decompressed"])

        self.meta["downloads"].append({
            "name": "uniprot",
            "updated": upd,
            "old_md5": old,
            "new_md5": new,
            "decompressed": self.uniprot["decompressed"]
        })

    def run(self):
        self.fetch_and_process_refseq()
        self.fetch_and_process_ensembl()
        self.fetch_and_process_uniprot()
        self.meta["timestamp"]["end"] = datetime.now().isoformat()

        # write metadata next to diff_base
        meta_path = os.path.join(os.path.dirname(self.diff_base), "dl_refseq_metadata.json")
        with open(meta_path, "w") as f:
            json.dump(self.meta, f, indent=2)
        logging.info("All downloads complete.")

if __name__=="__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--config", type=str,
               default="config/targets_config.yaml",
               help="YAML config (default: config/targets_config.yaml)")

    args = p.parse_args()
    cfg = yaml.safe_load(open(args.config))
    RefSeqDownloader(cfg).run()
