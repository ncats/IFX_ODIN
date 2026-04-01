#!/usr/bin/env python
"""
refseq_download.py - Download RefSeq / Ensembl / UniProt collaborations,
with update detection, human filtering, and logging.

NO raw-file diffs — version tracking happens on cleaned output in the transformer.
"""

import os
import gzip
import json
import hashlib
import logging
import argparse
import shutil
import subprocess
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from requests.exceptions import RequestException, SSLError
import yaml
from publicdata.target_data.download_utils import retry_request, setup_logging


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

    def _old_download_meta(self, name):
        for item in self.old_meta.get("downloads", []):
            if item.get("name") == name:
                return item
        return {}

    def _is_valid_table(self, path, required_columns, sep=",", require_tax_id=None):
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return False
        try:
            df = pd.read_csv(path, nrows=25, sep=sep, dtype=str)
            if not all(col in df.columns for col in required_columns):
                return False
            if require_tax_id is not None:
                tax_col = next((c for c in ("#tax_id", "tax_id", "NCBI_tax_id") if c in df.columns), None)
                if tax_col is None:
                    return False
                observed = set(df[tax_col].dropna().astype(str).str.strip())
                if observed and observed != {str(require_tax_id)}:
                    return False
            return True
        except Exception:
            return False

    def _all_decompressed_exist(self):
        return all([
            self._is_valid_table(
                self.refs["decompressed"],
                ["#tax_id", "GeneID", "RNA_nucleotide_accession.version"],
                sep="\t",
                require_tax_id="9606",
            ),
            self._is_valid_table(
                self.ensembl["decompressed"],
                ["#tax_id", "GeneID", "Ensembl_gene_identifier"],
                sep="\t",
                require_tax_id="9606",
            ),
            self._is_valid_table(
                self.uniprot["decompressed"],
                ["#NCBI_protein_accession", "UniProtKB_protein_accession", "NCBI_tax_id"],
                sep=",",
                require_tax_id="9606",
            ),
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
            r = retry_request("HEAD", url, timeout=(10, 10))
            last_mod = r.headers.get("Last-Modified")
            version = "unknown"
            if last_mod:
                dt = email.utils.parsedate_to_datetime(last_mod)
                version = dt.strftime("%Y-%m-%d")
            return version, {
                "Last-Modified": last_mod,
                "ETag": r.headers.get("ETag"),
                "Content-Length": r.headers.get("Content-Length"),
            }
        except Exception as e:
            logging.warning(f"HEAD failed for {url}: {e}")
            return "unknown", {}

    def _source_unchanged(self, name, url, out_path):
        version, headers = self._headers_and_version(url)
        old = self._old_download_meta(name)
        if not old:
            return False, version, headers

        old_version = old.get("version")
        if version != "unknown" and old_version == version and os.path.exists(out_path):
            return True, version, headers

        if os.path.exists(out_path) and headers:
            if headers.get("ETag") and headers["ETag"] == old.get("ETag"):
                return True, version, headers
            if headers.get("Last-Modified") and headers["Last-Modified"] == old.get("Last-Modified"):
                return True, version, headers

        return False, version, headers

    def _download_and_replace(self, url, out_path, max_attempts=3):
        tmp = out_path + ".tmp"
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        last_exc = None
        for attempt in range(1, max_attempts + 1):
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
                logging.info(f"Downloading {url} (attempt {attempt}/{max_attempts})")
                with retry_request("GET", url, stream=True, timeout=(30, None)) as r:
                    total = int(r.headers.get("content-length", 0))
                    with tqdm(total=total, unit="iB", unit_scale=True, desc=os.path.basename(out_path)) as bar:
                        with open(tmp, "wb") as f:
                            for ch in r.iter_content(1024 * 1024):
                                if not ch:
                                    continue
                                f.write(ch)
                                bar.update(len(ch))
                break
            except (RequestException, SSLError, OSError) as exc:
                last_exc = exc
                logging.warning(
                    "Download failed for %s on attempt %d/%d: %s",
                    url, attempt, max_attempts, exc,
                )
                curl_ok = self._download_with_curl(url, tmp, attempt)
                if curl_ok:
                    break
                if os.path.exists(tmp):
                    os.remove(tmp)
                if attempt == max_attempts:
                    raise
        else:
            raise last_exc

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

    def _download_with_curl(self, url, tmp_path, attempt):
        curl_path = shutil.which("curl")
        if not curl_path:
            return False

        logging.info(
            "Falling back to curl for %s after Python stream failure (attempt %d)",
            url, attempt,
        )
        cmd = [
            curl_path,
            "-L",
            "--fail",
            "--retry", "5",
            "--retry-all-errors",
            "--continue-at", "-",
            "--output", tmp_path,
            url,
        ]
        try:
            subprocess.run(cmd, check=True)
            return True
        except subprocess.CalledProcessError as exc:
            logging.warning("curl fallback failed for %s: %s", url, exc)
            return False

    def fetch_and_process_refseq(self):
        logging.info("=== REFSEQ gene2refseq.gz ===")
        version, hdrs = self._headers_and_version(self.refs["download_url"])
        upd, old, new = self._download_and_replace(self.refs["download_url"], self.refs["path"])

        os.makedirs(os.path.dirname(self.refs["decompressed"]), exist_ok=True)
        total_rows = 0
        human_rows = 0
        with gzip.open(self.refs["path"], "rt", encoding="utf-8", errors="ignore") as zin, \
                open(self.refs["decompressed"], "w", encoding="utf-8", newline="") as zout:
            header = zin.readline()
            if not header:
                raise RuntimeError(f"RefSeq source file is empty: {self.refs['path']}")
            zout.write(header)
            for line in zin:
                total_rows += 1
                if line.startswith("9606\t"):
                    zout.write(line)
                    human_rows += 1
        logging.info("Human filter: %d → %d rows", total_rows, human_rows)

        self.meta["downloads"].append({
            "name": "gene2refseq", "url": self.refs["download_url"],
            "version": version, "updated": upd, "old_hash": old, "new_hash": new,
            **hdrs,
            "rows_total": total_rows, "rows_human": human_rows,
        })

    def fetch_and_process_ensembl(self):
        logging.info("=== REFSEQ gene2ensembl.gz ===")
        version, hdrs = self._headers_and_version(self.ensembl["url"])
        upd, old, new = self._download_and_replace(self.ensembl["url"], self.ensembl["path"])

        os.makedirs(os.path.dirname(self.ensembl["decompressed"]), exist_ok=True)
        total_rows = 0
        human_rows = 0
        with gzip.open(self.ensembl["path"], "rt", encoding="utf-8", errors="ignore") as zin, \
                open(self.ensembl["decompressed"], "w", encoding="utf-8", newline="") as zout:
            header = zin.readline()
            if not header:
                raise RuntimeError(f"RefSeq Ensembl source file is empty: {self.ensembl['path']}")
            zout.write(header)
            for line in zin:
                total_rows += 1
                if line.startswith("9606\t"):
                    zout.write(line)
                    human_rows += 1
        logging.info("Human filter: %d → %d rows", total_rows, human_rows)

        self.meta["downloads"].append({
            "name": "gene2ensembl", "url": self.ensembl["url"],
            "version": version, "updated": upd, "old_hash": old, "new_hash": new,
            **hdrs,
            "rows_total": total_rows, "rows_human": human_rows,
        })

    def fetch_and_process_uniprot(self):
        logging.info("=== REFSEQ gene_refseq_uniprotkb_collab.gz ===")
        version, hdrs = self._headers_and_version(self.uniprot["url"])
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

        self.meta["downloads"].append({
            "name": "gene_refseq_uniprotkb", "url": self.uniprot["url"],
            "version": version, "updated": upd, "old_hash": old, "new_hash": new,
            **hdrs,
            "rows_total": total, "rows_human": len(rows),
        })

    def run(self):
        # Version-based skip: check all 3 source files independently
        refseq_same, refseq_version, _ = self._source_unchanged(
            "gene2refseq", self.refs["download_url"], self.refs["path"]
        )
        ensembl_same, ensembl_version, _ = self._source_unchanged(
            "gene2ensembl", self.ensembl["url"], self.ensembl["path"]
        )
        uniprot_same, uniprot_version, _ = self._source_unchanged(
            "gene_refseq_uniprotkb", self.uniprot["url"], self.uniprot["path"]
        )
        if refseq_same and ensembl_same and uniprot_same and self._all_decompressed_exist():
            current_versions = [v for v in (refseq_version, ensembl_version, uniprot_version) if v != "unknown"]
            logging.info(
                "RefSeq sources unchanged and all files present — skipping download."
            )
            self.meta["download_end"] = datetime.now().isoformat()
            self.meta["downloads"] = self.old_meta.get("downloads", [])
            self.meta["source_version"] = max(current_versions) if current_versions else self.old_meta.get("source_version", "unknown")
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
        return True


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config/targets_config.yaml")
    args = p.parse_args()
    cfg = yaml.safe_load(open(args.config))
    RefSeqDownloader(cfg).run()