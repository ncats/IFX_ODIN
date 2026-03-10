#!/usr/bin/env python
# medgen_download.py - Modular downloader for MedGen data with diff tracking + version

import os
import json
import yaml
import gzip
import shutil
import logging
import pandas as pd
import difflib
import requests
import email.utils
from datetime import datetime
from pathlib import Path
import argparse

def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode="a"),
            logging.StreamHandler()
        ],
        force=True
    )

def head_last_modified(url: str):
    """Return (YYYY-MM-DD, datetime|None)."""
    try:
        r = requests.head(url, allow_redirects=True, timeout=30)
        lm = r.headers.get("Last-Modified") or r.headers.get("last-modified")
        if lm:
            dt = email.utils.parsedate_to_datetime(lm)
            return dt.strftime("%Y-%m-%d"), dt
    except Exception as e:
        logging.warning(f"MedGen HEAD failed for {url}: {e}")
    return "unknown", None

class MedGenDownloader:
    def __init__(self, full_config):
        self.cfg = full_config["medgen"]
        self.qc_mode = full_config.get("global", {}).get("qc_mode", False)

        setup_logging(self.cfg["log_file"])
        self.metadata_path = Path(self.cfg["dl_metadata_file"])
        os.makedirs(self.metadata_path.parent, exist_ok=True)
        self.old_meta = {}
        if self.metadata_path.exists():
            try:
                self.old_meta = json.loads(self.metadata_path.read_text())
            except Exception:
                pass
        # Build old per-file Last-Modified lookup
        self._old_versions = {}
        for f in self.old_meta.get("files", []):
            if f.get("label") and f.get("last_modified"):
                self._old_versions[f["label"]] = f["last_modified"]

    def compute_diff(self, old_file, new_file):
        base = Path(old_file).stem
        diff_txt = Path(old_file).with_name(f"{base}.diff.txt")
        backup = Path(old_file).with_name(f"{base}.backup")
        try:
            # Auto-detect delimiter: pipe-delimited files (e.g. MedGenIDMappings) vs tab
            with open(old_file, "r", encoding="utf-8", errors="ignore") as fh:
                sample = fh.read(2048)
            sep = "|" if sample.count("|") > sample.count("\t") else "\t"

            old_df = pd.read_csv(old_file, sep=sep, dtype=str, comment='#').fillna("")
            new_df = pd.read_csv(new_file, sep=sep, dtype=str, comment='#').fillna("")
            key_col = old_df.columns[0]
            old_ids = set(old_df[key_col]); new_ids = set(new_df[key_col])
            added = sorted(new_ids - old_ids)
            removed = sorted(old_ids - new_ids)
            common = sorted(old_ids & new_ids)   # list, not set — required for .loc
            old_sub = old_df.set_index(key_col).loc[common]
            new_sub = new_df.set_index(key_col).loc[common]
            updated = [idx for idx in common if not old_sub.loc[idx].equals(new_sub.loc[idx])]
            summary = [
                f"🔄 DIFF SUMMARY for {base}",
                f"➕ Added IDs: {len(added)}",
                f"➖ Removed IDs: {len(removed)}",
                f"✏️  Updated IDs: {len(updated)}",
                "", f"Sample Added: {added[:3]}", f"Sample Removed: {removed[:3]}", f"Sample Updated: {updated[:3]}",
            ]
            with open(diff_txt, "w") as f: f.write("\n".join(summary))
            logging.info("\n".join(summary))
            return str(diff_txt), None, str(backup)
        except Exception as e:
            logging.warning(f"⚠️ Diff summary generation failed: {e}")
            return None, None, None

    def download_and_extract(self, url, destination):
        local_gz = Path(destination + ".gz")
        local_txt = Path(destination)
        tmp_txt = Path(destination + ".tmp")
        backup_file = None; diff_txt = None; status = "new"
        try:
            logging.info(f"⬇️ Downloading: {url}")
            with requests.get(url, stream=True, timeout=120) as response:
                response.raise_for_status()
                with open(local_gz, "wb") as f:
                    for chunk in response.iter_content(chunk_size=1024 * 256):
                        if chunk: f.write(chunk)
            logging.info(f"📦 Extracting to temp file: {tmp_txt}")
            with gzip.open(local_gz, 'rt', encoding='utf-8') as gz_file, open(tmp_txt, 'w', encoding='utf-8') as out_file:
                for line in gz_file:
                    if not line.startswith("#"):
                        out_file.write(line)
            os.remove(local_gz)

            if local_txt.exists():
                with open(local_txt, "rb") as f1, open(tmp_txt, "rb") as f2:
                    if f1.read() == f2.read():
                        os.remove(tmp_txt)
                        logging.info(f"✅ No change: {local_txt}")
                        return str(local_txt), "skipped", None, None
                backup_file = local_txt.with_name(f"{local_txt.stem}.backup")
                shutil.copy2(local_txt, backup_file)
                diff_txt, _, _ = self.compute_diff(backup_file, tmp_txt)
                os.replace(tmp_txt, local_txt)
                status = "updated"
            else:
                os.replace(tmp_txt, local_txt)
        finally:
            if not self.qc_mode:
                for f in [str(backup_file) if backup_file else None, diff_txt]:
                    if f and os.path.exists(f): os.remove(f)
            if tmp_txt.exists(): os.remove(tmp_txt)
        return str(local_txt), status, diff_txt, None

    def run(self):
        metadata = {"timestamp": datetime.now().isoformat(), "files": []}
        freshest_dt = None; freshest_str = "unknown"
        perfile_versions = []

        # Iterate file sections under medgen
        for key, entry in self.cfg.items():
            if key in ["dl_metadata_file", "transform_metadata", "log_file"]:
                continue
            if not isinstance(entry, dict) or "url" not in entry:
                continue

            url = entry["url"]; out_path = entry["local_txt"]
            vstr, vdt = head_last_modified(url)
            perfile_versions.append({"label": key, "url": url, "last_modified": vstr})
            if vdt and (freshest_dt is None or vdt > freshest_dt):
                freshest_dt, freshest_str = vdt, vstr

            # Skip if Last-Modified unchanged and local file exists
            old_ver = self._old_versions.get(key)
            if old_ver and old_ver == vstr and os.path.exists(out_path):
                logging.info(f"Skipping {key} — file unchanged (Last-Modified: {vstr})")
                metadata["files"].append({
                    "label": key, "url": url, "path": out_path,
                    "status": "skipped", "last_modified": vstr,
                })
                continue

            try:
                extracted_file, status, diff_txt, diff_html = self.download_and_extract(url, out_path)
                metadata["files"].append({
                    "label": key, "url": url, "path": extracted_file,
                    "status": status, "diff_txt": diff_txt, "diff_html": diff_html,
                    "last_modified": vstr,
                })
            except Exception as e:
                logging.error(f"❌ Failed: {url} → {e}")
                metadata["files"].append({
                    "label": key, "url": url, "path": out_path,
                    "status": "error", "error": str(e), "last_modified": vstr
                })

        metadata["source"] = {
            "name": "MedGen",
            "version": freshest_str,
            "files": perfile_versions
        }

        with open(self.metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)
        logging.info(f"📄 Metadata written → {self.metadata_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()
    with open(args.config) as f:
        cfg = yaml.safe_load(f)
    MedGenDownloader(cfg).run()