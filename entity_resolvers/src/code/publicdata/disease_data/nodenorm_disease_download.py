#!/usr/bin/env python3
"""
nodenorm_disease_download.py — Download NodeNorm Disease compendium (Disease.txt)
from the configured "latest/compendia" URL, with robust retry + resume,
metadata capture, and optional unified diff vs previous raw file.

YAML (example):
nodenorm:
  url_base: https://stars.renci.org/var/babel_outputs/latest/compendia/
  raw_file: src/data/publicdata/disease_data/raw/nodenorm_disease.txt
  diff_file: src/data/publicdata/disease_data/metadata/nodenorm_disease.diff.txt
  dl_metadata_file: src/data/publicdata/disease_data/metadata/nodenorm_disease_dl_metadata.json
  log_file: src/data/publicdata/disease_data/metadata/nodenorm_disease.log
"""

from __future__ import annotations

import argparse
import difflib
import email.utils
import json
import logging
import os
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Optional

import requests
import yaml


def setup_logging(log_file: str) -> None:
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


def head_info(url: str) -> Dict[str, Any]:
    """
    Best-effort HEAD to get Last-Modified and Content-Length.
    """
    try:
        h = requests.head(url, allow_redirects=True, timeout=30)
        lm = h.headers.get("Last-Modified") or h.headers.get("last-modified")
        lm_str = "unknown"
        if lm:
            try:
                lm_dt = email.utils.parsedate_to_datetime(lm)
                lm_str = lm_dt.strftime("%Y-%m-%d")
            except Exception:
                lm_str = str(lm)
        return {
            "Last-Modified": lm_str,
            "Content-Length": h.headers.get("Content-Length"),
        }
    except Exception as e:
        logging.warning(f"HEAD failed for {url}: {e}")
        return {"Last-Modified": "unknown", "Content-Length": None}


class NodeNormDiseaseDownloader:
    def __init__(self, cfg: Dict[str, Any]) -> None:
        c = cfg["nodenorm"]
        setup_logging(c["log_file"])

        self.url_base: str = c["url_base"]
        self.raw_file: str = c["raw_file"]
        self.diff_base: str = c.get("diff_file", "")
        self.meta_file: str = c["dl_metadata_file"]

        # load old metadata (optional)
        if os.path.exists(self.meta_file):
            try:
                with open(self.meta_file, "r", encoding="utf-8") as f:
                    self.old_meta = json.load(f)
            except Exception:
                self.old_meta = {"files": {}}
        else:
            self.old_meta = {"files": {}}

        self.new_meta: Dict[str, Any] = {
            "downloaded_at": datetime.now().isoformat(),
            "files": {},
            "raw_file": self.raw_file,
            "diff_txt": None,
            "source": None,
        }

    def _download_resumable(
        self,
        url: str,
        dst: str,
        chunk_size: int = 1 << 20,
        max_retries: int = 8,
    ) -> None:
        """
        Download with retry + resume using HTTP Range and a .part file.
        """
        os.makedirs(os.path.dirname(os.path.abspath(dst)) or ".", exist_ok=True)
        part = dst + ".part"

        sess = requests.Session()
        sess.headers.update({"User-Agent": "TargetGraph/NodeNormDiseaseDownloader"})

        for attempt in range(1, max_retries + 1):
            resume_from = os.path.getsize(part) if os.path.exists(part) else 0
            headers = {"Range": f"bytes={resume_from}-"} if resume_from > 0 else {}

            try:
                with sess.get(url, stream=True, timeout=(30, 300), headers=headers) as r:
                    # If we tried to resume but server ignored Range and returned full content,
                    # restart from scratch.
                    if resume_from > 0 and r.status_code == 200:
                        logging.warning("Server ignored Range resume; restarting download from scratch.")
                        try:
                            os.remove(part)
                        except FileNotFoundError:
                            pass
                        resume_from = 0

                    # If partial already complete
                    if r.status_code == 416:
                        os.replace(part, dst)
                        print()
                        return

                    r.raise_for_status()

                    total = r.headers.get("Content-Length")
                    total = int(total) if total and str(total).isdigit() else None

                    mode = "ab" if resume_from > 0 else "wb"
                    seen = resume_from

                    with open(part, mode) as f:
                        for chunk in r.iter_content(chunk_size=chunk_size):
                            if not chunk:
                                continue
                            f.write(chunk)
                            seen += len(chunk)

                            if total is not None:
                                denom = resume_from + total
                                pct = (seen / denom * 100) if denom else 0.0
                                print(f"\r{os.path.basename(dst)}: {pct:.1f}% ", end="", flush=True)
                            else:
                                print(f"\r{os.path.basename(dst)}: {seen/1e6:.1f}MB ", end="", flush=True)

                os.replace(part, dst)
                print()
                return

            except (
                requests.exceptions.SSLError,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout,
            ) as e:
                logging.warning(f"Download error (attempt {attempt}/{max_retries}) for {url}: {e}")
                time.sleep(min(60, 5 * attempt))

        raise RuntimeError(f"Failed to download {url} after {max_retries} attempts.")

    def _write_metadata(self, url: str, version_string: str, last_modified: str) -> None:
        self.new_meta["source"] = {
            "name": "NodeNorm (Disease)",
            "version": version_string,
            "files": [
                {
                    "label": "Disease.txt",
                    "url": url,
                    "last_modified": last_modified,
                }
            ],
        }
        os.makedirs(os.path.dirname(self.meta_file), exist_ok=True)
        with open(self.meta_file, "w", encoding="utf-8") as m:
            json.dump(self.new_meta, m, indent=2)
        logging.info(f"Metadata saved to {self.meta_file}")

    def run(self) -> None:
        base = self.url_base.rstrip("/")
        url = base if base.endswith("Disease.txt") else f"{base}/Disease.txt"
        logging.info(f"Using NodeNorm Disease URL: {url}")

        hdr = head_info(url)
        last_modified = hdr["Last-Modified"]
        version_string = last_modified if last_modified != "unknown" else "latest"

        fname = os.path.basename(url)
        self.new_meta["files"][fname] = {
            "Last-Modified": last_modified,
            "Content-Length": hdr.get("Content-Length"),
        }

        # Skip unchanged if last-modified is stable and matches prior metadata
        old_hdr = (self.old_meta.get("files") or {}).get(fname, {})
        # Also try old URL-keyed entries for backwards compatibility
        if not old_hdr:
            old_hdr = (self.old_meta.get("files") or {}).get(url, {})
        if last_modified != "unknown" and last_modified == old_hdr.get("Last-Modified"):
            logging.info(f"Skipping unchanged {fname}")
            self._write_metadata(url=url, version_string=version_string, last_modified=last_modified)
            return

        # Fallback: compare local file size against server Content-Length
        server_size = hdr.get("Content-Length")
        if server_size and os.path.exists(self.raw_file):
            local_size = str(os.path.getsize(self.raw_file))
            if local_size == server_size:
                logging.info(f"Skipping {fname} — local file matches server size ({server_size} bytes)")
                self._write_metadata(url=url, version_string=version_string, last_modified=last_modified)
                return

        # Backup BEFORE download so we can diff
        os.makedirs(os.path.dirname(self.raw_file), exist_ok=True)
        prev: Optional[str] = None
        if os.path.exists(self.raw_file):
            prev = self.raw_file + ".backup"
            os.replace(self.raw_file, prev)

        logging.info(f"Downloading {url}")
        self._download_resumable(url, self.raw_file)
        logging.info(f"Wrote raw file to {self.raw_file}")

        # Diff
        if prev and self.diff_base:
            try:
                old_lines = open(prev, "r", encoding="utf-8", errors="ignore").readlines()
                new_lines = open(self.raw_file, "r", encoding="utf-8", errors="ignore").readlines()
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                diff_txt = f"{os.path.splitext(self.diff_base)[0]}_{ts}.txt"
                os.makedirs(os.path.dirname(diff_txt), exist_ok=True)
                with open(diff_txt, "w", encoding="utf-8") as d:
                    d.write("".join(difflib.unified_diff(old_lines, new_lines)))
                logging.info(f"Diff written to {diff_txt}")
                self.new_meta["diff_txt"] = diff_txt
            except Exception as e:
                logging.warning(f"Diff failed (continuing): {e}")

        self._write_metadata(url=url, version_string=version_string, last_modified=last_modified)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config/diseases/diseases_config.yaml")
    args = p.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    NodeNormDiseaseDownloader(cfg).run()


if __name__ == "__main__":
    main()
