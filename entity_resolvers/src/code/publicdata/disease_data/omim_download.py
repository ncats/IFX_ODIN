#!/usr/bin/env python
# omim_download.py - Download static OMIM files with metadata/hash tracking only

import os
import json
import yaml
import hashlib
import logging
import argparse
import requests
import email.utils
from pathlib import Path
from datetime import datetime

PUBLIC_MIM2GENE_URL = "https://omim.org/static/omim/data/mim2gene.txt"


def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file, mode="a"), logging.StreamHandler()],
        force=True
    )


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def head_metadata(url: str):
    out = {"last_modified": "unknown", "last_modified_iso": None, "etag": None, "content_length": None}
    try:
        r = requests.head(url, allow_redirects=True, timeout=30)
        r.raise_for_status()
        lm = r.headers.get("Last-Modified") or r.headers.get("last-modified")
        etag = r.headers.get("ETag") or r.headers.get("etag")
        clen = r.headers.get("Content-Length") or r.headers.get("content-length")
        if lm:
            out["last_modified"] = lm
            try:
                out["last_modified_iso"] = email.utils.parsedate_to_datetime(lm).isoformat()
            except Exception:
                pass
        out["etag"] = etag
        out["content_length"] = int(clen) if clen and str(clen).isdigit() else None
    except Exception as e:
        logging.warning(f"OMIM HEAD failed for {url}: {e}")
    return out


class OMIMDownloader:
    def __init__(self, config):
        self.cfg = config["omim"]
        self.output_dir = Path(self.cfg["raw_dir"])
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.api_key = os.environ.get("OMIM_API_KEY", "").strip()
        self.metadata_file = Path(self.cfg["dl_metadata_file"])
        self.metadata_file.parent.mkdir(parents=True, exist_ok=True)
        setup_logging(self.cfg["log_file"])
        self.urls = self.build_urls()

        self.old_meta = {}
        if self.metadata_file.exists():
            try:
                self.old_meta = yaml.safe_load(self.metadata_file.read_text()) or {}
            except Exception:
                pass

        self._old_file_meta = {}
        for f in self.old_meta.get("source", {}).get("files", []):
            label = f.get("file")
            if label:
                self._old_file_meta[label] = f

    def build_urls(self):
        urls = {"mim2gene.txt": PUBLIC_MIM2GENE_URL}
        if self.api_key:
            base = f"https://data.omim.org/downloads/{self.api_key}"
            urls["mimTitles.txt"] = f"{base}/mimTitles.txt"
            urls["genemap2.txt"] = f"{base}/genemap2.txt"
            urls["morbidmap.txt"] = f"{base}/morbidmap.txt"
        else:
            logging.warning("OMIM_API_KEY not set; will fetch only public mim2gene.txt")
        return urls

    def _should_skip(self, filename: str, dest: Path, remote_meta: dict) -> bool:
        if not dest.exists():
            return False

        old = self._old_file_meta.get(filename, {})
        old_lm = old.get("last_modified")
        old_etag = old.get("etag")
        old_clen = old.get("content_length")

        new_lm = remote_meta.get("last_modified")
        new_etag = remote_meta.get("etag")
        new_clen = remote_meta.get("content_length")

        if old_lm and new_lm and old_etag and new_etag:
            return old_lm == new_lm and old_etag == new_etag

        if old_lm and new_lm:
            if old_lm == new_lm:
                if old_clen is not None and new_clen is not None:
                    return old_clen == new_clen
                return True

        if old_etag and new_etag:
            return old_etag == new_etag

        return False

    def run(self):
        file_meta = []
        freshest_iso = None
        freshest_str = "unknown"

        for filename, url in self.urls.items():
            dest = self.output_dir / filename
            remote_meta = head_metadata(url)

            if self._should_skip(filename, dest, remote_meta):
                file_meta.append({
                    "file": filename,
                    "url": url,
                    "last_modified": remote_meta.get("last_modified"),
                    "last_modified_iso": remote_meta.get("last_modified_iso"),
                    "etag": remote_meta.get("etag"),
                    "content_length": remote_meta.get("content_length"),
                    "status": "skipped",
                    "sha256": self._old_file_meta.get(filename, {}).get("sha256"),
                })
                continue

            r = requests.get(url, timeout=(30, 180))
            r.raise_for_status()

            new_text = r.text
            new_hash = sha256_text(new_text)
            old_hash = self._old_file_meta.get(filename, {}).get("sha256")
            status = "new"

            if dest.exists():
                old_text = dest.read_text(encoding="utf-8", errors="replace")
                if old_hash == new_hash or old_text == new_text:
                    status = "skipped"
                else:
                    dest.write_text(new_text, encoding="utf-8")
                    status = "updated"
            else:
                dest.write_text(new_text, encoding="utf-8")

            file_meta.append({
                "file": filename,
                "url": url,
                "last_modified": remote_meta.get("last_modified"),
                "last_modified_iso": remote_meta.get("last_modified_iso"),
                "etag": remote_meta.get("etag"),
                "content_length": remote_meta.get("content_length"),
                "status": status,
                "sha256": sha256_text(dest.read_text(encoding="utf-8", errors="replace")) if dest.exists() else None,
            })

            if remote_meta.get("last_modified_iso"):
                if freshest_iso is None or remote_meta["last_modified_iso"] > freshest_iso:
                    freshest_iso = remote_meta["last_modified_iso"]
                    freshest_str = remote_meta["last_modified"]

        meta = {
            "timestamp": datetime.now().isoformat(),
            "output_dir": str(self.output_dir),
            "files": list(self.urls.keys()),
            "source": {
                "name": "OMIM",
                "version": freshest_str,
                "files": file_meta
            }
        }
        self.metadata_file.write_text(yaml.dump(meta, sort_keys=False))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    full_config = yaml.safe_load(open(args.config))
    OMIMDownloader(full_config).run()