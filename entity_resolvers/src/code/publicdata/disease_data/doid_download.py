#!/usr/bin/env python
# doid_download.py - Download DOID OBO with metadata/hash tracking only

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


def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file, mode="a"), logging.StreamHandler()],
        force=True
    )


def sha256sum(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


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
        logging.warning(f"DOID HEAD failed for {url}: {e}")
    return out


def should_skip_download(dest: Path, old_meta: dict, remote_meta: dict) -> bool:
    if not dest.exists():
        return False
    old_lm = old_meta.get("last_modified")
    old_etag = old_meta.get("etag")
    old_clen = old_meta.get("content_length")
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


class DOIDDownloader:
    def __init__(self, full_config):
        self.cfg = full_config["doid"]
        setup_logging(self.cfg["log_file"])
        self.download_url = self.cfg["download_url"]
        self.raw_file = Path(self.cfg["raw_file"])
        self.metadata_file = Path(self.cfg["dl_metadata_file"])
        self.raw_file.parent.mkdir(parents=True, exist_ok=True)
        self.metadata_file.parent.mkdir(parents=True, exist_ok=True)

    def _old_meta(self):
        if not self.metadata_file.exists():
            return {}
        try:
            return json.load(open(self.metadata_file))
        except Exception:
            return {}

    def run(self):
        remote_meta = head_metadata(self.download_url)
        old_meta = self._old_meta()

        if should_skip_download(self.raw_file, old_meta, remote_meta):
            logging.info("✅ Skipping DOID download — remote metadata unchanged.")
            meta = {
                **old_meta,
                "timestamp": datetime.now().isoformat(),
                "status": "skipped",
                "last_modified": remote_meta.get("last_modified"),
                "last_modified_iso": remote_meta.get("last_modified_iso"),
                "etag": remote_meta.get("etag"),
                "content_length": remote_meta.get("content_length"),
            }
            json.dump(meta, open(self.metadata_file, "w"), indent=2)
            return

        tmp = self.raw_file.with_suffix(".tmp")
        r = requests.get(self.download_url, stream=True, timeout=(30, 180))
        r.raise_for_status()

        with open(tmp, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)

        new_hash = sha256sum(tmp)
        old_hash = old_meta.get("sha256")
        changed = True

        if self.raw_file.exists() and new_hash == old_hash:
            changed = False
            tmp.unlink(missing_ok=True)
        else:
            os.replace(tmp, self.raw_file)

        meta = {
            "timestamp": datetime.now().isoformat(),
            "download_url": self.download_url,
            "raw_file": str(self.raw_file),
            "last_modified": remote_meta.get("last_modified"),
            "last_modified_iso": remote_meta.get("last_modified_iso"),
            "etag": remote_meta.get("etag"),
            "content_length": remote_meta.get("content_length"),
            "sha256": old_hash if not changed else sha256sum(self.raw_file),
            "changed": changed,
        }
        json.dump(meta, open(self.metadata_file, "w"), indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    cfg = yaml.safe_load(open(args.config))
    DOIDDownloader(cfg).run()