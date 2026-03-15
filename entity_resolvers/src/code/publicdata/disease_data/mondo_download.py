#!/usr/bin/env python
# mondo_download.py - Download MONDO ontology with metadata/hash tracking only

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
        handlers=[
            logging.FileHandler(log_file, mode="a"),
            logging.StreamHandler()
        ],
        force=True
    )


def sha256sum(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def head_metadata(url: str):
    out = {
        "last_modified": "unknown",
        "last_modified_iso": None,
        "etag": None,
        "content_length": None,
    }
    try:
        r = requests.head(url, allow_redirects=True, timeout=30)
        r.raise_for_status()

        lm = r.headers.get("Last-Modified") or r.headers.get("last-modified")
        etag = r.headers.get("ETag") or r.headers.get("etag")
        clen = r.headers.get("Content-Length") or r.headers.get("content-length")

        if lm:
            out["last_modified"] = lm
            try:
                dt = email.utils.parsedate_to_datetime(lm)
                out["last_modified_iso"] = dt.isoformat()
            except Exception:
                out["last_modified_iso"] = None

        out["etag"] = etag
        out["content_length"] = int(clen) if clen and str(clen).isdigit() else None

    except Exception as e:
        logging.warning(f"MONDO HEAD failed for {url}: {e}")

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


def download_stream_to_temp(url: str, temp_path: Path):
    response = requests.get(url, stream=True, timeout=(30, 180))
    response.raise_for_status()

    total_bytes = int(response.headers.get("Content-Length", 0))
    written = 0

    with open(temp_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if not chunk:
                continue
            f.write(chunk)
            written += len(chunk)

            if total_bytes > 0 and written % (25 * 1024 * 1024) < len(chunk):
                pct = written / total_bytes * 100
                logging.info(f"  Downloaded {written:,}/{total_bytes:,} bytes ({pct:.1f}%)")
            elif total_bytes == 0 and written % (50 * 1024 * 1024) < len(chunk):
                logging.info(f"  Downloaded {written:,} bytes...")


class MondoDownloader:
    def __init__(self, full_config):
        self.cfg = full_config["mondo"]
        setup_logging(self.cfg["log_file"])

        self.download_url = self.cfg["download_url"]
        self.owl_file = Path(self.cfg["raw_file"])
        self.metadata_file = Path(self.cfg["dl_metadata_file"])

        os.makedirs(self.owl_file.parent, exist_ok=True)
        os.makedirs(self.metadata_file.parent, exist_ok=True)

    def _load_old_metadata(self):
        if not self.metadata_file.exists():
            return {}
        try:
            with open(self.metadata_file) as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"Could not read old metadata: {e}")
            return {}

    def download(self):
        remote_meta = head_metadata(self.download_url)
        old_meta = self._load_old_metadata()

        logging.info("🌐 Remote MONDO metadata:")
        logging.info(f"  Last-Modified: {remote_meta.get('last_modified')}")
        logging.info(f"  ETag:          {remote_meta.get('etag')}")
        logging.info(f"  Content-Length:{remote_meta.get('content_length')}")

        if should_skip_download(self.owl_file, old_meta, remote_meta):
            logging.info("✅ Skipping MONDO download — remote metadata unchanged.")
            meta = {
                **old_meta,
                "timestamp": datetime.now().isoformat(),
                "status": "skipped",
                "last_modified": remote_meta.get("last_modified"),
                "last_modified_iso": remote_meta.get("last_modified_iso"),
                "etag": remote_meta.get("etag"),
                "content_length": remote_meta.get("content_length"),
            }
            with open(self.metadata_file, "w") as f:
                json.dump(meta, f, indent=2)
            return

        temp_path = self.owl_file.with_suffix(self.owl_file.suffix + ".tmp")

        logging.info(f"⬇️  Downloading MONDO from {self.download_url}")
        download_stream_to_temp(self.download_url, temp_path)
        logging.info(f"✅ Temp MONDO download saved to {temp_path}")

        logging.info("🔍 Computing hash for temp download...")
        new_hash = sha256sum(temp_path)
        logging.info("✅ Temp hash complete")

        old_hash = old_meta.get("sha256")
        changed = True

        if self.owl_file.exists() and old_hash == new_hash:
            changed = False
            logging.info("✅ MONDO content hash unchanged after download.")
            temp_path.unlink(missing_ok=True)
        else:
            logging.info("🔄 Replacing old MONDO file...")
            os.replace(temp_path, self.owl_file)
            logging.info(f"✅ MONDO raw file updated → {self.owl_file}")

        final_hash = old_hash if not changed else sha256sum(self.owl_file)

        meta = {
            "timestamp": datetime.now().isoformat(),
            "download_url": self.download_url,
            "raw_file": str(self.owl_file),
            "last_modified": remote_meta.get("last_modified"),
            "last_modified_iso": remote_meta.get("last_modified_iso"),
            "etag": remote_meta.get("etag"),
            "content_length": remote_meta.get("content_length"),
            "sha256": final_hash,
            "changed": changed,
        }
        with open(self.metadata_file, "w") as f:
            json.dump(meta, f, indent=2)
        logging.info(f"📝 Metadata saved → {self.metadata_file}")

    def run(self):
        self.download()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download MONDO ontology")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    MondoDownloader(cfg).run()