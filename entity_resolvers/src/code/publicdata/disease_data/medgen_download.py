#!/usr/bin/env python
# medgen_download.py - Download MedGen files with metadata/hash tracking only

import os
import json
import yaml
import gzip
import logging
import requests
import email.utils
import hashlib
from datetime import datetime
from pathlib import Path
import argparse


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
        logging.warning(f"MedGen HEAD failed for {url}: {e}")
    return out


class MedGenDownloader:
    def __init__(self, full_config):
        self.cfg = full_config["medgen"]
        setup_logging(self.cfg["log_file"])
        self.metadata_path = Path(self.cfg["dl_metadata_file"])
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)

        self.old_meta = {}
        if self.metadata_path.exists():
            try:
                self.old_meta = json.loads(self.metadata_path.read_text())
            except Exception:
                pass

        self._old = {}
        for f in self.old_meta.get("files", []):
            label = f.get("label")
            if label:
                self._old[label] = f

    def _should_skip(self, label, out_path, remote_meta):
        if not os.path.exists(out_path):
            return False

        old = self._old.get(label, {})
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

    def _download_and_extract(self, url, destination, label):
        local_gz = Path(destination + ".gz")
        local_txt = Path(destination)
        tmp_txt = Path(destination + ".tmp")

        with requests.get(url, stream=True, timeout=(30, 180)) as response:
            response.raise_for_status()
            with open(local_gz, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        with gzip.open(local_gz, "rt", encoding="utf-8") as gz_file, open(tmp_txt, "w", encoding="utf-8") as out_file:
            for line in gz_file:
                if not line.startswith("#"):
                    out_file.write(line)
        os.remove(local_gz)

        new_hash = sha256sum(tmp_txt)
        old_hash = self._old.get(label, {}).get("sha256")
        status = "new"

        if local_txt.exists() and old_hash == new_hash:
            tmp_txt.unlink(missing_ok=True)
            return str(local_txt), "skipped", new_hash

        if local_txt.exists():
            status = "updated"

        os.replace(tmp_txt, local_txt)
        return str(local_txt), status, sha256sum(local_txt)

    def run(self):
        metadata = {"timestamp": datetime.now().isoformat(), "files": []}
        freshest_iso = None
        freshest_str = "unknown"
        perfile_versions = []

        for key, entry in self.cfg.items():
            if key in ["dl_metadata_file", "transform_metadata", "log_file"]:
                continue
            if not isinstance(entry, dict) or "url" not in entry:
                continue

            url = entry["url"]
            out_path = entry["local_txt"]

            remote_meta = head_metadata(url)
            perfile_versions.append({
                "label": key,
                "url": url,
                "last_modified": remote_meta.get("last_modified"),
                "last_modified_iso": remote_meta.get("last_modified_iso"),
                "etag": remote_meta.get("etag"),
                "content_length": remote_meta.get("content_length"),
            })

            if remote_meta.get("last_modified_iso"):
                if freshest_iso is None or remote_meta["last_modified_iso"] > freshest_iso:
                    freshest_iso = remote_meta["last_modified_iso"]
                    freshest_str = remote_meta["last_modified"]

            if self._should_skip(key, out_path, remote_meta):
                metadata["files"].append({
                    "label": key,
                    "url": url,
                    "path": out_path,
                    "status": "skipped",
                    "last_modified": remote_meta.get("last_modified"),
                    "last_modified_iso": remote_meta.get("last_modified_iso"),
                    "etag": remote_meta.get("etag"),
                    "content_length": remote_meta.get("content_length"),
                    "sha256": self._old.get(key, {}).get("sha256"),
                })
                continue

            try:
                extracted_file, status, sha = self._download_and_extract(url, out_path, key)
                metadata["files"].append({
                    "label": key,
                    "url": url,
                    "path": extracted_file,
                    "status": status,
                    "last_modified": remote_meta.get("last_modified"),
                    "last_modified_iso": remote_meta.get("last_modified_iso"),
                    "etag": remote_meta.get("etag"),
                    "content_length": remote_meta.get("content_length"),
                    "sha256": sha,
                })
            except Exception as e:
                logging.error(f"❌ Failed: {url} → {e}")
                metadata["files"].append({
                    "label": key,
                    "url": url,
                    "path": out_path,
                    "status": "error",
                    "error": str(e),
                    "last_modified": remote_meta.get("last_modified"),
                    "last_modified_iso": remote_meta.get("last_modified_iso"),
                    "etag": remote_meta.get("etag"),
                    "content_length": remote_meta.get("content_length"),
                })

        metadata["source"] = {
            "name": "MedGen",
            "version": freshest_str,
            "files": perfile_versions
        }

        with open(self.metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    cfg = yaml.safe_load(open(args.config))
    MedGenDownloader(cfg).run()