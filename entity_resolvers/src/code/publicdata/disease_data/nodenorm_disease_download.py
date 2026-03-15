#!/usr/bin/env python
# nodenorm_disease_download.py - Download NodeNorm Disease.txt with metadata/hash tracking only

import os
import json
import yaml
import hashlib
import logging
import argparse
import requests
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


class NodeNormDiseaseDownloader:
    def __init__(self, full_config):
        self.cfg = full_config["nodenorm"]
        setup_logging(self.cfg["log_file"])
        self.url = self.cfg["url_base"].rstrip("/") + "/Disease.txt"
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
        old_meta = self._old_meta()

        tmp = self.raw_file.with_suffix(".tmp")
        r = requests.get(self.url, stream=True, timeout=(30, 180))
        r.raise_for_status()

        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

        new_hash = sha256sum(tmp)
        old_hash = old_meta.get("sha256")
        changed = True

        if self.raw_file.exists() and old_hash == new_hash:
            changed = False
            tmp.unlink(missing_ok=True)
        else:
            os.replace(tmp, self.raw_file)

        meta = {
            "timestamp": datetime.now().isoformat(),
            "url": self.url,
            "raw_file": str(self.raw_file),
            "sha256": old_hash if not changed else sha256sum(self.raw_file),
            "changed": changed,
        }
        json.dump(meta, open(self.metadata_file, "w"), indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    cfg = yaml.safe_load(open(args.config))
    NodeNormDiseaseDownloader(cfg).run()