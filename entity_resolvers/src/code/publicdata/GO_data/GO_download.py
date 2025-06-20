# GO Downloader - ODIN style
import os
import sys
import logging
import argparse
import requests
import yaml
import json
import hashlib
import shutil
from datetime import datetime
from pathlib import Path


def setup_logging(log_path):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout)
        ],
        force=True
    )


def compute_sha256(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def write_metadata(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(content, f, indent=2)


class GODownloader:
    def __init__(self, config):
        self.full_config = config
        self.cfg = config["go"]
        self.qc_mode = config.get("global", {}).get("qc_mode", False)
        setup_logging(os.path.abspath(self.cfg["log_file"]))

        self.metadata_file = Path(self.cfg["dl_metadata_file"])
        os.makedirs(self.metadata_file.parent, exist_ok=True)

    def _download_with_diff(self, url, dest_path, label):
        tmp_path = Path(dest_path).with_suffix(".tmp")
        backup_path = Path(dest_path).with_suffix(".backup")

        logging.info(f"🌐 Downloading {label} from {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        new_hash = compute_sha256(tmp_path)
        old_hash = compute_sha256(dest_path) if os.path.exists(dest_path) else None

        if old_hash == new_hash:
            logging.info(f"✅ No change detected for {label}.")
            os.remove(tmp_path)
            return {
                "label": label,
                "path": dest_path,
                "url": url,
                "sha256": new_hash,
                "status": "unchanged"
            }

        logging.info(f"🔄 Update detected for {label}.")
        shutil.copy2(dest_path, backup_path) if os.path.exists(dest_path) else None
        shutil.move(tmp_path, dest_path)

        diff_path = None
        if self.qc_mode and os.path.exists(backup_path):
            diff_path = str(Path(dest_path).with_suffix(".diff.txt"))
            with open(diff_path, "w") as f:
                f.write(f"Previous hash: {old_hash}\n")
                f.write(f"New hash:      {new_hash}\n")
            logging.info(f"📝 Diff file written: {diff_path}")

        return {
            "label": label,
            "path": dest_path,
            "url": url,
            "sha256": new_hash,
            "status": "updated",
            "diff_file": diff_path
        }

    def run(self):
        results = []
        for label, key in zip([
            "GO OBO", "GOA GAF", "gene2go"],
            ["obo", "gaf", "gene2go"]):
            url = self.cfg[f"{key}_url"]
            dest = self.cfg[f"{key}_raw"]
            results.append(self._download_with_diff(url, dest, label))

        meta = {
            "timestamp": datetime.now().isoformat(),
            "downloads": results
        }
        write_metadata(self.cfg["dl_metadata_file"], meta)
        logging.info(f"📦 Metadata written → {self.cfg['dl_metadata_file']}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="GO data downloader with diff tracking")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    GODownloader(config).run()
