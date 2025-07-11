# This script will be `reactome_download.py`

#!/usr/bin/env python
import os
import requests
import logging
import yaml
import json
import hashlib
from datetime import datetime
from difflib import unified_diff

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ReactomeDownloader:
    def __init__(self, config):
        self.cfg = config["pathways"]["reactome"]
        self.metadata = {
            "timestamp": {"start": str(datetime.now())},
            "downloads": []
        }

    def compute_md5(self, filepath):
        hash_md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def download_and_compare(self, url, dest_path):
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        logging.info(f"⬇️ Downloading {url} ...")

        try:
            response = requests.get(url)
            response.raise_for_status()
            new_content = response.content

            if os.path.exists(dest_path):
                with open(dest_path, 'rb') as old_file:
                    old_content = old_file.read()
                if old_content != new_content:
                    logging.info("🔁 File content changed. Generating diff...")
                    old_lines = old_content.decode(errors='ignore').splitlines()
                    new_lines = new_content.decode(errors='ignore').splitlines()
                    diff = unified_diff(old_lines, new_lines, fromfile='old', tofile='new', lineterm='')
                    diff_path = dest_path + ".diff.txt"
                    with open(diff_path, 'w') as diff_file:
                        diff_file.write("\n".join(diff))
                    logging.info(f"📄 Diff saved to {diff_path}")
                else:
                    logging.info("✅ No change detected in file.")
            with open(dest_path, 'wb') as f:
                f.write(new_content)
            logging.info(f"✅ Saved to {dest_path}")

            self.metadata["downloads"].append({
                "url": url,
                "path": dest_path,
                "downloaded_at": str(datetime.now()),
                "md5": self.compute_md5(dest_path)
            })

        except Exception as e:
            logging.error(f"❌ Failed to download {url}: {e}")

    def run(self):
        for key, entry in self.cfg["files"].items():
            self.download_and_compare(entry["url"], entry["raw_path"])

        gmt = self.cfg.get("gmt_file")
        if gmt:
            self.download_and_compare(gmt["url"], gmt["raw_path"])

        self.metadata["timestamp"]["end"] = str(datetime.now())
        with open(self.cfg["dl_metadata_file"], "w") as f:
            json.dump(self.metadata, f, indent=2)
        logging.info(f"📝 Download metadata saved to {self.cfg['dl_metadata_file']}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    ReactomeDownloader(config).run()
