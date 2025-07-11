#!/usr/bin/env python
"""
wikipathways_download.py - Download WikiPathways GMT and JSON files
  • Checks for updates using MD5 hashes
  • Saves diffs if content has changed
  • Logs metadata to JSON
"""

import os
import logging
import requests
import yaml
import json
import hashlib
import difflib
from datetime import datetime
import re
from bs4 import BeautifulSoup

def setup_logging(log_file):
    handlers = [logging.StreamHandler()]
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        handlers.insert(0, logging.FileHandler(log_file, mode='a'))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
        force=True
    )

def compute_md5(file_path):
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except FileNotFoundError:
        return None

def save_diff(old_path, new_path, diff_txt_path, diff_html_path):
    with open(old_path, 'r') as f1, open(new_path, 'r') as f2:
        old_lines = f1.readlines()
        new_lines = f2.readlines()

    diff = list(difflib.unified_diff(old_lines, new_lines, fromfile='old', tofile='new'))
    html_diff = difflib.HtmlDiff().make_file(old_lines, new_lines, fromdesc='Old', todesc='New')

    with open(diff_txt_path, 'w') as f:
        f.writelines(diff)
    with open(diff_html_path, 'w') as f:
        f.write(html_diff)

class WikiPathwaysDownloader:
    def __init__(self, config):
        self.cfg = config["pathways"]["wikipathways"]
        setup_logging(self.cfg.get("log_file"))
        self.metadata = {
            "timestamp": {"start": str(datetime.now())},
            "downloads": []
        }
   
    def download_and_check(self, url, raw_path):
        os.makedirs(os.path.dirname(raw_path), exist_ok=True)
        logging.info(f"⬇️ Downloading {url}...")

        temp_path = raw_path + ".tmp"
        try:
            response = requests.get(url, verify=False)
            response.raise_for_status()
            with open(temp_path, 'wb') as f:
                f.write(response.content)

            old_md5 = compute_md5(raw_path)
            new_md5 = compute_md5(temp_path)

            if old_md5 == new_md5:
                logging.info("🟡 No change detected. Keeping existing file.")
                os.remove(temp_path)
            else:
                logging.info("🟢 Update detected. Replacing old file.")
                if os.path.exists(raw_path):
                    diff_txt = raw_path + ".diff.txt"
                    diff_html = raw_path + ".diff.html"
                    save_diff(raw_path, temp_path, diff_txt, diff_html)
                    logging.info(f"📄 Diffs saved: {diff_txt}, {diff_html}")
                os.replace(temp_path, raw_path)

                self.metadata["downloads"].append({
                    "url": url,
                    "path": raw_path,
                    "downloaded_at": str(datetime.now()),
                    "md5": new_md5,
                    "updated": True
                })
        except Exception as e:
            logging.error(f"❌ Failed to download {url}: {e}")
            if os.path.exists(temp_path):
                os.remove(temp_path)

    @staticmethod
    def get_latest_gmt_url():
        """Scrape the latest Homo sapiens GMT file from the WikiPathways 'current/gmt' folder"""
        index_url = "https://data.wikipathways.org/current/gmt/"
        try:
            response = requests.get(index_url, verify=False)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all("a", href=True)
            for link in links:
                href = link['href']
                if re.match(r"wikipathways-\d{8}-gmt-Homo_sapiens\.gmt", href):
                    return index_url + href, href  # full URL and filename
            raise ValueError("❌ Could not find a valid Homo sapiens GMT file.")
        except Exception as e:
            logging.error(f"❌ Failed to get latest GMT file: {e}")
            raise

    def run(self):
        latest_url, latest_filename = self.get_latest_gmt_url()
        for key, entry in self.cfg["files"].items():
            if key == "gmt":
                # Construct dynamic path for versioned file
                save_dir = entry["save_dir"]
                os.makedirs(save_dir, exist_ok=True)
                raw_path = os.path.join(save_dir, latest_filename)
                latest_alias = entry.get("latest_local_copy")

                # Download the latest version
                self.download_and_check(latest_url, raw_path)

                # Save a copy to latest_local_copy
                if latest_alias:
                    os.replace(raw_path, latest_alias)
                    logging.info(f"📎 Copied latest version to {latest_alias}")
                    self.metadata["downloads"][-1]["alias_path"] = latest_alias

                self.metadata["downloads"][-1]["version"] = latest_filename
            else:
                # Handle static entries like 'pathway_list'
                url = entry["url"]
                raw_path = entry["raw_path"]
                self.download_and_check(url, raw_path)

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
    WikiPathwaysDownloader(config).run()
