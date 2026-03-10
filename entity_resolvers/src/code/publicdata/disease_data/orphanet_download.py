#!/usr/bin/env python

# orphanet_download.py - Downloads OWL and XML files from Orphanet with diff tracking

import os
import sys
import json
import logging
import argparse
import shutil
import difflib
import requests
import yaml
from datetime import datetime
from pathlib import Path
import email.utils

def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    handlers = [
        logging.FileHandler(log_file, mode="a"),
        logging.StreamHandler(sys.stdout),
    ]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
        force=True,
    )

class OrphanetDownloader:
    def __init__(self, full_config):
        self.cfg = full_config["orphanet"]
        self.qc_mode = full_config.get("global", {}).get("qc_mode", False)

        setup_logging(os.path.abspath(self.cfg["log_file"]))

        self.owl_url = self.cfg["owl_url"]
        self.owl_file = Path(self.cfg["owl_file"])

        self.xml_url = self.cfg["xml_url"]
        self.xml_file = Path(self.cfg["xml_file"])

        self.metadata_file = Path(self.cfg["dl_metadata_file"])

        os.makedirs(self.owl_file.parent, exist_ok=True)
        os.makedirs(self.xml_file.parent, exist_ok=True)
        os.makedirs(self.metadata_file.parent, exist_ok=True)
        # Load old metadata for skip checks
        self._old_versions = {}
        if self.metadata_file.exists():
            try:
                old = json.loads(self.metadata_file.read_text())
                for f in old.get("source", {}).get("files", []):
                    if f.get("label") and f.get("last_modified"):
                        self._old_versions[f["label"]] = f["last_modified"]
            except Exception:
                pass

    @staticmethod
    def _last_modified_version(url: str):
        """Return (YYYY-MM-DD, datetime|None) from Last-Modified header, else ('unknown', None)."""
        try:
            r = requests.head(url, allow_redirects=True, timeout=30)
            lm = r.headers.get("Last-Modified") or r.headers.get("last-modified")
            if lm:
                dt = email.utils.parsedate_to_datetime(lm)
                return dt.strftime("%Y-%m-%d"), dt
        except Exception as e:
            logging.warning(f"Orphanet version check failed for {url}: {e}")
        return "unknown", None

    def _prompt_for_new_url(self, current_url: str) -> str | None:
        """
        Prompt the user for an updated URL when the download fails.
        Returns a new URL string, or None if the user chooses to skip.
        """
        logging.warning(
            "\nThe download failed for:\n"
            f"   {current_url}\n"
            "If a newer or corrected URL is available, paste it below.\n"
            "Press ENTER without typing anything to skip this file.\n"
        )
        try:
            new_url = input("New URL (or leave blank to skip): ").strip()
        except EOFError:
            logging.error("Interactive input is not available; cannot prompt for a replacement URL.")
            return None
        except Exception as e:
            logging.error(f"Interactive prompt failed: {e}")
            return None

        if not new_url:
            logging.info("No replacement URL provided; this file will be skipped.")
            return None

        logging.info(f"Using user-provided URL: {new_url}")
        return new_url

    def download_with_diff(self, url, dest: Path):
        """
        Attempt to download `url` to `dest`, with diff tracking.
        If the download fails (e.g., 404), prompt the user for a replacement URL.
        Returns: (status, diff_txt, diff_html, final_url_used)
        """
        temp_file = dest.with_suffix(dest.suffix + ".tmp")
        backup_file = dest.with_name(dest.stem + ".backup")
        diff_txt = dest.with_name(dest.stem + ".diff.txt")
        diff_html = dest.with_name(dest.stem + ".diff.html")

        current_url = url

        while True:
            logging.info(f"⬇️  Attempting download from: {current_url}")
            try:
                response = requests.get(current_url, stream=True, timeout=120)
            except Exception as e:
                logging.error(f"❌ Failed to request {current_url}: {e}")
                new_url = self._prompt_for_new_url(current_url)
                if not new_url:
                    logging.error(f"Skipping download for {dest} after failed request.")
                    return "skipped_failed", None, None, current_url
                current_url = new_url
                continue

            if response.status_code != 200:
                logging.error(f"❌ Failed to download {current_url} (HTTP {response.status_code})")
                if "ordo" in current_url.lower():
                    logging.warning(
                        "⚠️  The hardcoded ORDO OWL version URL may be outdated.\n"
                        "   👉 Please check https://www.orphadata.com/ordo/ for the latest OWL file link."
                    )
                elif "en_product6" in current_url.lower():
                    logging.warning(
                        "⚠️  The hardcoded XML gene association URL may have changed.\n"
                        "   👉 Please verify the XML link via https://www.orphadata.com/data/xml/"
                    )

                new_url = self._prompt_for_new_url(current_url)
                if not new_url:
                    logging.error(f"Skipping download for {dest} after HTTP error and no replacement URL.")
                    return "skipped_failed", None, None, current_url
                current_url = new_url
                continue

            # Success: break out of loop with `response` and `current_url`
            break

        # Write to temp file
        with open(temp_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)

        # If dest exists, compare and maybe diff
        if dest.exists():
            with open(dest, "rb") as f1, open(temp_file, "rb") as f2:
                if f1.read() == f2.read():
                    os.remove(temp_file)
                    logging.info(f"🟢 No change: {dest}")
                    return "skipped", None, None, current_url

            shutil.copy2(dest, backup_file)

            # Text diffs (ignore decode errors)
            with open(backup_file, "r", encoding="utf-8", errors="ignore") as old_f, \
                 open(temp_file, "r", encoding="utf-8", errors="ignore") as new_f:
                old_lines = old_f.readlines()
                new_lines = new_f.readlines()

            with open(diff_txt, "w", encoding="utf-8") as f:
                f.writelines(difflib.unified_diff(old_lines, new_lines, fromfile="old", tofile="new"))

            with open(diff_html, "w", encoding="utf-8") as f:
                f.write(difflib.HtmlDiff().make_file(old_lines, new_lines, "Old", "New"))

            os.replace(temp_file, dest)
            logging.info(f"📝 Updated: {dest}")
            return "updated", str(diff_txt), str(diff_html), current_url

        # No previous file: just move temp into place
        os.replace(temp_file, dest)
        logging.info(f"✅ New file saved: {dest}")
        return "new", None, None, current_url

    def run(self):
        # HEAD-check each file: skip if Last-Modified unchanged
        owl_url_used = self.owl_url
        xml_url_used = self.xml_url
        v1_str, v1_dt = self._last_modified_version(self.owl_url)
        v2_str, v2_dt = self._last_modified_version(self.xml_url)

        old_owl = self._old_versions.get("owl")
        if old_owl and old_owl == v1_str and self.owl_file.exists():
            logging.info(f"Skipping OWL — unchanged (Last-Modified: {v1_str})")
            owl_status, owl_diff_txt, owl_diff_html = "skipped", None, None
        else:
            owl_status, owl_diff_txt, owl_diff_html, owl_url_used = self.download_with_diff(
                self.owl_url, self.owl_file
            )

        old_xml = self._old_versions.get("xml")
        if old_xml and old_xml == v2_str and self.xml_file.exists():
            logging.info(f"Skipping XML — unchanged (Last-Modified: {v2_str})")
            xml_status, xml_diff_txt, xml_diff_html = "skipped", None, None
        else:
            xml_status, xml_diff_txt, xml_diff_html, xml_url_used = self.download_with_diff(
                self.xml_url, self.xml_file
            )

        # Optionally update the instance URLs so other code sees what was used
        self.owl_url = owl_url_used
        self.xml_url = xml_url_used

        # 2) Optional cleanup if qc_mode is False
        if not self.qc_mode:
            for f in [
                self.owl_file.with_suffix(".backup"),
                owl_diff_txt,
                owl_diff_html,
                self.xml_file.with_suffix(".backup"),
                xml_diff_txt,
                xml_diff_html,
            ]:
                if f and os.path.exists(f):
                    os.remove(f)

        # 3) Version detection (pick freshest Last-Modified — already fetched above)
        chosen_dt = v1_dt if (v1_dt and (not v2_dt or v1_dt >= v2_dt)) else v2_dt
        chosen_str = v1_str if chosen_dt == v1_dt else v2_str

        # 4) Metadata
        meta = {
            "timestamp": datetime.now().isoformat(),
            "owl_file": str(self.owl_file),
            "xml_file": str(self.xml_file),
            "owl_status": owl_status,
            "xml_status": xml_status,
            "owl_diff_txt": owl_diff_txt,
            "owl_diff_html": owl_diff_html,
            "xml_diff_txt": xml_diff_txt,
            "xml_diff_html": xml_diff_html,
            "source": {
                "name": "Orphanet/ORDO",
                "version": chosen_str,
                "files": [
                    {"label": "owl", "url": owl_url_used, "last_modified": v1_str},
                    {"label": "xml", "url": xml_url_used, "last_modified": v2_str},
                ],
            },
        }

        with open(self.metadata_file, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2, ensure_ascii=False)

        logging.info(f"📝 Metadata saved → {self.metadata_file}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Download Orphanet OWL and XML files")
    parser.add_argument("--config", required=True, help="YAML config path")
    args = parser.parse_args()
    cfg = yaml.safe_load(open(args.config))
    OrphanetDownloader(cfg).run()
