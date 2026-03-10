# omim_download.py - Downloads static OMIM files using an API key and YAML config with diff tracking

import os
import logging
import requests
from pathlib import Path
import yaml
import argparse
import difflib
from datetime import datetime
import email.utils
import json

PUBLIC_MIM2GENE_URL = "https://omim.org/static/omim/data/mim2gene.txt"

class OMIMDownloader:
    def __init__(self, config):
        self.cfg = config["omim"]
        self.output_dir = Path(self.cfg["raw_dir"])
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Prefer non-interactive; only use prompt if you REALLY want to.
        self.api_key = os.environ.get("OMIM_API_KEY", "").strip()

        self.metadata_file = Path(self.cfg["dl_metadata_file"])
        self.metadata_file.parent.mkdir(parents=True, exist_ok=True)

        self.log_file = Path(self.cfg["log_file"])
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(self.log_file, mode="a"),
                logging.StreamHandler()
            ]
        )

        self.urls = self.build_urls()
        # Load old metadata for skip checks
        self._old_file_versions = {}
        if self.metadata_file.exists():
            try:
                old = yaml.safe_load(self.metadata_file.read_text()) or {}
                for f in old.get("source", {}).get("files", []):
                    if f.get("file") and f.get("last_modified"):
                        self._old_file_versions[f["file"]] = f["last_modified"]
            except Exception:
                pass

    def build_urls(self):
        urls = {}
        # Always include the public file
        urls["mim2gene.txt"] = PUBLIC_MIM2GENE_URL

        # Include key-gated files only if we have an API key
        if self.api_key:
            base = f"https://data.omim.org/downloads/{self.api_key}"
            urls["mimTitles.txt"] = f"{base}/mimTitles.txt"
            urls["genemap2.txt"]  = f"{base}/genemap2.txt"
            urls["morbidmap.txt"] = f"{base}/morbidmap.txt"
        else:
            logging.warning("OMIM_API_KEY not set; will fetch only public mim2gene.txt")

        return urls

    @staticmethod
    def _lm_to_version(resp):
        lm = resp.headers.get("Last-Modified") or resp.headers.get("last-modified")
        if not lm:
            return "unknown", None
        try:
            dt = email.utils.parsedate_to_datetime(lm)
            return dt.strftime("%Y-%m-%d"), dt
        except Exception:
            return "unknown", None

    def save_diff_if_changed(self, new_content, dest: Path):
        if dest.exists():
            old_content = dest.read_text()
            if old_content != new_content:
                diff_file = dest.with_suffix(dest.suffix + ".diff.txt")
                diff = difflib.unified_diff(
                    old_content.splitlines(),
                    new_content.splitlines(),
                    fromfile="previous",
                    tofile="new",
                    lineterm=""
                )
                diff_text = "\n".join(diff)
                diff_file.write_text(diff_text)
                logging.info(f"🔁 Changes detected in {dest.name}, diff saved to {diff_file.name}")
            else:
                logging.info(f"🟢 No changes detected in {dest.name}")
        dest.write_text(new_content)

    def download_files(self):
        file_meta = []
        freshest_dt = None
        freshest_str = "unknown"

        for filename, url in self.urls.items():
            dest = self.output_dir / filename

            # HEAD check: skip if Last-Modified unchanged
            try:
                head_resp = requests.head(url, allow_redirects=True, timeout=30)
                head_vstr, _ = self._lm_to_version(head_resp)
                old_ver = self._old_file_versions.get(filename)
                if old_ver and old_ver == head_vstr and dest.exists():
                    logging.info(f"Skipping {filename} — unchanged (Last-Modified: {head_vstr})")
                    file_meta.append({"file": filename, "url": url, "last_modified": head_vstr, "status": "skipped"})
                    continue
            except Exception:
                pass  # fall through to download

            logging.info(f"⬇️  Downloading {filename} from {url}")
            try:
                r = requests.get(url, timeout=60)
            except Exception as e:
                logging.error(f"❌ Request failed for {filename}: {e}")
                continue

            if r.status_code == 200:
                self.save_diff_if_changed(r.text, dest)
                vstr, vdt = self._lm_to_version(r)
                file_meta.append({"file": filename, "url": url, "last_modified": vstr})
                if vdt and (freshest_dt is None or vdt > freshest_dt):
                    freshest_dt, freshest_str = vdt, vstr
                logging.info(f"✅ Saved to {dest}")
            elif r.status_code in (401, 403):
                logging.warning(f"🔒 Unauthorized for {filename} (HTTP {r.status_code}). "
                                f"Is OMIM_API_KEY set correctly?")
            else:
                logging.error(f"❌ Failed to download {filename} — HTTP {r.status_code}")

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
        logging.info(f"📝 Wrote metadata: {self.metadata_file}")

    def run(self):
        self.download_files()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    with open(args.config) as f:
        full_config = yaml.safe_load(f)

    downloader = OMIMDownloader(full_config)
    downloader.run()
