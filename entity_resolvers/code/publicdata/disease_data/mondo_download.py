#!/usr/bin/env python

# mondo_download.py - Modular MONDO downloader with optimized diff tracking

import os
import sys
import yaml
import json
import logging
import time
import argparse
import shutil
from datetime import datetime
from pathlib import Path
import requests
from deepdiff import DeepDiff

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

class MondoDownloader:
    def __init__(self, full_config):
        self.full_config = full_config
        self.cfg = full_config["mondo"]
        self.qc_mode = full_config.get("global", {}).get("qc_mode", False)
        setup_logging(os.path.abspath(self.cfg["log_file"]))
        self.download_url = self.cfg.get("download_url", "https://purl.obolibrary.org/obo/mondo.json")
        self.mondo_file = Path(self.cfg["mondo_file"])
        self.metadata_file = Path(self.cfg["dl_metadata_file"])

        os.makedirs(self.mondo_file.parent, exist_ok=True)
        os.makedirs(self.metadata_file.parent, exist_ok=True)

    def compute_structured_diff(self, old_file, new_file):
        base = self.mondo_file.stem
        summary_file = self.mondo_file.with_name(f"{base}.diff.txt")
        changed_ids_file = self.mondo_file.with_name(f"{base}.changed_ids.json")
        full_diff_file = self.mondo_file.with_name(f"{base}.diff.json")

        try:
            logging.info("📂 Loading MONDO JSON files for comparison...")
            with open(old_file, "r", encoding="utf-8") as f1:
                old_data = json.load(f1)
            with open(new_file, "r", encoding="utf-8") as f2:
                new_data = json.load(f2)

            old_graph = {d["@id"]: d for d in old_data.get("@graph", []) if "@id" in d}
            new_graph = {d["@id"]: d for d in new_data.get("@graph", []) if "@id" in d}

            old_ids = set(old_graph)
            new_ids = set(new_graph)

            added = sorted(new_ids - old_ids)
            removed = sorted(old_ids - new_ids)
            common = old_ids & new_ids

            updated = []
            for mid in common:
                if old_graph[mid] != new_graph[mid]:
                    updated.append(mid)

            changed_ids = added + removed + updated
            summary = [
                f"🔄 MONDO DIFF SUMMARY: {base}",
                f"➕ Added IDs: {len(added)}",
                f"➖ Removed IDs: {len(removed)}",
                f"✏️  Updated IDs: {len(updated)}",
                "",
                f"Sample Added: {added[:3]}",
                f"Sample Removed: {removed[:3]}",
                f"Sample Updated: {updated[:3]}",
            ]
            logging.info("\n".join(summary))

            with open(summary_file, "w") as f:
                f.write("\n".join(summary))

            with open(changed_ids_file, "w") as f:
                json.dump({"changed_ids": changed_ids}, f, indent=2)

            # Prompt to save full DeepDiff
            save_full = input("💡 Save full DeepDiff JSON diff as well? (y/N): ").strip().lower()
            if save_full == "y":
                from deepdiff import DeepDiff
                logging.info("🔍 Running full DeepDiff...")
                start = time.time()
                diff = DeepDiff(list(old_graph.values()), list(new_graph.values()), ignore_order=True)
                logging.info(f"✅ Full DeepDiff completed in {time.time() - start:.2f} sec")

                with open(full_diff_file, "w") as f:
                    json.dump(diff, f, indent=2)
                logging.info(f"📝 Full JSON diff written: {full_diff_file}")
            else:
                full_diff_file = None

            return str(changed_ids_file), str(full_diff_file)

        except Exception as e:
            logging.warning(f"⚠️ Structured diff generation failed: {e}")
            return None, None

    def download(self):
        tmp_file = self.mondo_file.with_suffix(".json.tmp")
        backup_file = None
        changed_ids_path = None
        full_diff_path = None
        status = "new"

        try:
            logging.info(f"🌐 Downloading: {self.download_url}")
            response = requests.get(self.download_url, stream=True)
            response.raise_for_status()

            with open(tmp_file, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            if self.mondo_file.exists():
                with open(self.mondo_file, "rb") as f1, open(tmp_file, "rb") as f2:
                    if f1.read() == f2.read():
                        os.remove(tmp_file)
                        status = "skipped"
                        logging.info("✅ No update needed.")
                    else:
                        logging.info("🔄 MONDO file update detected.")
                        backup_file = self.mondo_file.with_name(f"{self.mondo_file.stem}.backup")
                        shutil.copy2(self.mondo_file, backup_file)
                        changed_ids_path, full_diff_path = self.compute_structured_diff(backup_file, tmp_file)
                        os.replace(tmp_file, self.mondo_file)
                        status = "updated"
            else:
                os.replace(tmp_file, self.mondo_file)

        finally:
            # 🧼 Clean up files unless QC mode is on
            if not self.qc_mode:
                for f in [changed_ids_path, full_diff_path, str(backup_file) if backup_file else None]:
                    if f and os.path.exists(f):
                        os.remove(f)

            # Always clean up temp file if it somehow survives
            if tmp_file.exists():
                os.remove(tmp_file)

        return status, changed_ids_path, full_diff_path

    def run(self):
        status, changed_ids_path, full_diff_path = self.download()

        meta = {
            "timestamp": datetime.now().isoformat(),
            "mondo_file": str(self.mondo_file),
            "mondo_found": self.mondo_file.exists(),
            "status": status,
            "changed_ids_json": changed_ids_path,
            "full_diff_json": full_diff_path,
        }

        with open(self.metadata_file, "w") as f:
            json.dump(meta, f, indent=2)

        logging.info(f"📦 Metadata written → {self.metadata_file}")
        logging.info(f"💾 MONDO file saved at: {self.mondo_file}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="MONDO file downloader with diff tracking")
    parser.add_argument("--config", required=True, help="YAML config path")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    MondoDownloader(cfg).run()
