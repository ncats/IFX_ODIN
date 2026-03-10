#!/usr/bin/env python
"""
mondo_download.py - Enhanced with version tracking and structured diffs
"""
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

class MondoDownloader:
    def __init__(self, full_config):
        self.full_config = full_config
        self.cfg = full_config["mondo"]
        self.qc_mode = full_config.get("global", {}).get("qc_mode", False)
        self.save_full_diff = os.getenv("MONDO_SAVE_FULL_DIFF", "0").lower() in ("1","true","yes","y")

        setup_logging(os.path.abspath(self.cfg["log_file"]))

        self.targets = {
            "json": {
                "url": self.cfg.get("json_download_url", "https://purl.obolibrary.org/obo/mondo.json"),
                "file": Path(self.cfg["json_file"]),
                "diffable": True
            },
            "owl": {
                "url": self.cfg.get("owl_download_url", "https://purl.obolibrary.org/obo/mondo.owl"),
                "file": Path(self.cfg["owl_file"]),
                "diffable": False
            }
        }
        for t in self.targets.values():
            os.makedirs(t["file"].parent, exist_ok=True)

        self.metadata_file = Path(self.cfg["dl_metadata_file"])
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
    def _last_modified(url: str):
        """Return (YYYY-MM-DD, datetime|None) from Last-Modified header."""
        try:
            r = requests.head(url, allow_redirects=True, timeout=30)
            lm = r.headers.get("Last-Modified") or r.headers.get("last-modified")
            if lm:
                dt = email.utils.parsedate_to_datetime(lm)
                return dt.strftime("%Y-%m-%d"), dt
        except Exception as e:
            logging.warning(f"MONDO HEAD failed for {url}: {e}")
        return "unknown", None

    def compute_structured_diff(self, old_file: Path, new_file: Path, label: str):
        """Generate structured diff for JSON files"""
        base = new_file.stem
        summary_file   = new_file.with_name(f"{base}.diff.txt")
        changed_ids    = new_file.with_name(f"{base}.changed_ids.json")
        full_diff_file = new_file.with_name(f"{base}.diff.json")

        try:
            logging.info(f"📂 Loading {label.upper()} JSON files for comparison...")
            with open(old_file, "r", encoding="utf-8") as f1:
                old_data = json.load(f1)
            with open(new_file, "r", encoding="utf-8") as f2:
                new_data = json.load(f2)

            old_graph = {d["@id"]: d for d in old_data.get("@graph", []) if isinstance(d, dict) and "@id" in d}
            new_graph = {d["@id"]: d for d in new_data.get("@graph", []) if isinstance(d, dict) and "@id" in d}

            old_ids = set(old_graph)
            new_ids = set(new_graph)
            added   = sorted(new_ids - old_ids)
            removed = sorted(old_ids - new_ids)
            common  = old_ids & new_ids
            updated = [mid for mid in common if old_graph[mid] != new_graph[mid]]

            summary = [
                f"📄 {label.upper()} DIFF SUMMARY",
                f"➕ Added IDs: {len(added)}",
                f"➖ Removed IDs: {len(removed)}",
                f"✏️  Updated IDs: {len(updated)}",
                "",
                f"Sample Added: {added[:3]}",
                f"Sample Removed: {removed[:3]}",
                f"Sample Updated: {updated[:3]}",
            ]
            logging.info("\n".join(summary))
            summary_file.write_text("\n".join(summary), encoding="utf-8")
            changed_ids.write_text(json.dumps({"changed_ids": added + removed + updated}, indent=2), encoding="utf-8")

            # Only write the expensive DeepDiff if qc_mode or env flag says so
            if self.qc_mode and self.save_full_diff:
                start = time.time()
                diff = DeepDiff(list(old_graph.values()), list(new_graph.values()), ignore_order=True)
                full_diff_file.write_text(json.dumps(diff, indent=2), encoding="utf-8")
                logging.info(f"✅ Full DeepDiff written → {full_diff_file} ({time.time()-start:.1f}s)")
            else:
                full_diff_file = None

            return str(changed_ids), str(full_diff_file) if full_diff_file else None

        except Exception as e:
            logging.warning(f"⚠️ Diff generation failed for {label}: {e}")
            return None, None

    def download_file(self, url: str, target_file: Path, label: str, diffable: bool):
        """Download file with diff tracking"""
        tmp_file = target_file.with_suffix(".tmp")
        backup_file = None
        changed_ids_path = None
        full_diff_path = None
        status = "new"

        try:
            logging.info(f"🌐 Downloading {label.upper()} from {url}")
            with requests.get(url, stream=True, timeout=180) as response:
                response.raise_for_status()
                with open(tmp_file, "wb") as f:
                    for chunk in response.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)

            if target_file.exists():
                with open(target_file, "rb") as f1, open(tmp_file, "rb") as f2:
                    if f1.read() == f2.read():
                        os.remove(tmp_file)
                        status = "skipped"
                        logging.info(f"🟢 {label.upper()} is up to date.")
                    else:
                        logging.info(f"🔄 {label.upper()} file update detected.")
                        backup_file = target_file.with_name(f"{target_file.stem}.backup")
                        shutil.copy2(target_file, backup_file)
                        if diffable:
                            changed_ids_path, full_diff_path = self.compute_structured_diff(backup_file, tmp_file, label)
                        os.replace(tmp_file, target_file)
                        status = "updated"
            else:
                os.replace(tmp_file, target_file)

        finally:
            if not self.qc_mode:
                for f in [changed_ids_path, full_diff_path, str(backup_file) if backup_file else None]:
                    if f and os.path.exists(f):
                        try: os.remove(f)
                        except Exception: pass
            if tmp_file.exists():
                try: os.remove(tmp_file)
                except Exception: pass

        return {
            "label": label,
            "url": url,
            "path": str(target_file),
            "status": status,
            "changed_ids_json": changed_ids_path,
            "full_diff_json": full_diff_path
        }

    def run(self):
        # HEAD-check each target first, skip if unchanged
        results = []
        lm_cache = {}  # reuse for metadata so we don't HEAD twice
        for key, meta in self.targets.items():
            lm_str, lm_dt = self._last_modified(meta["url"])
            lm_cache[key] = (lm_str, lm_dt)
            old_ver = self._old_versions.get(key)
            if old_ver and old_ver == lm_str and meta["file"].exists():
                logging.info(f"Skipping {key} — unchanged (Last-Modified: {lm_str})")
                results.append({"label": key, "url": meta["url"], "path": str(meta["file"]),
                                "status": "skipped", "changed_ids_json": None, "full_diff_json": None})
                continue
            results.append(self.download_file(meta["url"], meta["file"], key, meta["diffable"]))

        # Per-file last-modified + overall version = freshest across json/owl
        json_url = self.targets["json"]["url"]
        owl_url  = self.targets["owl"]["url"]
        j_str, j_dt = lm_cache.get("json", ("unknown", None))
        o_str, o_dt = lm_cache.get("owl", ("unknown", None))
        chosen_dt = j_dt if (j_dt and (not o_dt or j_dt >= o_dt)) else o_dt
        chosen_str = j_str if chosen_dt == j_dt else o_str

        meta_out = {
            "timestamp": datetime.now().isoformat(),
            "downloads": results,
            "source": {
                "name": "MONDO",
                "version": chosen_str,
                "files": [
                    {"label": "json", "url": json_url, "last_modified": j_str},
                    {"label": "owl",  "url": owl_url,  "last_modified": o_str},
                ],
            },
        }

        with open(self.metadata_file, "w", encoding="utf-8") as f:
            json.dump(meta_out, f, indent=2, ensure_ascii=False)
        logging.info(f"📦 Metadata written → {self.metadata_file}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="MONDO file downloader for JSON and OWL with diff tracking")
    parser.add_argument("--config", required=True, help="YAML config path")
    args = parser.parse_args()
    with open(args.config) as f:
        cfg = yaml.safe_load(f)
    MondoDownloader(cfg).run()