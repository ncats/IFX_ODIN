#!/usr/bin/env python3
"""
UMLS disease downloader (resumable + autosave + throttled logs + heartbeat + robust stop + release capture)

"""

import os
import sys
import json
import time
import yaml
import logging
import argparse
import requests
import pandas as pd
from datetime import datetime
from typing import List, Tuple, Optional, Dict

SEARCH_BASE = "https://uts-ws.nlm.nih.gov/rest"
RELEASES_API = "https://uts-ws.nlm.nih.gov/releases"  # ?releaseType=umls-full-release&current=true


# ---------------- Logging ----------------
def setup_logging(log_file: str, level: str = "INFO"):
    log_dir = os.path.dirname(os.path.abspath(log_file))
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, str(level).upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_file, mode="a")],
        force=True
    )


# ---------------- HTTP (retry/backoff) ----------------
def request_json(url: str, params: dict = None, timeout: int = 30, max_retries: int = 6, backoff_base: float = 1.6):
    """GET JSON with retries + exponential backoff. Returns (json, status_code) or (None, None)."""
    params = params or {}
    attempt = 0
    while True:
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            code = resp.status_code
            if code == 200:
                return resp.json(), code
            if code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {code}")
            return None, code
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                logging.error(f"❌ request_json exhausted retries: {url} params={params} -> {e}")
                return None, None
            sleep_s = (backoff_base ** attempt) + 0.1 * attempt
            logging.warning(f"⏳ Retry {attempt}/{max_retries} for {url} in {sleep_s:.1f}s ({e})")
            time.sleep(sleep_s)


# ---------------- Downloader ----------------
class UMLSDownloader:
    def __init__(self, full_config: Dict, fresh: bool = False):
        self.full_config = full_config
        self.cfg = full_config["umls"]
        self.fresh = fresh

        self.api_key = os.getenv("UMLS_API_KEY")
        if not self.api_key:
            raise ValueError("UMLS_API_KEY not set in environment")

        # YAML paths
        self.download_url = self.cfg.get("download_url")  # provenance only
        self.raw_file = self.cfg["raw_file"]
        self.cleaned_file = self.cfg["cleaned_file"]
        self.dl_metadata_file = self.cfg["dl_metadata_file"]
        self.transform_metadata_file = self.cfg["transform_metadata_file"]

        # Autosave + resume knobs (with defaults)
        meta_dir = os.path.dirname(os.path.abspath(self.dl_metadata_file)) or "."
        self.autosave_every = int(self.cfg.get("autosave_every", 1000))
        self.page_size = int(self.cfg.get("page_size", 100))
        self.politeness_sleep = float(self.cfg.get("politeness_sleep", 0.1))
        self.checkpoint_file = self.cfg.get("checkpoint_file", os.path.join(meta_dir, "umls_resume.checkpoint.json"))
        self.cuis_cache_file = self.cfg.get("cuis_cache_file", os.path.join(meta_dir, "umls_discovered_cuis.json"))

        # Throttling/verbosity knobs
        self.log_every_pages = int(self.cfg.get("log_every_pages", 10))
        self.checkpoint_every_pages = int(self.cfg.get("checkpoint_every_pages", 10))
        self.suppress_checkpoint_log = bool(self.cfg.get("suppress_checkpoint_log", True))
        self.heartbeat_secs = int(self.cfg.get("heartbeat_secs", 30))

        # Stop conditions
        self.max_pages = int(self.cfg.get("max_pages", 5000))
        self.max_empty_pages = int(self.cfg.get("max_empty_pages", 5))
        self.no_growth_patience_pages = int(self.cfg.get("no_growth_patience_pages", 300))

        # REST version: use "current"; we’ll record the real release via Releases API
        self.version = "current"

        # State
        self.release_info = {}
        self.discovered_cuis: List[Tuple[str, str]] = []  # (cui, name)
        self.processed_records: List[dict] = []
        self.processed_cuis: set = set()
        self.resume_state = {
            "stage": "discover",     # or "process"
            "page": 1,
            "discovered_count": 0,
            "processed_count": 0,
            "last_saved_at": None,
            "release": None
        }

        # heartbeat timer
        self._last_hb = 0.0
        self._run_t0 = time.time()

        # Try to resume (or clear if fresh)
        self._load_existing_outputs()

    # -------- Version / release tracking --------
    def fetch_current_release(self):
        params = {"releaseType": "umls-full-release", "current": "true"}
        data, code = request_json(RELEASES_API, params=params, timeout=20)
        info = None
        if isinstance(data, dict) and data.get("releases"):
            info = data["releases"][0]
        elif isinstance(data, list) and data:
            info = data[0]
        self.release_info = info or {}
        self.resume_state["release"] = self.release_info
        logging.info(f"📦 UMLS release info: {json.dumps(self.release_info, indent=2)}")

    # -------- Resume helpers --------
    def _load_existing_outputs(self):
        if self.fresh:
            for p in [self.raw_file, self.cleaned_file, self.checkpoint_file, self.cuis_cache_file]:
                if p and os.path.exists(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass
            logging.info("🧹 Fresh run requested: cleared caches/checkpoints/outputs.")
            return

        # previous RAW JSON (processed rows)
        if os.path.exists(self.raw_file):
            try:
                with open(self.raw_file, "r") as f:
                    self.processed_records = json.load(f) or []
                for r in self.processed_records:
                    c = r.get("cui")
                    if c:
                        self.processed_cuis.add(c)
                logging.info(f"🔁 Found existing RAW JSON with {len(self.processed_records)} rows.")
            except Exception as e:
                logging.warning(f"Couldn't read raw_file: {e}")

        # discovered CUIs cache
        if os.path.exists(self.cuis_cache_file):
            try:
                with open(self.cuis_cache_file, "r") as f:
                    arr = json.load(f) or []
                if arr and isinstance(arr[0], dict):
                    self.discovered_cuis = [(d.get("cui"), d.get("name")) for d in arr if d.get("cui")]
                else:
                    self.discovered_cuis = [(a[0], a[1]) for a in arr if a and len(a) >= 1]
                logging.info(f"🔁 Found CUIs cache with {len(self.discovered_cuis)} items.")
            except Exception as e:
                logging.warning(f"Couldn't read cuis_cache_file: {e}")

        # checkpoint
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, "r") as f:
                    cp = json.load(f)
                if isinstance(cp, dict):
                    self.resume_state.update(cp)
                logging.info(f"🔁 Loaded checkpoint: {json.dumps(self.resume_state, indent=2)}")
            except Exception as e:
                logging.warning(f"Couldn't read checkpoint: {e}")

    def _save_checkpoint(self, quiet: Optional[bool] = None):
        os.makedirs(os.path.dirname(self.checkpoint_file), exist_ok=True)
        self.resume_state["last_saved_at"] = datetime.now().isoformat()
        with open(self.checkpoint_file, "w") as f:
            json.dump(self.resume_state, f, indent=2)
        if quiet is None:
            quiet = self.suppress_checkpoint_log
        (logging.debug if quiet else logging.info)(
            f"💾 Checkpoint saved → {self.checkpoint_file}"
        )

    def _autosave_outputs(self):
        # JSON (atomic-ish)
        os.makedirs(os.path.dirname(self.raw_file), exist_ok=True)
        tmp = self.raw_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.processed_records, f, indent=2)
        os.replace(tmp, self.raw_file)

        # CSV
        os.makedirs(os.path.dirname(self.cleaned_file), exist_ok=True)
        df = pd.DataFrame(self.processed_records)
        tmp_csv = self.cleaned_file + ".tmp"
        df.to_csv(tmp_csv, index=False)
        os.replace(tmp_csv, self.cleaned_file)

        self._save_checkpoint(quiet=True)
        logging.info(f"💾 Autosaved {len(self.processed_records)} rows → {self.raw_file} / {self.cleaned_file}")

    # -------- Heartbeat --------
    def _heartbeat(self, message: str):
        now = time.time()
        if (now - self._last_hb) >= self.heartbeat_secs:
            elapsed = now - self._run_t0
            logging.info(f"{message} | elapsed {int(elapsed)}s")
            self._last_hb = now

    # -------- Orchestrator --------
    def run(self):
        start_ts = datetime.now().isoformat()
        self.fetch_current_release()

        # Skip if release is unchanged and raw file already exists
        old_release = {}
        if os.path.exists(self.dl_metadata_file):
            try:
                with open(self.dl_metadata_file) as f:
                    old_meta = json.load(f)
                old_release = old_meta.get("release_info", {})
            except Exception:
                pass
        cur_ver = self.release_info.get("releaseVersion") or self.release_info.get("releaseDate")
        old_ver = old_release.get("releaseVersion") or old_release.get("releaseDate")
        if cur_ver and old_ver and cur_ver == old_ver and os.path.exists(self.raw_file):
            logging.info(f"Skipping UMLS download — release unchanged ({cur_ver})")
            meta = {
                "timestamp_start": start_ts,
                "timestamp_end": datetime.now().isoformat(),
                "rest_version": self.version,
                "release_info": self.release_info,
                "status": "skipped",
                "raw_file": self.raw_file,
                "cleaned_file": self.cleaned_file,
            }
            os.makedirs(os.path.dirname(self.dl_metadata_file), exist_ok=True)
            with open(self.dl_metadata_file, "w") as f:
                json.dump(meta, f, indent=2)
            return

        # 1) discover CUIs
        if self.resume_state["stage"] == "discover" and not self.discovered_cuis:
            logging.info(f"▶️ Starting discovery at page {self.resume_state.get('page', 1)}")
            self.discover_disease_cuis()
        else:
            logging.info("⏭️ Discovery skipped (have cached CUIs).")

        # 2) process CUIs
        self.resume_state["stage"] = "process"
        self._save_checkpoint(quiet=True)
        logging.info(f"▶️ Starting processing at index {self.resume_state.get('processed_count', 0)} "
                     f"with {len(self.discovered_cuis)} discovered CUIs and "
                     f"{len(self.processed_records)} already processed.")
        self.process_discovered_cuis()

        # 3) metadata
        meta = {
            "timestamp_start": start_ts,
            "timestamp_end": datetime.now().isoformat(),
            "rest_version": self.version,
            "release_info": self.release_info,
            "records": len(self.processed_records),
            "raw_file": self.raw_file,
            "cleaned_file": self.cleaned_file,
            "download_url_provenance": self.download_url,
            "checkpoint_file": self.checkpoint_file,
            "cuis_cache_file": self.cuis_cache_file
        }
        os.makedirs(os.path.dirname(self.dl_metadata_file), exist_ok=True)
        with open(self.dl_metadata_file, "w") as f:
            json.dump(meta, f, indent=2)
        with open(self.transform_metadata_file, "w") as f:
            json.dump(meta, f, indent=2)

        logging.info(f"✅ Done. {len(self.processed_records)} rows. "
                     f"Metadata → {self.dl_metadata_file} / {self.transform_metadata_file}")

    # -------- Step 1: discover --------
    def discover_disease_cuis(self):
        """
        Iterate /search/current for 'disease' term and stop robustly:
          - break on official sentinel page (ui='NONE')
          - or after N consecutive empty/bad pages
          - or after K pages of no growth in discovered CUIs
          - or after a hard max_pages cap
        """
        page = int(self.resume_state.get("page", 1))

        empty_streak = 0
        no_growth_streak = 0
        last_total = len(self.discovered_cuis)

        while True:
            # hard cap
            if page > self.max_pages:
                self._save_checkpoint(quiet=False)
                logging.info(f"🛑 Discovery stopped at hard cap max_pages={self.max_pages}. "
                             f"Total discovered: {len(self.discovered_cuis)}")
                break

            params = {
                "apiKey": self.api_key,
                "string": "disease",
                "pageNumber": page,
                "pageSize": self.page_size
            }
            url = f"{SEARCH_BASE}/search/{self.version}"
            data, code = request_json(url, params=params, timeout=40)

            if not data or "result" not in data:
                empty_streak += 1
                self._heartbeat(f"⏳ Discovering… page {page} | total discovered: {len(self.discovered_cuis)}")
                if page % self.checkpoint_every_pages == 0:
                    self._save_checkpoint(quiet=True)
                if empty_streak >= self.max_empty_pages:
                    self._save_checkpoint(quiet=False)
                    logging.info(f"🛑 Discovery stopped after {empty_streak} consecutive empty/bad pages "
                                 f"(last code={code}). Total discovered: {len(self.discovered_cuis)}")
                    break
                page += 1
                continue

            results = (data.get("result") or {}).get("results") or []

            # official sentinel page?
            if len(results) == 1 and results[0].get("ui") == "NONE":
                self._save_checkpoint(quiet=False)
                logging.info(f"🔎 Discovery complete via sentinel. CUIs discovered: {len(self.discovered_cuis)}")
                break

            added = 0
            for item in results:
                cui = item.get("ui")
                name = item.get("name", "")
                if not cui or not cui.startswith("C"):
                    continue
                self.discovered_cuis.append((cui, name))
                added += 1

            total_found = len(self.discovered_cuis)

            # growth tracking
            if total_found == last_total:
                no_growth_streak += 1
            else:
                no_growth_streak = 0
                last_total = total_found

            # persist cache + checkpoint occasionally
            os.makedirs(os.path.dirname(self.cuis_cache_file), exist_ok=True)
            with open(self.cuis_cache_file, "w") as f:
                json.dump([{"cui": c, "name": n} for c, n in self.discovered_cuis], f, indent=2)
            if page % self.checkpoint_every_pages == 0:
                self._save_checkpoint(quiet=True)

            # log some progress and heartbeat
            if (page % self.log_every_pages == 0) or page == 1:
                logging.info(f"📄 Page {page}: +{added} | total discovered: {total_found}")
            else:
                logging.debug(f"Page {page}: +{added} | total discovered: {total_found}")
            self._heartbeat(f"⏳ Discovering… page {page} | total discovered: {total_found}")

            # stop if we’ve had no growth for too long
            if no_growth_streak >= self.no_growth_patience_pages:
                self._save_checkpoint(quiet=False)
                logging.info(f"🛑 Discovery stopped after {no_growth_streak} no-growth pages "
                             f"(still at {total_found}).")
                break

            # advance
            self.resume_state["page"] = page + 1
            self.resume_state["discovered_count"] = total_found
            page += 1

    # -------- Step 2: process --------
    def _concept_semtypes(self, cui: str) -> List[str]:
        url = f"{SEARCH_BASE}/content/{self.version}/CUI/{cui}"
        data, code = request_json(url, params={"apiKey": self.api_key}, timeout=40)
        if not data:
            return []
        sems = data.get("result", {}).get("semanticTypes", []) or []
        return [s.get("name", "") for s in sems if s.get("name")]

    def _resolve_related_id(self, related_id_url: str) -> Tuple[Optional[str], Optional[str]]:
        data, code = request_json(related_id_url, params={"apiKey": self.api_key}, timeout=40)
        if not data:
            return None, None
        res = data.get("result", {}) or {}
        cui = res.get("ui")
        xref = None
        root = res.get("rootSource")
        code = res.get("code")
        if root and code:
            xref = f"{root}:{code}"
        return cui, xref

    def _fetch_relations(self, cui: str) -> Tuple[List[str], List[str], List[str], List[str]]:
        url = f"{SEARCH_BASE}/content/{self.version}/CUI/{cui}/relations"
        data, code = request_json(url, params={"apiKey": self.api_key}, timeout=40)
        if not data:
            logging.debug(f"relations empty for {cui}")
            return [], [], [], []

        rels = data.get("result", []) or []
        parents, children, parent_xrefs, child_xrefs = [], [], [], []
        for r in rels:
            label = r.get("relationLabel")
            related_url = r.get("relatedId")
            if not related_url:
                continue
            rcui, xref = self._resolve_related_id(related_url)
            if label == "PAR":
                if rcui: parents.append(rcui)
                if xref: parent_xrefs.append(xref)
            elif label == "CHD":
                if rcui: children.append(rcui)
                if xref: child_xrefs.append(xref)
        return parents, children, parent_xrefs, child_xrefs

    def process_discovered_cuis(self):
        # initialize processed set from existing records
        if not self.processed_cuis and self.processed_records:
            for r in self.processed_records:
                c = r.get("cui")
                if c:
                    self.processed_cuis.add(c)

        processed_since_save = 0
        start_idx = int(self.resume_state.get("processed_count", 0))

        total_disc = len(self.discovered_cuis)

        for idx, (cui, name) in enumerate(self.discovered_cuis):
            if idx < start_idx:
                continue
            if cui in self.processed_cuis:
                continue

            semtypes = self._concept_semtypes(cui)
            # Keep strictly "Disease or Syndrome" (exact match)
            if not any(st == "Disease or Syndrome" for st in semtypes):
                self.resume_state["processed_count"] = idx + 1
                # periodic quiet checkpoint to preserve position
                if (idx + 1) % max(1, (self.autosave_every // 2)) == 0:
                    self._save_checkpoint(quiet=True)
                # heartbeat even when skipping
                self._heartbeat(f"⏳ Processing… scanned {idx+1}/{total_disc} | kept {len(self.processed_records)}")
                continue

            parents, children, parent_xrefs, child_xrefs = self._fetch_relations(cui)
            row = {
                "cui": cui,
                "name": name,
                "semantic_types": "|".join(sorted(semtypes)),
                "parents": "|".join(parents),
                "children": "|".join(children),
                "parent_xrefs": "|".join(parent_xrefs),
                "child_xrefs": "|".join(child_xrefs),
            }
            self.processed_records.append(row)
            self.processed_cuis.add(cui)
            processed_since_save += 1
            self.resume_state["processed_count"] = idx + 1

            if (len(self.processed_records) % self.autosave_every == 0) or (processed_since_save >= self.autosave_every):
                self._autosave_outputs()
                logging.info(f"🧮 Processed {len(self.processed_records)} disease CUIs "
                             f"(scanned {idx+1}/{total_disc})")
                processed_since_save = 0

            self._heartbeat(f"⏳ Processing… scanned {idx+1}/{total_disc} | kept {len(self.processed_records)}")
            time.sleep(self.politeness_sleep)

        # final save
        self._autosave_outputs()


# ---------------- CLI ----------------
def main():
    parser = argparse.ArgumentParser(description="UMLS disease downloader (resumable + autosave + throttled logs + heartbeat + robust stop)")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument("-y", "--yes", action="store_true", help="Run without interactive confirmation")
    parser.add_argument("--fresh", action="store_true", help="Ignore caches/checkpoints and start discovery from scratch")
    args = parser.parse_args()

    with open(args.config) as f:
        full_config = yaml.safe_load(f)

    cfg = full_config.get("umls", {})
    setup_logging(cfg.get("log_file", "umls_download.log"), cfg.get("log_level", "INFO"))

    if not args.yes and sys.stdin.isatty():
        try:
            resp = input("🧬 Run UMLS download now? (Y/n): ").strip().lower()
            if resp not in ("", "y", "yes"):
                logging.info("⏭️  Skipping UMLS download as requested.")
                sys.exit(0)
        except EOFError:
            pass

    UMLSDownloader(full_config, fresh=args.fresh).run()


if __name__ == "__main__":
    main()
