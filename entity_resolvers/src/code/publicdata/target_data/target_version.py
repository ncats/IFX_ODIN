#!/usr/bin/env python3
import os, json, glob, logging, re
import pandas as pd
from datetime import datetime
import argparse
import yaml

from publicdata.target_data.download_utils import setup_logging
from publicdata.target_data.shared.pipeline_version import get_pipeline_version

class DownloadCatalogProcessor:
    """
    Collects TARGETS dl_* metadata JSON files into a single CSV.

    YAML section (example):
      download_catalog:
        meta_dir: src/data/publicdata/target_data/metadata
        out_csv:  src/data/publicdata/target_data/metadata/dl_catalog.csv
        metadata_file: src/data/publicdata/target_data/metadata/dl_catalog_run.json
        filename_glob: dl_*_metadata.json
        log_file: src/data/publicdata/target_data/metadata/dl_catalog.log
    """
    def __init__(self, cfg):
        # Allow either 'download_catalog' or 'dl_catalog' section names
        section = "download_catalog" if "download_catalog" in cfg else ("dl_catalog" if "dl_catalog" in cfg else "download_catalog")
        c = cfg.get(section, {})

        self.meta_dir      = c.get("meta_dir", "src/data/publicdata/target_data/metadata")
        self.out_csv       = c.get("out_csv", os.path.join(self.meta_dir, "dl_catalog.csv"))
        self.metadata_file = c.get("metadata_file", os.path.join(self.meta_dir, "dl_catalog_run.json"))
        self.filename_glob = c.get("filename_glob", "dl_*_metadata.json")
        self.log_file      = c.get("log_file")

        setup_logging(self.log_file)
        logging.info("🚀 Initialized DownloadCatalogProcessor")

        now = datetime.now().isoformat(timespec="seconds")
        pv = get_pipeline_version()
        self.metadata = {
            "processor": "DownloadCatalogProcessor",
            "pipeline_version": pv["version_string"],
            "pipeline_git_commit": pv["git_commit"],
            "createdAt": now,
            "updatedAt": now,
            "timestamp": {"start": now, "end": None},
            "config": {
                "meta_dir": self.meta_dir,
                "out_csv": self.out_csv,
                "filename_glob": self.filename_glob
            },
            "processing_steps": [],
            "inputs": [],
            "outputs": []
        }
        self.df = None

    def add_metadata_step(self, step_name, description, **details):
        entry = {
            "step_name": step_name,
            "description": description,
            "performed_at": datetime.now().isoformat(timespec="seconds"),
            **details
        }
        self.metadata.setdefault("processing_steps", []).append(entry)
        logging.info("Metadata step: %s – %s", step_name, description)

    @staticmethod
    def _coalesce(dct, *keys):
        for k in keys:
            if isinstance(dct, dict) and k in dct and dct[k] is not None:
                return dct[k]
        return None

    @staticmethod
    def _norm_dt(s):
        """Normalize a datetime string (ISO, HTTP-date, or informal) to ISO format."""
        if not s:
            return None
        s_str = str(s).strip()
        # Pure date (YYYY-MM-DD) — keep as date, don't add T00:00:00
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s_str):
            return s_str
        # ISO format with time component
        try:
            return datetime.fromisoformat(s_str.replace("Z", "+00:00")).isoformat()
        except Exception:
            pass
        # HTTP-date: "Tue, 10 Feb 2026 09:35:12 GMT"
        from email.utils import parsedate_to_datetime
        try:
            return parsedate_to_datetime(s_str).strftime("%Y-%m-%d")
        except Exception:
            pass
        # Informal: "28-January-2026"
        for fmt in ("%d-%B-%Y", "%B %d, %Y", "%d %B %Y"):
            try:
                return datetime.strptime(s_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return s_str

    @staticmethod
    def _safe_load_json(path, max_bytes=50_000_000):
        """Load JSON, but skip files larger than max_bytes to avoid OOM/segfault."""
        size = os.path.getsize(path)
        if size > max_bytes:
            # Read only the first chunk and parse what we can
            logging.warning("Metadata file %s is %.1f GB — reading first %d bytes only",
                            path, size / 1e9, max_bytes)
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                raw = f.read(max_bytes)
            # Try to parse a truncated-but-usable top-level object
            # by closing any open strings/braces
            for trim in (raw, raw + '"}', raw + '"}'*2, raw + '"}'*3):
                try:
                    return json.loads(trim)
                except json.JSONDecodeError:
                    continue
            # Last resort: manually extract top-level keys
            data = {}
            for key in ("name", "version", "release_date", "url", "download_url",
                        "download_start", "download_end", "updated", "status",
                        "archive_size", "decompressed_path"):
                m = re.search(rf'"{key}"\s*:\s*("(?:[^"\\]|\\.)*?"|\d+(?:\.\d+)?|true|false|null)', raw)
                if m:
                    try:
                        data[key] = json.loads(m.group(1))
                    except Exception:
                        data[key] = m.group(1).strip('"')
            return data
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _extract_row(self, path):
        base = os.path.basename(path)
        try:
            data = self._safe_load_json(path)
        except Exception as e:
            return {"metadata_file": base, "status": f"ERROR: {e}"}

        coalesce  = self._coalesce
        downloads = data.get("downloads", []) if isinstance(data.get("downloads"), list) else []
        files_map = data.get("files") if isinstance(data.get("files"), dict) else {}

        # Top-level first (support both old schema "name"/"version" and new "source_name"/"source_version")
        source_name  = coalesce(data, "source_name", "name") or coalesce(data.get("source", {}), "name")
        version      = coalesce(data, "source_version", "version") or coalesce(data.get("source", {}), "version")
        release_date = coalesce(data, "release_date")
        url          = (coalesce(data, "url", "download_url")
                        or coalesce(data.get("source", {}), "url"))

        # Fallbacks from downloads[]
        if downloads:
            if not source_name:
                source_name = downloads[0].get("name")
            if not url:
                url = downloads[0].get("url") or downloads[0].get("download_url")
            if not version:
                vers = [d.get("version") for d in downloads if d.get("version")]
                if vers:
                    version = sorted(set(vers))[-1]  # YYYY-MM-DD sorts lexically

        # NEW: final URL fallback from legacy "files" dict (its keys are full URLs)
        if not url and files_map:
            try:
                url = next(iter(files_map.keys()))
            except StopIteration:
                pass

        # --- NodeNorm-specific friendly naming (override generic "NodeNorm") ---
        def infer_nodenorm_name():
            # Build a text haystack: URL(s), download names, raw_jsonl path, and filename
            names = "|".join([d.get("name", "") for d in downloads if isinstance(d, dict)])
            url_for_infer = url or (downloads[0].get("url") if downloads else None) \
                            or (downloads[0].get("download_url") if downloads else None)
            raw = str(data.get("raw_jsonl", ""))
            hay = "|".join([str(url_for_infer or ""), names, raw, base]).lower()

            is_nn = ("nodenorm" in hay) or ("babel_outputs" in hay)
            if not is_nn:
                return None
            if ("gene.txt" in hay) or ("/gene" in hay) or ("genes" in hay):
                return "NodeNorm Genes"
            if ("protein.txt" in hay) or ("/protein" in hay) or ("proteins" in hay):
                return "NodeNorm Proteins"
            return "NodeNorm"

        inferred = infer_nodenorm_name()
        if (not source_name) or (str(source_name).strip().lower() == "nodenorm" and inferred):
            source_name = inferred

        # Normalize common source names for display
        _NAME_MAP = {"refseq": "RefSeq", "uniprot": "UniProt", "hgnc": "HGNC"}
        if source_name and str(source_name).strip().lower() in _NAME_MAP:
            source_name = _NAME_MAP[str(source_name).strip().lower()]

        updated_flag    = coalesce(data, "updated")
        update_detected = coalesce(data, "update_detected")
        status          = coalesce(data, "status")
        updated_any = (updated_flag if isinstance(updated_flag, bool)
                    else update_detected if isinstance(update_detected, bool)
                    else (str(status).lower() == "updated" if status is not None else None))
        # Fallback: check downloads[] for updated flag
        if updated_any is None and downloads:
            dl_updated = [d.get("updated") for d in downloads if isinstance(d.get("updated"), bool)]
            if dl_updated:
                updated_any = any(dl_updated)

        archive_size            = data.get("archive_size")
        total_size_bytes        = data.get("total_size_bytes")
        compressed_size_bytes   = data.get("compressed_size_bytes")
        decompressed_size_bytes = data.get("decompressed_size_bytes")

        # Fallback sizes from downloads[]
        if downloads:
            if not compressed_size_bytes:
                csz = [d.get("compressed_size_bytes") for d in downloads if d.get("compressed_size_bytes")]
                if csz:
                    compressed_size_bytes = csz[0]
            if not decompressed_size_bytes:
                dsz = [d.get("decompressed_size_bytes") for d in downloads if d.get("decompressed_size_bytes")]
                if dsz:
                    decompressed_size_bytes = dsz[0]

        # outputs aggregation
        outputs = []
        for k in ("decompressed_path", "decompressed_file", "output_path", "raw_jsonl"):
            if data.get(k):
                outputs.append(f"{k}:{data[k]}")
        if isinstance(data.get("outputs"), list):
            for o in data["outputs"]:
                out = (o.get("output") or o.get("path"))
                if out:
                    outputs.append(f"output:{out}")
        if downloads:
            for o in downloads:
                out = (o.get("compressed_path") or o.get("decompressed_path"))
                if out:
                    outputs.append(f"path:{out}")
        outputs_str = " | ".join(outputs) if outputs else None

        # Release date fallback from headers
        release_last_modified = None
        if downloads:
            release_last_modified = downloads[0].get("remote_headers", {}).get("Last-Modified")
        if not release_date and files_map:
            try:
                # use the newest Last-Modified across files_map
                lm_vals = [v.get("Last-Modified") for v in files_map.values() if isinstance(v, dict)]
                lm_vals = [v for v in lm_vals if v]
                if lm_vals:
                    release_last_modified = max(lm_vals)
            except Exception:
                pass

        # --- Resolve release_date ---
        effective_release = release_date or release_last_modified
        # If version looks like a date (YYYY-MM-DD) and release_date is empty,
        # use version as the release_date (HGNC/NCBI use Last-Modified as version)
        if not effective_release and version:
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(version)):
                effective_release = version

        return {
            "metadata_file": base,
            "source_name": source_name,
            "version": version,
            "release_date": self._norm_dt(effective_release),
            "download_start": self._norm_dt((coalesce(data, "download_start")
                                            or coalesce(data, "downloaded_at")
                                            or coalesce(data.get("timestamp", {}), "start"))),
            "download_end":   self._norm_dt((coalesce(data, "download_end")
                                            or coalesce(data.get("timestamp", {}), "end"))),
            "url": url,
            "updated": updated_any,
            "status": status,
            "archive_size": archive_size,
            "total_size_bytes": total_size_bytes,
            "compressed_size_bytes": compressed_size_bytes,
            "decompressed_size_bytes": decompressed_size_bytes,
            "outputs": outputs_str,
        }

    # ── Map dl_X_metadata.json → tf_X_metadata.json ──────────────────────────
    _DL_TO_TF_NAME = {
        "dl_refseq_metadata.json": "tf_refSeq_metadata.json",
    }

    def _collect_transform_stats(self):
        """
        For each dl_*_metadata.json, find the corresponding tf_*_metadata.json
        and extract input/output row counts, duration, and diff file path.
        Returns a dict keyed by dl metadata filename.
        """
        stats = {}
        dl_pattern = os.path.join(self.meta_dir, self.filename_glob)
        for dl_path in sorted(glob.glob(dl_pattern)):
            dl_base = os.path.basename(dl_path)

            # Map dl_ → tf_  (handle special cases via _DL_TO_TF_NAME)
            tf_base = self._DL_TO_TF_NAME.get(dl_base)
            if tf_base is None:
                tf_base = dl_base.replace("dl_", "tf_", 1)
            tf_path = os.path.join(self.meta_dir, tf_base)

            if not os.path.exists(tf_path):
                continue

            try:
                with open(tf_path, "r", encoding="utf-8") as f:
                    tf = json.load(f)
            except Exception:
                continue

            coalesce = self._coalesce

            # --- input rows ---
            tf_input = (coalesce(tf, "num_records_input")
                        or coalesce(tf.get("input", {}), "record_count_raw")
                        or coalesce(tf.get("cleaning", {}), "before")
                        or coalesce(tf.get("record_counts", {}), "after_df1"))

            # --- output rows ---
            tf_output = coalesce(tf, "num_records_output", "records_output")
            if tf_output is None:
                tf_output = coalesce(tf.get("record_counts", {}), "after_merge")
            if tf_output is None:
                tf_output = coalesce(tf.get("output", {}), "record_count")
            if tf_output is None:
                tf_output = coalesce(tf.get("cleaning", {}), "after")
            if tf_output is None:
                tf_output = coalesce(tf.get("summary", {}), "final_rows")
            if tf_output is None:
                # Try last outputs[] entry with a "records" key
                for o in reversed(tf.get("outputs", [])):
                    if isinstance(o, dict) and o.get("records") is not None:
                        tf_output = o["records"]
                        break

            # --- duration ---
            tf_dur = coalesce(tf, "transformation_duration_seconds")
            if tf_dur is None:
                tf_dur = coalesce(tf.get("timestamp", {}), "duration_seconds")

            # --- diff file ---
            tf_diff = coalesce(tf, "diff_file", "diff_csv")
            if tf_diff is None:
                tf_diff = coalesce(tf.get("summary", {}), "entity_diff_file")
            if tf_diff is None:
                for o in tf.get("outputs", []):
                    if isinstance(o, dict) and "diff" in str(o.get("name", "")).lower():
                        tf_diff = o.get("path")
                        break

            # --- entity diff summary from transform ---
            tf_summary = tf.get("summary", {})
            tf_added = coalesce(tf_summary, "n_added_ids", "n_added")
            tf_removed = coalesce(tf_summary, "n_removed_ids", "n_removed")
            tf_changes = coalesce(tf_summary, "n_field_changes")

            stats[dl_base] = {
                "tf_input_rows": tf_input,
                "tf_output_rows": tf_output,
                "tf_added_ids": tf_added,
                "tf_removed_ids": tf_removed,
                "tf_field_changes": tf_changes,
                "tf_duration_secs": tf_dur,
                "tf_diff_file": tf_diff,
            }
        return stats

    def collect(self):
        self.add_metadata_step("collect", f"Scanning {self.meta_dir} for {self.filename_glob}")
        paths = sorted(glob.glob(os.path.join(self.meta_dir, self.filename_glob)))
        self.metadata.setdefault("inputs", []).append({
            "name": "metadata_json_files",
            "path": self.meta_dir,
            "pattern": self.filename_glob,
            "count": len(paths)
        })
        rows = [self._extract_row(p) for p in paths]
        df = pd.DataFrame(rows)

        # Merge transform stats
        tf_stats = self._collect_transform_stats()
        if tf_stats:
            tf_df = pd.DataFrame.from_dict(tf_stats, orient="index")
            tf_df.index.name = "metadata_file"
            tf_df = tf_df.reset_index()
            df = df.merge(tf_df, on="metadata_file", how="left")

        df = df.sort_values(by=["source_name", "metadata_file"], na_position="last").reset_index(drop=True)
        self.df = df
        self.add_metadata_step("collect_done", f"Collected {len(df)} rows")

    @staticmethod
    def _human_size(nbytes):
        """Convert bytes to human-readable string."""
        if nbytes is None or (isinstance(nbytes, float) and pd.isna(nbytes)):
            return None
        try:
            nbytes = float(nbytes)
        except (TypeError, ValueError):
            return str(nbytes)
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if abs(nbytes) < 1024:
                return f"{nbytes:.1f} {unit}"
            nbytes /= 1024
        return f"{nbytes:.1f} PB"

    def save(self):
        if self.df is None:
            self.collect()

        df = self.df.copy()

        # Add human-readable size columns
        for col in ("total_size_bytes", "compressed_size_bytes", "decompressed_size_bytes"):
            if col in df.columns:
                hr_col = col.replace("_bytes", "")
                df[hr_col] = df[col].apply(self._human_size)

        # Column order for the output
        CATALOG_COLUMN_ORDER = [
            "source_name", "version", "release_date", "updated", "status",
            # Transform entity diff summary
            "tf_added_ids", "tf_removed_ids", "tf_field_changes",
            # Transform stats
            "tf_input_rows", "tf_output_rows", "tf_duration_secs", "tf_diff_file",
            # Download details
            "download_start", "download_end", "url",
            "total_size", "compressed_size", "decompressed_size",
            "outputs", "metadata_file",
            # raw byte columns kept at end for programmatic use
            "archive_size", "total_size_bytes", "compressed_size_bytes", "decompressed_size_bytes",
        ]
        ordered = [c for c in CATALOG_COLUMN_ORDER if c in df.columns]
        remaining = [c for c in df.columns if c not in ordered]
        df = df[ordered + remaining]

        os.makedirs(os.path.dirname(self.out_csv), exist_ok=True)

        # Try .xlsx first (much better for Excel), fall back to .tsv
        out_xlsx = os.path.splitext(self.out_csv)[0] + ".xlsx"
        wrote_xlsx = False
        try:
            import openpyxl
            with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Download Catalog")
                ws = writer.sheets["Download Catalog"]
                # Auto-fit column widths (cap at 60)
                for col_idx, col_name in enumerate(df.columns, 1):
                    max_len = max(
                        len(str(col_name)),
                        df[col_name].astype(str).str.len().max() if len(df) else 0,
                    )
                    ws.column_dimensions[openpyxl.utils.get_column_letter(col_idx)].width = min(max_len + 2, 60)
            logging.info("Wrote %d rows to %s", len(df), out_xlsx)
            wrote_xlsx = True
        except ImportError:
            logging.info("openpyxl not available — falling back to TSV")
        except Exception as e:
            logging.warning("Failed to write .xlsx (%s) — falling back to TSV", e)

        # Always write .tsv as machine-readable backup
        out_tsv = os.path.splitext(self.out_csv)[0] + ".tsv"
        df.to_csv(out_tsv, index=False, sep="\t")
        logging.info("Wrote %d rows to %s", len(df), out_tsv)

        out_path = out_xlsx if wrote_xlsx else out_tsv
        self.metadata["outputs"].append({
            "name": "dl_catalog",
            "path": out_path,
            "format": "xlsx" if wrote_xlsx else "tsv",
            "records": int(len(df))
        })
        self.metadata["updatedAt"] = datetime.now().isoformat(timespec="seconds")
        self.metadata["timestamp"]["end"] = self.metadata["updatedAt"]

        os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
        with open(self.metadata_file, "w", encoding="utf-8") as f:
            json.dump(self.metadata, f, indent=2)
        logging.info("Saved run metadata to %s", self.metadata_file)

    # ── Manifest generation (consolidated from main.py) ──────────────────────
    def build_manifests(self, full_config):
        """
        Walk the full YAML config to produce source_versions and
        transform_summary manifest files (JSON + CSV).

        This replaces the old build_target_manifests() function from main.py,
        consolidating all version/catalog reporting into this processor.
        """
        manifest_cfg = full_config.get("manifests", {})
        default_dir = os.path.join(self.meta_dir, "")
        source_json = manifest_cfg.get(
            "source_versions_json",
            os.path.join(default_dir, "source_versions_manifest.json"),
        )
        source_csv = manifest_cfg.get(
            "source_versions_csv",
            os.path.join(default_dir, "source_versions_manifest.csv"),
        )
        transform_json = manifest_cfg.get(
            "transform_summary_json",
            os.path.join(default_dir, "transform_summary_manifest.json"),
        )
        transform_csv = manifest_cfg.get(
            "transform_summary_csv",
            os.path.join(default_dir, "transform_summary_manifest.csv"),
        )

        source_rows = []
        transform_rows = []

        for section_key, section_cfg in full_config.items():
            if not isinstance(section_cfg, dict):
                continue

            dl_meta_path = section_cfg.get("dl_metadata_file")
            tf_meta_path = (section_cfg.get("tf_metadata_file")
                           or section_cfg.get("metadata_file"))

            dl_meta = self._load_meta(dl_meta_path)
            tf_meta = self._load_meta(tf_meta_path)

            if dl_meta:
                source_rows.append({
                    "category": "TARGETS",
                    "source_key": section_key,
                    "source_name": dl_meta.get("source_name", section_key),
                    "source_version": (dl_meta.get("source_version")
                                       or dl_meta.get("version")),
                    "download_start": dl_meta.get("download_start"),
                    "download_end": (dl_meta.get("download_end")
                                     or dl_meta.get("timestamp")),
                    "status": dl_meta.get("status"),
                    "updated": dl_meta.get("updated"),
                    "metadata_file": dl_meta_path,
                    "raw_files": (
                        json.dumps([x.get("path") for x in dl_meta.get("outputs", [])])
                        if isinstance(dl_meta.get("outputs"), list) else None
                    ),
                })

            if tf_meta:
                summary = tf_meta.get("summary", {})
                rc = tf_meta.get("record_counts", {})
                ts = tf_meta.get("timestamp", {})
                transform_rows.append({
                    "category": "TARGETS",
                    "source_key": section_key,
                    "transform_start": (ts.get("start") if isinstance(ts, dict)
                                        else tf_meta.get("timestamp")),
                    "transform_end": (ts.get("end") if isinstance(ts, dict)
                                      else tf_meta.get("timestamp")),
                    "final_output": (tf_meta.get("final_output")
                                     or tf_meta.get("output_file")),
                    "archived_output": tf_meta.get("archived_output"),
                    "records_output": (
                        rc.get("after_merge") if isinstance(rc, dict)
                        else (tf_meta.get("records_output")
                              or tf_meta.get("num_records_output")
                              or tf_meta.get("records"))
                    ),
                    "n_added_ids": summary.get("n_added_ids"),
                    "n_removed_ids": summary.get("n_removed_ids"),
                    "n_field_changes": summary.get("n_field_changes"),
                    "entity_diff_file": summary.get("entity_diff_file"),
                    "metadata_file": tf_meta_path,
                })

        pv = get_pipeline_version()
        now_iso = datetime.now().isoformat()

        source_payload = {
            "generated_at": now_iso,
            "pipeline_version": pv["version_string"],
            "pipeline_git_commit": pv["git_commit"],
            "category": "TARGETS",
            "sources": source_rows,
        }
        transform_payload = {
            "generated_at": now_iso,
            "pipeline_version": pv["version_string"],
            "pipeline_git_commit": pv["git_commit"],
            "category": "TARGETS",
            "transforms": transform_rows,
        }

        for p in (source_json, source_csv, transform_json, transform_csv):
            os.makedirs(os.path.dirname(p), exist_ok=True)

        with open(source_json, "w") as f:
            json.dump(source_payload, f, indent=2)
        with open(transform_json, "w") as f:
            json.dump(transform_payload, f, indent=2)

        pd.DataFrame(source_rows).to_csv(source_csv, index=False)
        pd.DataFrame(transform_rows).to_csv(transform_csv, index=False)

        logging.info("Wrote source manifest → %s", source_json)
        logging.info("Wrote transform manifest → %s", transform_json)

        self.metadata["outputs"].extend([
            {"name": "source_versions_manifest", "path": source_json,
             "records": len(source_rows)},
            {"name": "transform_summary_manifest", "path": transform_json,
             "records": len(transform_rows)},
        ])
        self.add_metadata_step(
            "build_manifests",
            f"Generated source ({len(source_rows)} rows) and transform "
            f"({len(transform_rows)} rows) manifests",
        )

    @staticmethod
    def _load_meta(path_str):
        """Load a metadata file (JSON or YAML) and return its contents, or None."""
        if not path_str:
            return None
        if not os.path.exists(path_str):
            return None
        try:
            if path_str.endswith((".yaml", ".yml")):
                with open(path_str) as f:
                    return yaml.safe_load(f)
            with open(path_str) as f:
                return json.load(f)
        except Exception:
            return None

    def run(self, full_config=None):
        self.collect()
        self.save()
        if full_config is not None:
            self.build_manifests(full_config)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect TARGETS dl_* metadata into one CSV + manifests")
    parser.add_argument("--config", default="config/targets_config.yaml")
    parser.add_argument("--skip-manifests", action="store_true",
                        help="Only build the dl_catalog, skip source/transform manifests")
    args = parser.parse_args()

    cfg = yaml.safe_load(open(args.config))
    proc = DownloadCatalogProcessor(cfg)
    proc.run(full_config=None if args.skip_manifests else cfg)