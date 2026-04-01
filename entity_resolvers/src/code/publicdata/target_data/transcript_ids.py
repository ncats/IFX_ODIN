#!/usr/bin/env python
"""
transcript_ids.py – Post-process transcript provenance mappings:
  • load the merged transcript CSV
  • preserve old IFX IDs and mint new ones for new rows
  • generate NCATS Transcript IDs
  • record detailed metadata for each step
"""

import os
import json
import yaml
import logging
import argparse
import secrets
import pandas as pd
from datetime import datetime
from logging.handlers import RotatingFileHandler
from publicdata.target_data.shared.output_versioning import save_versioned_output
from publicdata.target_data.download_utils import setup_logging

class TranscriptDataProcessor:
    def __init__(self, cfg):
        from datetime import datetime
        c = cfg['transcript_ids']

        self.config      = c
        self.source_file = c['source_file']

        # Normalize output to .tsv early so reads/writes agree
        out = c['transcript_ids_path']
        self.legacy_csv_path = out if out.endswith(".csv") else None
        if out.endswith(".csv"):
            out = out.replace(".csv", ".tsv")
        self.output_path = out

        self.metadata_path = c['metadata_file']
        log_file = c.get('log_file', self.metadata_path.replace('.json', '.log'))

        # Runtime holders
        self.dataset  = None
        self.ncats_df = None

        # ✅ Initialize metadata containers
        now_iso = datetime.now().isoformat(timespec='seconds')
        self.metadata = {
            "processor": "TranscriptDataProcessor",
            "createdAt": now_iso,
            "updatedAt": now_iso,
            "config": {
                "source_file": self.source_file,
                "output_path": self.output_path
            },
            "processing_steps": [],
            "inputs": [],
            "outputs": []
        }

        logging.info("🚀 Starting TranscriptDataProcessor")

    def add_metadata_step(self, step_name, description, records=None, duration=None, path=None):
        entry = {
            "step_name": step_name,
            "description": description,
            "performed_at": datetime.now().isoformat()
        }
        if records is not None:
            entry["records"] = records
        if duration is not None:
            entry["duration_seconds"] = duration
        if path is not None:
            entry["output_path"] = path
        self.metadata["processing_steps"].append(entry)
        logging.info("Added metadata step: %s – %s", step_name, description)

    def load_dataset(self):
        t0 = datetime.now()
        logging.info("STEP 1: load_dataset")
        keep = [
            'ensembl_gene_id','refseq_ncbi_id','ensembl_transcript_name',
            'symbol','ensembl_transcript_id','ensembl_transcript_id_version',
            'ensembl_transcript_type','ensembl_trans_bp_start','ensembl_trans_bp_end',
            'ensembl_trans_length','ensembl_transcript_tsl','ensembl_canonical',
            'ensembl_refseq_NM','ensembl_refseq_MANEselect',
            'refseq_status','refseq_rna_id',
            'Ensembl_Transcript_ID_Provenance','RefSeq_Provenance'
        ]
        try:
            df = pd.read_csv(self.source_file, low_memory=False)
            df = df.reindex(columns=keep)
            logging.info("Loaded %d rows from %s", len(df), self.source_file)
        except Exception as e:
            logging.error("Error loading dataset: %s", e)
            df = pd.DataFrame(columns=keep)

        duration = (datetime.now() - t0).total_seconds()
        self.df = df
        self.add_metadata_step("Dataset Loading",
                               f"Loaded & filtered dataset from {self.source_file}",
                               records=len(df), duration=duration)
        self.metadata.setdefault("outputs", []).append({
            "name": "raw_transcript_data",
            "path": self.source_file,
            "records": len(df)
        })

    def _read_table(self, path):
        """
        Read CSV/TSV by sniffing delimiter or using extension.
        """
        sep = '\t' if path.endswith('.tsv') else None
        if sep is None:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                sample = f.read(8192)
            sep = '\t' if sample.count('\t') > sample.count(',') else ','
        return pd.read_csv(path, dtype=str, sep=sep, low_memory=False)
    
    def generate_transcript_ids(self):
        """
        Generate stable ncats_transcript_id values, reusing any existing IDs from a prior run.
        Falls back to loading the source file if load_dataset() didn't set self.dataset.
        """
        from datetime import datetime
        import hashlib
        import os

        t0 = datetime.now()
        logging.info("STEP 2: generate_transcript_ids")

        # --- Ensure dataset is present (lazy load or alias pickup) ---
        if getattr(self, 'dataset', None) is None:
            # Try common alternate attributes first
            for alt in ('df', 'input_df', 'mapping_df', 'source_df', 'transcript_df'):
                val = getattr(self, alt, None)
                if isinstance(val, pd.DataFrame):
                    self.dataset = val
                    logging.info("Detected dataset from self.%s with %d rows", alt, len(self.dataset))
                    break
            else:
                # Fallback: read directly from the configured source file
                logging.warning("Dataset not set by load_dataset(); reading from %s", self.source_file)
                self.dataset = self._read_table(self.source_file)
                logging.info("Loaded %d rows directly from source_file", len(self.dataset))

        # --- Determine key columns ---
        configured_keys = self.config.get('id_key_columns') or []
        if configured_keys:
            keys = [k for k in configured_keys if k in self.dataset.columns]
        else:
            candidates = [
                'ensembl_transcript_id_version',
                'ensembl_transcript_id',
                'refseq_transcript_id_version',
                'refseq_transcript_id',
                'uniprot_isoform_id',
                'gene_id'
            ]
            keys = [k for k in candidates if k in self.dataset.columns]

        if not keys:
            keys = [self.dataset.columns[0]]
            logging.warning("No configured key columns found; falling back to %s", keys)

        # --- Unique current key set ---
        current = (
            self.dataset[keys]
            .copy()
            .drop_duplicates()
            .reset_index(drop=True)
        )

        # --- Load prior IDs (prefer .tsv, fall back to legacy .csv) ---
        candidates = []
        if os.path.exists(self.output_path):
            candidates.append(self.output_path)
        if getattr(self, 'legacy_csv_path', None) and os.path.exists(self.legacy_csv_path):
            candidates.append(self.legacy_csv_path)

        if candidates:
            prior_path = candidates[0]
            old = self._read_table(prior_path)
            for col in ["ncats_transcript_id", "createdAt", "updatedAt"]:
                if col not in old.columns:
                    old[col] = None

            missing = [k for k in keys if k not in old.columns]
            if missing:
                logging.warning(
                    "Existing ID file %s missing key columns %s; ignoring legacy IDs.",
                    prior_path, missing
                )
                old = pd.DataFrame(columns=keys + ["ncats_transcript_id", "createdAt", "updatedAt"])
            else:
                old = old[keys + ["ncats_transcript_id", "createdAt", "updatedAt"]].drop_duplicates(subset=keys)
            logging.info("Loaded %d existing IFX transcript IDs from %s", len(old), prior_path)
        else:
            old = pd.DataFrame(columns=keys + ["ncats_transcript_id", "createdAt", "updatedAt"])
            logging.info("No existing IFX transcript IDs found")

        # --- Merge + assign IDs where missing ---
        merged = current.merge(old, on=keys, how='left')

        now_iso = datetime.now().isoformat(timespec='seconds')
        need_id = merged['ncats_transcript_id'].isna() | (merged['ncats_transcript_id'] == '')
        if need_id.any():
            def make_id(row):
                vals = [str(row[k]) if pd.notna(row[k]) else '' for k in keys]
                digest = hashlib.md5("|".join(vals).encode()).hexdigest()[:8].upper()
                return f"IFXTranscript:{digest}"

            merged.loc[need_id, 'ncats_transcript_id'] = merged.loc[need_id].apply(make_id, axis=1)
            merged.loc[need_id, 'createdAt'] = now_iso
            merged.loc[need_id, 'updatedAt'] = now_iso

        merged['updatedAt'] = merged['updatedAt'].fillna(now_iso)

        # --- Finalize ---
        self.ncats_df = merged[keys + ["ncats_transcript_id", "createdAt", "updatedAt"]]

        if hasattr(self, "add_metadata_step"):
            self.add_metadata_step(
                "Transcript ID Generation",
                f"Generated/merged {len(self.ncats_df):,} transcript IDs using keys={keys}"
            )

        dt = (datetime.now() - t0).total_seconds()
        logging.info("✅ Generated %d transcript IDs in %.2fs", len(self.ncats_df), dt)

    def save_transcript_ids(self):
        t0 = datetime.now()
        logging.info("STEP 3: save_transcript_ids")

        # Enrich output with context columns from source dataset
        source_df = getattr(self, 'df', getattr(self, 'dataset', None))
        if source_df is not None:
            context_cols = [
                'ensembl_gene_id', 'symbol', 'ensembl_transcript_type',
                'ensembl_trans_length', 'ensembl_transcript_tsl', 'ensembl_canonical',
                'ensembl_refseq_MANEselect', 'refseq_rna_id', 'refseq_ncbi_id',
                'refseq_status', 'Ensembl_Transcript_ID_Provenance', 'RefSeq_Provenance',
            ]
            available = [c for c in context_cols if c in source_df.columns]
            if available:
                # Determine key columns used for ID generation
                keys = [c for c in self.ncats_df.columns
                        if c not in ('ncats_transcript_id', 'createdAt', 'updatedAt')]
                # Merge context from source, keeping one row per key set
                ctx = source_df[keys + available].drop_duplicates(subset=keys)
                enriched = self.ncats_df.merge(ctx, on=keys, how='left')
                logging.info("Enriched transcript IDs with %d context columns: %s",
                             len(available), available)
            else:
                enriched = self.ncats_df
        else:
            enriched = self.ncats_df

        # Reorder columns: ncats_transcript_id first, then key IDs, context, provenance, timestamps
        TRANSCRIPT_COLUMN_ORDER = [
            'ncats_transcript_id', 'ensembl_transcript_id_version', 'ensembl_transcript_id',
            'ensembl_gene_id', 'symbol', 'ensembl_transcript_type',
            'ensembl_trans_length', 'ensembl_transcript_tsl', 'ensembl_canonical',
            'ensembl_refseq_MANEselect', 'refseq_rna_id', 'refseq_ncbi_id', 'refseq_status',
            'Ensembl_Transcript_ID_Provenance', 'RefSeq_Provenance',
            'createdAt', 'updatedAt',
        ]
        ordered = [c for c in TRANSCRIPT_COLUMN_ORDER if c in enriched.columns]
        remaining = [c for c in enriched.columns if c not in ordered]
        enriched = enriched[ordered + remaining]

        ver_result = save_versioned_output(
            df=enriched,
            output_path=self.output_path,
            id_col="ncats_transcript_id",
            sep="\t",
            output_kind="resolved_node_ids",
        )
        self.metadata["output_versioning"] = ver_result

        duration = (datetime.now() - t0).total_seconds()
        logging.info("Transcript IDs saved to %s", self.output_path)
        self.add_metadata_step("Save Transcript IDs",
                            f"Saved {len(enriched)} rows to {self.output_path}",
                            records=len(enriched),
                            duration=duration,
                            path=self.output_path)

    def save_metadata(self):
        """Write run metadata JSON safely."""
        from datetime import datetime
        import os, json

        # Ensure containers exist
        self.metadata.setdefault("processing_steps", [])
        self.metadata.setdefault("inputs", [])
        self.metadata.setdefault("outputs", [])
        self.metadata.setdefault("config", {})
        self.metadata.setdefault("timestamp", {})

        # Close out timestamps
        self.metadata["updatedAt"] = datetime.now().isoformat(timespec='seconds')
        self.metadata["timestamp"].setdefault("start", self.metadata.get("createdAt"))
        self.metadata["timestamp"]["end"] = datetime.now().isoformat(timespec='seconds')

        # Optional: small summary counts
        try:
            self.metadata["summary"] = {
                "num_rows_input": int(getattr(self, "dataset", getattr(self, "df", pd.DataFrame())).shape[0]),
                "num_rows_output": int(getattr(self, "ncats_df", pd.DataFrame()).shape[0]),
            }
        except Exception:
            pass

        # Ensure dir exists and write file
        os.makedirs(os.path.dirname(self.metadata_path), exist_ok=True)
        with open(self.metadata_path, "w", encoding="utf-8") as f:
            json.dump(self.metadata, f, indent=2)
        logging.info("📝 Metadata written to %s", self.metadata_path)


    def run(self):
        self.load_dataset()
        self.generate_transcript_ids()
        self.save_transcript_ids()
        self.save_metadata()
        logging.info("🎉 TranscriptDataProcessor complete!")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Post-process transcript provenance mappings")
    p.add_argument("--config", type=str,
               default="config/targets_config.yaml",
               help="YAML config (default: config/targets_config.yaml)")

    args = p.parse_args()

    full_cfg = yaml.safe_load(open(args.config))
    processor = TranscriptDataProcessor(full_cfg)
    processor.run()
