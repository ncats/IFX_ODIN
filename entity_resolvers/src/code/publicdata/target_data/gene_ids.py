#!/usr/bin/env python
"""
gene_data_processor.py - Post-process gene provenance mappings:
  • load the main mapping CSV
  • merge with Ensembl, NCBI & HGNC feature tables
  • consolidate and aggregate IDs
  • upsert NCATS Gene IDs (preserve old IFXGene: IDs, mint only truly new)
  • record detailed metadata for each step
"""

import os
import json
import yaml
import logging
import argparse
import hashlib
import secrets
import pandas as pd
from datetime import datetime
from logging.handlers import RotatingFileHandler

def setup_logging(log_file):
    root = logging.getLogger()
    # If we've already added handlers, do nothing.
    if root.handlers:
        return

    root.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Always add console handler
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # And only add file handler if requested
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        root.addHandler(fh)

def compute_md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
    return h.hexdigest()

class GeneDataProcessor:
    def __init__(self, cfg):
        c = cfg['gene_data']
        self.source_file       = c['source_file']
        self.ensembl_file      = c['ensembl_data']
        self.ncbi_file         = c['ncbi_data']
        self.hgnc_file         = c['hgnc_data']
        self.intermediate_path = c['intermediate_gene_ids_path']
        self.gene_ids_path     = c['gene_ids_path']
        self.metadata_path     = c['metadata_file']
        self.log_file          = c.get('log_file', "")
        setup_logging(self.log_file)
        logging.info("🚀 Starting GeneDataProcessor ~ consolidating and creating IFX gene_ids")

        self.metadata = {
            "timestamp": {"start": datetime.now().isoformat()},
            "data_sources": [ self.source_file,
                              self.ensembl_file,
                              self.ncbi_file,
                              self.hgnc_file ],
            "processing_steps": [],
            "outputs": []
        }

    def add_metadata_step(self, step_name, description):
        entry = {
            "step_name": step_name,
            "description": description,
            "performed_at": datetime.now().isoformat()
        }
        self.metadata["processing_steps"].append(entry)
        logging.info("Added metadata step: %s – %s", step_name, description)

    def load_dataset(self, file_path):
        t0 = datetime.now()
        cols = [
            'ensembl_gene_id','ncbi_ensembl_gene_id','hgnc_ensembl_gene_id','nodenorm_ensembl_gene_id',
            'Ensembl_ID_Provenance','ensembl_NCBI_id','ncbi_NCBI_id','hgnc_NCBI_id','nodenorm_NCBI_id',
            'NCBI_ID_Provenance','ensembl_hgnc_id','ncbi_hgnc_id','hgnc_hgnc_id','nodenorm_HGNC',
            'HGNC_ID_Provenance','hgnc_omim_id','nodenorm_OMIM','OMIM_ID_Provenance','nodenorm_UMLS',
            'ensembl_symbol','ncbi_symbol','hgnc_symbol','nodenorm_symbol',
            'Symbol_Provenance','ensembl_gene_type','ncbi_gene_type','hgnc_gene_type',
            'ensembl_description','ncbi_description','hgnc_description','Description_Provenance',
            'ensembl_location','ncbi_location','hgnc_location','Location_Provenance',
            'ensembl_synonyms','ncbi_synonyms','hgnc_synonyms','Total_Mapping_Ratio'
        ]
        raw = pd.read_csv(file_path, low_memory=False)
        self.df = raw.reindex(columns=cols)
        logging.info("Loaded %d rows from %s; kept %d cols",
                     len(raw), file_path, len(self.df.columns))
        self.add_metadata_step("Dataset Loading", f"Loaded and filtered dataset from {file_path}")
        self.metadata["outputs"].append({
            "name": "loaded_source",
            "path": file_path,
            "records": len(self.df)
        })
        self.metadata["processing_steps"].append({
            "step": "load_dataset",
            "duration_seconds": (datetime.now() - t0).total_seconds(),
            "records": len(self.df)
        })

    def merge_with_external_data(self):
        t0 = datetime.now()
        en = pd.read_csv(self.ensembl_file, low_memory=False)
        nc = pd.read_csv(self.ncbi_file,    low_memory=False)
        hg = pd.read_csv(self.hgnc_file,    low_memory=False)

        en = en[['ensembl_gene_id','ensembl_strand']].drop_duplicates()
        nc = (nc[['ncbi_NCBI_id','ncbi_Feature_type','ncbi_mim_id',
                  'ncbi_miR_id','ncbi_imgt_id']]
              .drop_duplicates().replace('-', None))
        hg = hg[['hgnc_hgnc_id','hgnc_prev_symbol','hgnc_gene_group',
                 'hgnc_vega_id','hgnc_ccds_id','hgnc_pubmed_id','hgnc_orphanet_id']].drop_duplicates()

        self.df = self.df.merge(en, how='left', on='ensembl_gene_id')
        self.df = self.df.merge(nc, how='left', on='ncbi_NCBI_id')
        self.df = self.df.merge(hg, how='left', on='hgnc_hgnc_id')
        logging.info("Merged with Ensembl (%d), NCBI (%d), HGNC (%d)",
                     len(en), len(nc), len(hg))
        self.add_metadata_step("External Data Merge", "Merged with Ensembl/NCBI/HGNC feature tables")
        self.metadata["processing_steps"].append({
            "step": "merge_with_external_data",
            "duration_seconds": (datetime.now() - t0).total_seconds(),
            "records": len(self.df)
        })

    def consolidate_columns(self):
        t0 = datetime.now()
        expected = [
            'ensembl_gene_id','ncbi_ensembl_gene_id','hgnc_ensembl_gene_id','nodenorm_ensembl_gene_id',
            'ensembl_NCBI_id','ncbi_NCBI_id','hgnc_NCBI_id','nodenorm_NCBI_id',
            'ensembl_hgnc_id','ncbi_hgnc_id','hgnc_hgnc_id','nodenorm_HGNC',
            'ensembl_symbol','ncbi_symbol','hgnc_symbol','nodenorm_symbol',
            'ensembl_description','ncbi_description','hgnc_description',
            'ensembl_location','ncbi_location','hgnc_location',
            'ensembl_gene_type','ncbi_gene_type','hgnc_gene_type',
            'ncbi_mim_id','hgnc_omim_id','nodenorm_OMIM',
            'ensembl_synonyms','ncbi_synonyms','hgnc_synonyms'
        ]
        for c in expected:
            if c not in self.df.columns:
                self.df[c] = ""
        groups = [
            ('ensembl_gene_id','ncbi_ensembl_gene_id','hgnc_ensembl_gene_id','nodenorm_ensembl_gene_id','consolidated_gene_id'),
            ('ensembl_NCBI_id','ncbi_NCBI_id','hgnc_NCBI_id','nodenorm_NCBI_id','consolidated_NCBI_id'),
            ('ensembl_hgnc_id','ncbi_hgnc_id','hgnc_hgnc_id','nodenorm_HGNC','consolidated_hgnc_id'),
            ('ensembl_symbol','ncbi_symbol','hgnc_symbol','nodenorm_symbol','consolidated_symbol'),
            ('ensembl_description','ncbi_description','hgnc_description','consolidated_description'),
            ('ncbi_mim_id','hgnc_omim_id','nodenorm_OMIM','consolidated_mim_id'),
            ('ensembl_location','ncbi_location','hgnc_location','consolidated_location'),          # ← add this
            ('ensembl_gene_type','ncbi_gene_type','hgnc_gene_type','consolidated_gene_type')       # ← and this
        ]
        for tup in groups:
            if len(tup)==5:
                a,b,c,d,e = tup
                self.df[e] = (
                    self.df[[a,b,c,d]].apply(
                      lambda row: row[a] if (row[a]==row[b]==row[c]==row[d]) else None,
                      axis=1
                    )
                    .fillna(self.df[a])
                    .fillna(self.df[b])
                    .fillna(self.df[c])
                    .fillna(self.df[d])
                )
            else:
                a,b,c,e = tup
                self.df[e] = self.df[a].fillna(self.df[b]).fillna(self.df[c])

        columns_to_drop = [
            'ensembl_gene_id','ncbi_ensembl_gene_id','hgnc_ensembl_gene_id','nodenorm_ensembl_gene_id',
            'ensembl_NCBI_id','ncbi_NCBI_id','hgnc_NCBI_id','nodenorm_NCBI_id',
            'ensembl_hgnc_id','ncbi_hgnc_id','hgnc_hgnc_id','nodenorm_HGNC',
            'ensembl_symbol','ncbi_symbol','hgnc_symbol','nodenorm_symbol',
            'ensembl_description','ncbi_description','hgnc_description',
            'ensembl_location','ncbi_location','hgnc_location',
            'ensembl_gene_type','ncbi_gene_type','hgnc_gene_type','ncbi_Feature_type','hgnc_gene_group',
            'ensembl_synonyms','ncbi_synonyms','hgnc_synonyms','hgnc_omim_id','ncbi_mim_id','nodenorm_OMIM'
        ]
        to_drop = [c for c in columns_to_drop if c in self.df.columns]
        self.df.drop(columns=to_drop, inplace=True)
        logging.info("Dropped original cols: %s", to_drop)

        self.add_metadata_step("Consolidate Gene IDs", "Built consolidated_* columns and dropped raw ones")
        self.metadata["processing_steps"].append({
            "step": "consolidate_columns",
            "duration_seconds": (datetime.now()-t0).total_seconds(),
            "records": len(self.df)
        })

    # ← corrected signature here:
    def _normalize_keys(self, df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
        for k in keys:
            df[k] = (
                df[k]
                  .fillna("")
                  .astype(str)
                  .str.strip()
                  .str.upper()
            )
        return df

    def aggregate_gene_ids(self):
        t0 = datetime.now()
        key_col = 'consolidated_symbol'

        def merge_values(series, lowercase=False, delimiter="|"):
            seen = set()
            for val in series.dropna().astype(str):
                val = val.replace(";", delimiter).replace("|", delimiter).replace(",", delimiter)
                for v in val.split(delimiter):
                    v = v.strip()
                    if lowercase:
                        v = v.lower()
                    if v and v != "nan":
                        seen.add(v)
            return ",".join(sorted(seen)) if lowercase else delimiter.join(sorted(seen))

        # Split into two parts: those to merge, and those to leave untouched
        merge_df = self.df[self.df[key_col].notna() & (self.df[key_col].str.strip() != "")]
        untouched_df = self.df[self.df[key_col].isna() | (self.df[key_col].str.strip() == "")]
        logging.info("Merging %d rows by symbol, skipping %d null/empty symbols", len(merge_df), len(untouched_df))

        concat_columns = [
            'Ensembl_ID_Provenance', 'NCBI_ID_Provenance', 'HGNC_ID_Provenance', 'OMIM_ID_Provenance',
            'nodenorm_UMLS', 'Symbol_Provenance', 'Description_Provenance', 'Location_Provenance',
            'Total_Mapping_Ratio', 'ensembl_strand', 'ncbi_miR_id', 'ncbi_imgt_id',
            'hgnc_prev_symbol', 'hgnc_vega_id', 'hgnc_ccds_id', 'hgnc_pubmed_id', 'hgnc_orphanet_id',
            'consolidated_gene_id', 'consolidated_NCBI_id', 'consolidated_hgnc_id',
            'consolidated_description', 'consolidated_mim_id', 'consolidated_location',
            'consolidated_gene_type'
        ]

        provenance_cols = {
            'Ensembl_ID_Provenance', 'NCBI_ID_Provenance', 'HGNC_ID_Provenance',
            'OMIM_ID_Provenance', 'Symbol_Provenance',
            'Description_Provenance', 'Location_Provenance'
        }

        final_rows = []
        grouped = merge_df.groupby(key_col, dropna=False)

        for symbol, group in grouped:
            row = {key_col: symbol}
            for col in concat_columns:
                is_provenance = col in provenance_cols
                row[col] = merge_values(group[col],
                                        lowercase=is_provenance,
                                        delimiter="," if is_provenance else "|")
            final_rows.append(row)

        merged = pd.DataFrame(final_rows)

        # Recombine with untouched rows
        self.df = pd.concat([merged, untouched_df], ignore_index=True)

        # Ensure all expected columns exist
        for col in concat_columns + [key_col]:
            if col not in self.df.columns:
                self.df[col] = ""

        logging.info("Fuzzy-aggregated %d groups, final rows including untouched: %d", len(merged), len(self.df))
        self.add_metadata_step("Aggregate Gene IDs", "Merged rows by consolidated_symbol, kept null rows untouched")
        self.metadata["processing_steps"].append({
            "step": "aggregate_gene_ids",
            "duration_seconds": (datetime.now() - t0).total_seconds(),
            "records": len(self.df)
        })
        return self.df

    def process_gene_ids(self):
        logging.info("STEP: process_gene_ids (upsert IFXGene IDs)")
        keys = ['consolidated_NCBI_id','consolidated_hgnc_id','consolidated_symbol']

        # 1) normalize & dedupe
        prov = self.df.copy()
        prov = self._normalize_keys(prov, keys)
        prov = prov.drop_duplicates(subset=keys)
        logging.info("After dedupe on %s → %d rows", keys, len(prov))

        # 2) load existing IFXGene IDs
        if os.path.exists(self.gene_ids_path):
            existing = pd.read_csv(self.gene_ids_path, sep="\t", dtype=str)
            existing = self._normalize_keys(existing, keys)
            existing = existing.set_index(keys)[['ncats_gene_id','createdAt','updatedAt']]
            existing_df = existing.reset_index()
            logging.info("Loaded %d existing IFX IDs", len(existing_df))
        else:
            existing_df = pd.DataFrame(columns=keys + ['ncats_gene_id','createdAt','updatedAt'])
            logging.info("No existing IFX file — will mint all")

        # 3) upsert
        up = prov.merge(existing_df, on=keys, how='left')
        total, new_cnt = len(up), up['ncats_gene_id'].isna().sum()
        matched = total - new_cnt
        logging.info("Of %d rows: matched %d existing, minting %d new", total, matched, new_cnt)

        # 4) mint new & timestamp
        now = datetime.now().isoformat()
        if new_cnt:
            up.loc[up['ncats_gene_id'].isna(), 'ncats_gene_id'] = [
                'IFXGene:' + ''.join(secrets.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789') for _ in range(7))
                for _ in range(new_cnt)
            ]
            up.loc[up['createdAt'].isna(), 'createdAt'] = now
        up['updatedAt'] = now

        # 5) save
        cols = ['ncats_gene_id','createdAt','updatedAt'] + [c for c in up.columns
                                                           if c not in ('ncats_gene_id','createdAt','updatedAt')]
        final = up[cols]
        os.makedirs(os.path.dirname(self.gene_ids_path), exist_ok=True)
        final.to_csv(self.gene_ids_path, index=False, sep="\t")
        logging.info("Upserted IFXGene IDs to %s", self.gene_ids_path)
        self.add_metadata_step("Save Gene IDs",
                               f"Upserted IFXGene IDs to {self.gene_ids_path}")

    def save_metadata(self):
        self.metadata["timestamp"]["end"] = datetime.now().isoformat()
        os.makedirs(os.path.dirname(self.metadata_path), exist_ok=True)
        with open(self.metadata_path, 'w') as mf:
            json.dump(self.metadata, mf, indent=2)
        logging.info("✨ Metadata written to %s", self.metadata_path)

    def run(self):
        self.load_dataset(self.source_file)
        self.merge_with_external_data()
        self.consolidate_columns()
        self.aggregate_gene_ids()
        self.process_gene_ids()
        self.save_metadata()

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Post-process gene provenance mappings")
    p.add_argument("--config", required=True, help="YAML config with a `gene_data` section")
    args = p.parse_args()

    cfg_all = yaml.safe_load(open(args.config))
    processor = GeneDataProcessor(cfg_all)
    processor.run()
