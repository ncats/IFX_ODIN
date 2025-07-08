#!/usr/bin/env python
"""
protein_data_processor.py - Post-process protein provenance mappings:
  • load the main mapping CSV and UniProt info
  • match and consolidate descriptions (exact, fuzzy, semantic)
  • consolidate IDs across Ensembl, RefSeq, UniProt (including SPARQL overrides and error-handling)
  • upsert or mint NCATS Protein IDs (IFXProtein:) into cache
  • flag canonical vs isoform rows in-place
  • save final protein IDs (with two new columns) and detailed metadata
"""

import os
import json
import yaml
import logging
import argparse
import secrets
import pandas as pd
from datetime import datetime
from sentence_transformers import SentenceTransformer, util
from difflib import SequenceMatcher
import ssl
import re
import warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure SSL and suppress warnings
ssl._create_default_https_context = ssl._create_unverified_context

os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
warnings.filterwarnings("ignore", category=FutureWarning,
                        module="transformers.tokenization_utils_base")


def setup_logging(log_file):
    root = logging.getLogger()
    if root.handlers:
        return
    root.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    root.addHandler(ch)
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        root.addHandler(fh)


class ProteinDataProcessor:
    def __init__(self, cfg):
        self.cfg = cfg
        pd_cfg = cfg['protein_data']
        self.protein_data_path = pd_cfg['source_file']
        self.uniprot_info_path = pd_cfg['uniprot_info_file']
        self.metadata_file     = pd_cfg['metadata_file']
        self.log_file          = pd_cfg.get('log_file', '')
        self.protein_ids_path  = pd_cfg.get('protein_ids_path')
        self.qc_file           = pd_cfg['qc_file']
        setup_logging(self.log_file)
        logging.info("🚀 Initialized ProteinDataProcessor")

        self.metadata = {
            "timestamp": {"start": datetime.now().isoformat()},
            "processing_steps": [],
            "outputs": []
        }

    def add_metadata_step(self, step_name, description, **details):
        entry = {
            "step_name": step_name,
            "description": description,
            "performed_at": datetime.now().isoformat(),
            **details
        }
        self.metadata["processing_steps"].append(entry)
        logging.info("Metadata step: %s – %s", step_name, description)

    def load_data(self):
        t0 = datetime.now()
        logging.info("STEP: load_data")
        self.protein_data = pd.read_csv(self.protein_data_path, low_memory=False)
        ui_cols = [
            "uniprot_id", "uniprot_secondaryAccessions", "uniprot_uniProtkbId",
            "uniprot_sequence", "uniprot_references",
            "uniprot_feature_Domain", "uniprot_feature_Region",
            "uniprot_feature_Coiled_coil", "uniprot_FUNCTION"
        ]
        self.uniprot_info = pd.read_csv(
            self.uniprot_info_path,
            usecols=ui_cols,
            low_memory=False
        )
        # <<< EDIT: Deduplicate UniProt info so merge is 1:1 >>>
        before = len(self.uniprot_info)
        self.uniprot_info = self.uniprot_info.drop_duplicates(subset=['uniprot_id'])
        after = len(self.uniprot_info)
        logging.info(
            "Deduped UniProt info: %d → %d rows (by uniprot_id)",
            before, after
        )
        # Normalize uniprot_is_canonical to boolean
        mapping = {'TRUE': True, 'FALSE': False, '1': True, '0': False}
        self.protein_data['uniprot_is_canonical'] = (
            self.protein_data['uniprot_is_canonical']
                .astype(str)
                .str.strip()
                .str.upper()
                .map(mapping)
                .astype("boolean")  # nullable BooleanDtype
                .fillna(False)      # safely fills pd.NA → False
                .astype(bool)       # back to numpy bool
        )

        # Now merge 1:1
        self.protein_data = pd.merge(
            self.protein_data,
            self.uniprot_info,
            on='uniprot_id',
            how='left'
        )
        self.protein_data.fillna(
            {'ensembl_description': '',
             'uniprot_recommended_name': '',
             'nodenorm_name': ''},
            inplace=True
        )
        duration = (datetime.now() - t0).total_seconds()
        n = len(self.protein_data)
        self.add_metadata_step(
            "load_data",
            f"Loaded {n} protein rows & UniProt info",
            duration_seconds=duration,
            records=n
        )
        self.metadata["outputs"].append({
            "name": "loaded_data",
            "path": self.protein_data_path,
            "records": n
        })

    def match_descriptions(self, fuzzy_threshold=0.55, semantic_threshold=0.60):
        """
        Compute a ‘combined_protein_name’ by comparing
        Ensembl description, UniProt name, and NodeNorm name
        using exact, fuzzy, and semantic matching.
        """
        t0 = datetime.now()
        logging.info("STEP: match_descriptions")
        model = SentenceTransformer('all-MiniLM-L6-v2')

        df = self.protein_data
        df['combined_protein_name'] = pd.NA
        df['protein_name_score']    = 0.0
        df['protein_name_method']   = pd.NA

        for idx, row in df.iterrows():
            ed = row.get('ensembl_description', '') or ''
            un = row.get('uniprot_recommended_name', '') or ''
            nn = row.get('nodenorm_name', '') or ''

            if ed and not (un or nn):
                df.at[idx, 'combined_protein_name'] = ed
                df.at[idx, 'protein_name_method']  = "None"
                continue
            if un and not (ed or nn):
                df.at[idx, 'combined_protein_name'] = un
                df.at[idx, 'protein_name_method']  = "None"
                continue
            if nn and not (ed or un):
                df.at[idx, 'combined_protein_name'] = nn
                df.at[idx, 'protein_name_method']  = "None"
                continue

            if ed and un and ed == un:
                df.at[idx, 'combined_protein_name'] = un
                df.at[idx, 'protein_name_score']   = 1.0
                df.at[idx, 'protein_name_method']  = "Exact"
                continue
            if ed and nn and ed == nn:
                df.at[idx, 'combined_protein_name'] = nn
                df.at[idx, 'protein_name_score']   = 1.0
                df.at[idx, 'protein_name_method']  = "Exact"
                continue
            if un and nn and un == nn:
                df.at[idx, 'combined_protein_name'] = nn
                df.at[idx, 'protein_name_score']   = 1.0
                df.at[idx, 'protein_name_method']  = "Exact"
                continue

            pairs = [
                ('Ensembl-Uniprot', ed, un),
                ('Ensembl-NodeNorm', ed, nn),
                ('Uniprot-NodeNorm', un, nn),
            ]

            best_method, best_score, best_name = None, 0.0, None
            for method, a, b in pairs:
                if a and b:
                    score = SequenceMatcher(None, a, b).ratio()
                    if score > best_score:
                        best_method, best_score, best_name = method, score, b
            if best_score >= fuzzy_threshold:
                df.at[idx, 'combined_protein_name'] = best_name
                df.at[idx, 'protein_name_score']   = best_score
                df.at[idx, 'protein_name_method']  = f"Fuzzy-{best_method}"
                continue

            best_method, best_score, best_name = None, 0.0, None
            for method, a, b in pairs:
                if a and b:
                    emb1 = model.encode(a, convert_to_tensor=True, show_progress_bar=False)
                    emb2 = model.encode(b, convert_to_tensor=True, show_progress_bar=False)
                    score = util.pytorch_cos_sim(emb1, emb2).item()
                    if score > best_score:
                        best_method, best_score, best_name = method, score, b
            if best_score >= semantic_threshold:
                df.at[idx, 'combined_protein_name'] = best_name
                df.at[idx, 'protein_name_score']   = best_score
                df.at[idx, 'protein_name_method']  = f"Semantic-{best_method}"
            else:
                df.at[idx, 'combined_protein_name'] = " | ".join(filter(None, [ed, un, nn]))
                df.at[idx, 'protein_name_score']   = best_score
                df.at[idx, 'protein_name_method']  = "Semantic-Fallback"

        duration = (datetime.now() - t0).total_seconds()
        self.add_metadata_step(
            "match_descriptions",
            "Computed exact/fuzzy/semantic similarity scores",
            duration_seconds=duration,
            records=len(df)
        )
        self.protein_data = df

    def consolidate_columns(self):
        t0 = datetime.now()
        logging.info("STEP: consolidate_columns")
        df = self.protein_data
        # 🛡️ Ensure we preserve the original full UniProt ID
        if 'uniprot_id_full' in self.protein_data.columns:
            df['uniprot_id_full'] = self.protein_data['uniprot_id_full']

        def dedupe(vals):
            seen = {}
            for v in vals:
                if pd.notna(v) and v:
                    for token in str(v).split("|"):
                        token = token.strip()
                        if token and token not in seen:
                            seen[token] = None
            return "|".join(seen.keys())

        df['consolidated_ensembl_protein_id'] = df[
            ['uniprot_ensembl_pro',
             'ensembl_peptide_id_version',
             'refseq_ensembl_protein_id',
             'nodenorm_ensembl_protein_id']
        ].apply(lambda row: dedupe(row.tolist()), axis=1)

        df['consolidated_ensembl_transcript_id'] = df[
            ['ensembl_transcript_id_version',
             'uniprot_ensembl_transcript']
        ].apply(lambda row: dedupe(row.tolist()), axis=1)

        df['consolidated_refseq_protein'] = df[
            ['ensembl_refseq_NP',
             'uniprot_xref_RefSeq',
             'refseq_protein_id_merged']
        ].apply(lambda row: dedupe(row.tolist()), axis=1)

        df['consolidated_symbol'] = df[
            ['ensembl_symbol',
             'uniprot_gene_name']
        ].apply(lambda row: dedupe(row.tolist()), axis=1)

        raw = [
            'ensembl_peptide_id_version',
            'refseq_ensembl_protein_id',
            'nodenorm_ensembl_protein_id',
            'ensembl_transcript_id_version',
            'uniprot_recommended_name',
            'ensembl_description',
            'uniprot_ensembl_pro',
            'uniprot_ensembl_transcript',
            'ensembl_uniprot_id',
            'refseq_uniprot_id',
            'nodenorm_uniprot_id',
            'ensembl_refseq_NP',
            'uniprot_xref_RefSeq',
            'refseq_protein_id_merged',
            'ensembl_symbol',
            'uniprot_gene_name',
            'nodenorm_name'
        ]
        for c in raw:
            if c in df.columns:
                df.drop(columns=[c], inplace=True)
        # 🛡️ Re-assert preservation of uniprot_id_full (in case it got dropped)
        if 'uniprot_id_full' not in df.columns and 'uniprot_id' in df.columns:
            df['uniprot_id_full'] = df['uniprot_id']
        duration = (datetime.now() - t0).total_seconds()
        self.add_metadata_step(
            "consolidate_columns",
            "Built consolidated Ensembl/RefSeq fields and preserved native uniprot_id",
            duration_seconds=duration,
            records=len(df)
        )
        self.protein_data = df

    def generate_ncats_protein_ids(self):
        t0 = datetime.now()
        logging.info("STEP: generate_ncats_protein_ids")
        now_iso = datetime.now().isoformat()
        keys = ['uniprot_id_full']
        # 1) Build prov table & dedupe
        prov = self.protein_data.copy()
        for k in keys:
            prov[k] = prov[k].fillna('').astype(str).str.strip().str.upper()
        prov = prov.drop_duplicates(subset=keys)

        # 2) Load existing or empty cache
        if os.path.exists(self.protein_ids_path):
            existing = pd.read_csv(self.protein_ids_path, dtype=str)

            # 🛡️ Backfill uniprot_id_full from uniprot_id if not present
            if 'uniprot_id_full' not in existing.columns and 'uniprot_id' in existing.columns:
                logging.warning("⚠️ Cache missing 'uniprot_id_full'. Backfilling from 'uniprot_id'.")
                existing['uniprot_id_full'] = existing['uniprot_id']

            for k in keys:
                existing[k] = existing[k].fillna('').astype(str).str.strip().str.upper()

            existing = existing.set_index(keys)[['ncats_protein_id', 'createdAt', 'updatedAt']].reset_index()

            # <<< EDIT: diagnose & dedupe existing cache >>>
            dup_count = existing.duplicated(subset=keys, keep=False).sum()
            if dup_count:
                logging.info(
                    "Existing cache has %d duplicate rows on keys %s; deduping",
                    dup_count, keys
                )
            existing = existing.drop_duplicates(subset=keys)
            logging.info("After dedupe, cache has %d unique rows", len(existing))
        else:
            existing = pd.DataFrame(columns=keys + ['ncats_protein_id', 'createdAt', 'updatedAt'])

        # 3) Upsert
        up = prov.merge(existing, on=keys, how='left')
        new_mask = up['ncats_protein_id'].isna()
        new_cnt = int(new_mask.sum())
        if new_cnt:
            up.loc[new_mask, 'ncats_protein_id'] = [
                'IFXProtein:' + ''.join(secrets.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
                                        for _ in range(7))
                for _ in range(new_cnt)
            ]
            up.loc[new_mask, 'createdAt'] = now_iso
        up['updatedAt'] = now_iso

        # 4) Persist cache
        os.makedirs(os.path.dirname(self.protein_ids_path), exist_ok=True)
        up.to_csv(self.protein_ids_path, index=False)

        # 5) Merge back into working DataFrame
        for k in keys:
            self.protein_data[k] = (
                self.protein_data[k]
                .fillna('')
                .astype(str)
                .str.strip()
                .str.upper()
            )
        self.protein_data = pd.merge(
            self.protein_data,
            up[['ncats_protein_id', 'createdAt', 'updatedAt'] + keys],
            on=keys, how='left'
        )

        # 6) reorder columns
        cols_order = [
            'ncats_protein_id', 'createdAt', 'updatedAt',
        ] + [c for c in self.protein_data.columns if c not in ('ncats_protein_id', 'createdAt', 'updatedAt')]
        self.protein_data = self.protein_data[cols_order]

        duration = (datetime.now() - t0).total_seconds()
        self.add_metadata_step(
            "generate_ncats_protein_ids",
            f"Upserted IFXProtein IDs ({len(up)-new_cnt} existing, {new_cnt} new)",
            duration_seconds=duration, records=len(up)
        )

    def filter_isoforms(self):
        import re
        from datetime import datetime

        t0 = datetime.now()
        logging.info("STEP: filter_isoforms")
        df = self.protein_data.copy()

        # ✅ Preserve full UniProt ID with isoform suffix before modification
        if 'uniprot_id_full' not in df.columns:
            df['uniprot_id_full'] = df['uniprot_id'].astype(str)

        # ——— 0) Extract UniProt isoform suffixes ———
        df['uniprot_isoform'] = ''
        iso_mask = df['uniprot_id'].str.contains(r'-\d+$', na=False)
        df.loc[iso_mask, 'uniprot_isoform'] = df.loc[iso_mask, 'uniprot_id']
        df.loc[iso_mask, 'uniprot_id'] = df.loc[iso_mask, 'uniprot_id'].str.replace(r'-\d+$', '', regex=True)

        # ——— 1) Extract Ensembl "-1" canonical tokens & clean IDs ———
        def split_ensembl(val):
            toks = [t.strip() for t in str(val).split('|') if t.strip()]
            canon = [t for t in toks if t.endswith('-1')]
            stripped = [re.sub(r'-1$', '', t) for t in toks]
            return "|".join(canon), "|".join(dict.fromkeys(stripped))
        ens = (
            df['consolidated_ensembl_protein_id']
              .apply(split_ensembl)
              .apply(pd.Series, index=['ensembl_canonical','consolidated_ensembl_protein_id'])
        )
        df[['ensembl_canonical','consolidated_ensembl_protein_id']] = ens

        # ——— 2) Mark isoforms ———
        df['is_canonical'] = True
        df['canonical_isoform'] = ''

        def choose_canonical(sub):
            sel = sub[sub['uniprot_is_canonical']]
            if not sel.empty:
                return sel.sort_values(
                    by=['uniprot_annotationScore','Total_Mapping_Score','protein_name_score'],
                    ascending=False
                ).index[0]
            reviewed = sub[sub['uniprot_entryType']=="UniProtKB reviewed (Swiss-Prot)"]
            if not reviewed.empty:
                return reviewed.sort_values(
                    by=['uniprot_annotationScore','Total_Mapping_Score','protein_name_score'],
                    ascending=False
                ).index[0]
            ens_sel = sub[sub['ensembl_canonical']!='']
            return ens_sel.index[0] if not ens_sel.empty else sub.index[0]

        # 3a) within each uniprot_id group
        for uid, grp in df.groupby('uniprot_id'):
            if len(grp) < 2:
                continue
            keep = choose_canonical(grp)
            for idx in grp.index.drop(keep):
                df.at[idx, 'is_canonical'] = False
                df.at[idx, 'canonical_isoform'] = df.at[keep, 'ncats_protein_id']

        # 3b) across consolidated_symbol groups
        for sym, grp in df.groupby('consolidated_symbol'):
            if len(grp) < 2 or grp['uniprot_id'].nunique() < 2:
                continue
            keep = choose_canonical(grp)
            for idx in grp.index.drop(keep):
                df.at[idx, 'is_canonical'] = False
                df.at[idx, 'canonical_isoform'] = df.at[keep, 'ncats_protein_id']

        # ——— 3c) Derive uniprot_id_canonical flag ———
        lookup = df.set_index('ncats_protein_id')['uniprot_id'].to_dict()
        mapped = df['canonical_isoform'].map(lookup)
        df['uniprot_id_canonical'] = mapped.notna()

        # ——— Wrap up ———
        total = len(df)
        n_can = int(df['is_canonical'].sum())
        n_iso = total - n_can
        logging.info(f"filter_isoforms: {n_can} canonical, {n_iso} isoforms (out of {total})")
        duration = (datetime.now() - t0).total_seconds()

        self.protein_data = df
        self.add_metadata_step(
            "filter_isoforms",
            "Flagged canonical vs isoforms using UniProt and Ensembl rules",
            duration_seconds=duration,
            records=total,
            canonical_count=n_can,
            isoform_count=n_iso
        )

    def qc_flag_canonical_mismatches(self, output_path='src/data/publicdata/target_data/qc/qc_canonical_mismatches.qc.csv'):
        if not self.cfg.get("global", {}).get("qc_mode", True):
            logging.info("QC mode disabled – skipping canonical mismatch export")
            return
        df = self.protein_data
        # ✅ Future-proof boolean normalization
        df['uniprot_is_canonical'] = (
            df['uniprot_is_canonical']
                .astype(str)
                .str.lower()
                .map({'true': True, '1': True})
                .astype("boolean")
                .fillna(False)
                .astype(bool)
        )

        mismatches = df[(df['uniprot_is_canonical'] == True) & (df['is_canonical'] == False)]
        logging.info(f"QC check: {len(mismatches)} canonical mismatches identified")

        if not mismatches.empty:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            mismatches.to_csv(output_path, index=False)
            logging.warning(f"⚠️ Found {len(mismatches)} canonical mismatches. Saved to: {output_path}")
        else:
            logging.info("✅ No canonical mismatches found.")

    def save_data(self):
        t0 = datetime.now()
        logging.info("STEP: save_data (write IFX cache)")
        path = self.protein_ids_path

        # Change extension if still .csv in config
        if path.endswith(".csv"):
            path = path.replace(".csv", ".tsv")

        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.protein_data.to_csv(path, index=False, sep='\t')
        logging.info("Updated IFX cache (protein_ids_path) as TSV at %s", path)

        self.metadata["outputs"].append({
            "name": "ifx_protein_ids",
            "path": path,
            "records": len(self.protein_data)
        })
        self.metadata["timestamp"]["end"] = datetime.now().isoformat()

        os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
        with open(self.metadata_file, 'w') as mf:
            json.dump(self.metadata, mf, indent=2)
        logging.info("Saved metadata to %s", self.metadata_file)

    def run(self):
        self.load_data()
        self.match_descriptions()
        self.consolidate_columns()
        self.generate_ncats_protein_ids()
        self.filter_isoforms()
        self.qc_flag_canonical_mismatches()
        self.save_data()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Post-process protein provenance mappings"
    )
    p.add_argument("--config", type=str,
               default="config/targets_config.yaml",
               help="YAML config (default: config/targets_config.yaml)")

    args = parser.parse_args()
    cfg = yaml.safe_load(open(args.config))
    processor = ProteinDataProcessor(cfg)
    processor.run()
