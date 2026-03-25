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
from difflib import SequenceMatcher
import re
import warnings

os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
os.environ["TOKENIZERS_PARALLELISM"] = "false"  # prevent leaked semaphore at exit
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

    @staticmethod
    def _normalize_boolean(series):
        """Normalize a column of mixed boolean representations to numpy bool."""
        mapping = {'TRUE': True, 'FALSE': False, '1': True, '0': False}
        return (
            series
            .astype(str)
            .str.strip()
            .str.upper()
            .map(mapping)
            .astype("boolean")
            .fillna(False)
            .astype(bool)
        )

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
        self.protein_data['uniprot_is_canonical'] = self._normalize_boolean(
            self.protein_data['uniprot_is_canonical']
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
        import torch
        # Disable MPS backend to prevent segfault at process exit on macOS
        if hasattr(torch.backends, 'mps'):
            torch.backends.mps.is_available = lambda: False
            torch.backends.mps.is_built = lambda: False
        from sentence_transformers import SentenceTransformer, util
        model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')

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

        # Free the model to avoid PyTorch segfault / leaked semaphores at exit
        del model
        import gc; gc.collect()

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

        def dedupe(vals):
            seen = {}
            for v in vals:
                if pd.notna(v) and v:
                    for token in str(v).split("|"):
                        token = token.strip()
                        if token and token not in seen:
                            seen[token] = None
            return "|".join(seen.keys())

        def toks(x):
            if pd.isna(x) or not x:
                return []
            return [t.strip() for t in str(x).split("|") if t.strip()]

        def strip_version(enst):
            return re.sub(r"\.\d+$", "", enst)

        # ——— Consolidate Ensembl protein IDs ———
        df['consolidated_ensembl_protein_id'] = df[
            ['uniprot_ensembl_pro',
            'ensembl_peptide_id_version',
            'refseq_ensembl_protein_id',
            'nodenorm_ensembl_protein_id']
        ].apply(lambda row: dedupe(row.tolist()), axis=1)

        # ——— Ensembl transcript consolidation with mismatch rule ———
        def consolidate_transcripts(row):
            ens_vals = toks(row.get('ensembl_transcript_id_version', ''))
            uni_vals = toks(row.get('uniprot_ensembl_transcript', ''))

            if ens_vals and uni_vals:
                ens_norm = {strip_version(t) for t in ens_vals}
                uni_norm = {strip_version(t) for t in uni_vals}
                mismatch = ens_norm.isdisjoint(uni_norm)
                row_mismatch = mismatch
            else:
                row_mismatch = False
                mismatch = False

            if mismatch:
                return dedupe(ens_vals), True
            return dedupe(ens_vals + uni_vals), row_mismatch

        cons_vals, mismatches = zip(*df.apply(consolidate_transcripts, axis=1))
        df['consolidated_ensembl_transcript_id'] = list(cons_vals)
        df['ensembl_uniprot_transcript_mismatch'] = list(mismatches)

        # ——— Other consolidated fields ———
        df['consolidated_refseq_protein'] = df[
            ['ensembl_refseq_NP',
            'uniprot_xref_RefSeq',
            'refseq_protein_id_merged']
        ].apply(lambda row: dedupe(row.tolist()), axis=1)

        df['consolidated_symbol'] = df[
            ['ensembl_symbol',
            'uniprot_gene_name']
        ].apply(lambda row: dedupe(row.tolist()), axis=1)

        # Drop raw columns now that consolidated fields are built
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

        mismatch_count = int(df['ensembl_uniprot_transcript_mismatch'].sum()) if 'ensembl_uniprot_transcript_mismatch' in df.columns else 0
        duration = (datetime.now() - t0).total_seconds()
        self.add_metadata_step(
            "consolidate_columns",
            "Built consolidated Ensembl/RefSeq fields, preferred Ensembl on mismatch",
            duration_seconds=duration,
            records=len(df),
            transcript_mismatch_count=mismatch_count
        )

        self.protein_data = df

    def generate_ncats_protein_ids(self):
        t0 = datetime.now()
        logging.info("STEP: generate_ncats_protein_ids")
        now_iso = datetime.now().isoformat()

        # At this stage, uniprot_id still has isoform suffixes
        # (e.g., Q9UK61-1). This is the natural unique key.
        keys = ['uniprot_id']

        # 1) Build prov table & dedupe
        prov = self.protein_data.copy()
        for k in keys:
            prov[k] = prov[k].fillna('').astype(str).str.strip().str.upper()
        prov = prov.drop_duplicates(subset=keys)

        # --- helper: sniff separator ---
        def _sniff_sep(path):
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                sample = f.read(8192)
            return '\t' if sample.count('\t') > sample.count(',') else ','

        # 2) Load existing cache (try configured path, then alternate ext)
        existing = None
        candidates = [self.protein_ids_path]
        if self.protein_ids_path.endswith('.csv'):
            candidates.append(self.protein_ids_path.replace('.csv', '.tsv'))
        elif self.protein_ids_path.endswith('.tsv'):
            candidates.append(self.protein_ids_path.replace('.tsv', '.csv'))

        for p in candidates:
            if os.path.exists(p):
                try:
                    sep = _sniff_sep(p)
                    existing = pd.read_csv(p, dtype=str, sep=sep, low_memory=False)
                    logging.info(
                        "Loaded existing IFX cache from %s (sep=%s)",
                        p, 'TAB' if sep == '\t' else 'COMMA'
                    )
                    break
                except Exception as e:
                    logging.warning(f"Failed reading {p}: {e}")

        if existing is None:
            existing = pd.DataFrame(
                columns=keys + ['ncats_protein_id', 'createdAt', 'updatedAt']
            )
            logging.info("No existing IFX cache found; starting fresh.")

        # Backward compat: old caches may use 'uniprot_id_full' as key
        if 'uniprot_id_full' in existing.columns and 'uniprot_id' not in existing.columns:
            existing = existing.rename(columns={'uniprot_id_full': 'uniprot_id'})
            logging.info("Renamed legacy 'uniprot_id_full' to 'uniprot_id' in cache")
        elif 'uniprot_id_full' in existing.columns and 'uniprot_id' in existing.columns:
            # If both exist, prefer uniprot_id_full for matching (it has the suffix)
            # but use it under the 'uniprot_id' key
            existing['uniprot_id'] = existing['uniprot_id_full'].where(
                existing['uniprot_id_full'].str.contains('-', na=False),
                existing['uniprot_id']
            )

        for col in ['ncats_protein_id', 'createdAt', 'updatedAt']:
            if col not in existing.columns:
                existing[col] = None

        missing_key_cols = [k for k in keys if k not in existing.columns]
        if missing_key_cols:
            logging.warning(
                "Existing cache missing key columns %s; starting fresh.",
                missing_key_cols
            )
            existing = pd.DataFrame(
                columns=keys + ['ncats_protein_id', 'createdAt', 'updatedAt']
            )

        # Normalize + dedupe existing
        for k in keys:
            existing[k] = existing[k].fillna('').astype(str).str.strip().str.upper()

        if not existing.empty:
            existing = (
                existing
                .set_index(keys)[['ncats_protein_id', 'createdAt', 'updatedAt']]
                .reset_index()
            )
            dup_count = existing.duplicated(subset=keys, keep=False).sum()
            if dup_count:
                logging.info(
                    "Existing cache has %d dup rows on %s; deduping.",
                    dup_count, keys
                )
            existing = existing.drop_duplicates(subset=keys)
            logging.info("After dedupe, cache has %d unique rows", len(existing))

        # 3) Upsert
        up = prov.merge(existing, on=keys, how='left')
        new_mask = up['ncats_protein_id'].isna()
        new_cnt = int(new_mask.sum())
        if new_cnt:
            up.loc[new_mask, 'ncats_protein_id'] = [
                'IFXProtein:' + ''.join(
                    secrets.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
                    for _ in range(7)
                )
                for _ in range(new_cnt)
            ]
            up.loc[new_mask, 'createdAt'] = now_iso
        up['updatedAt'] = now_iso

        # 4) Persist cache
        os.makedirs(os.path.dirname(self.protein_ids_path), exist_ok=True)
        up.to_csv(self.protein_ids_path, index=False, sep='\t')

        # 5) Merge back into working DataFrame
        for k in keys:
            self.protein_data[k] = (
                self.protein_data[k]
                .fillna('').astype(str).str.strip().str.upper()
            )
        self.protein_data = pd.merge(
            self.protein_data,
            up[['ncats_protein_id', 'createdAt', 'updatedAt'] + keys],
            on=keys, how='left'
        )

        # 6) reorder columns
        cols_order = ['ncats_protein_id', 'createdAt', 'updatedAt'] + \
                    [c for c in self.protein_data.columns
                     if c not in ('ncats_protein_id', 'createdAt', 'updatedAt')]
        self.protein_data = self.protein_data[cols_order]

        duration = (datetime.now() - t0).total_seconds()
        self.add_metadata_step(
            "generate_ncats_protein_ids",
            f"Upserted IFXProtein IDs ({len(up)-new_cnt} existing, {new_cnt} new)",
            duration_seconds=duration, records=len(up)
        )

    # Gene-level fields that should be identical across all isoforms
    # of the same uniprot_id. We propagate from any row that has a
    # value to rows that are missing it (e.g., bare accession row has
    # uniprot_entryType but isoform rows don't, or vice versa).
    GENE_LEVEL_FILL_FIELDS = [
        'uniprot_entryType',
        'uniprot_annotationScore',
        'uniprot_NCBI_id',
        'nodenorm_UMLS',
        'NodeNorm_Protein',
    ]

    def filter_isoforms(self):
        import re
        from datetime import datetime

        t0 = datetime.now()
        logging.info("STEP: filter_isoforms")
        df = self.protein_data.copy()

        # ——— 0) Extract UniProt isoform suffixes ———
        df['uniprot_isoform'] = ''
        iso_mask = df['uniprot_id'].str.contains(r'-\d+$', na=False)
        df.loc[iso_mask, 'uniprot_isoform'] = df.loc[iso_mask, 'uniprot_id']
        df.loc[iso_mask, 'uniprot_id'] = (
            df.loc[iso_mask, 'uniprot_id'].str.replace(r'-\d+$', '', regex=True)
        )

        # ——— 1) Propagate gene-level fields across isoform groups ———
        # Some rows (e.g., bare Q9UK61 without isoform suffix) carry
        # uniprot_entryType / annotationScore but isoform rows don't.
        # Fill missing values within each uniprot_id group.
        for col in self.GENE_LEVEL_FILL_FIELDS:
            if col not in df.columns:
                continue
            df[col] = df[col].replace('', pd.NA)
            df[col] = df.groupby('uniprot_id')[col].transform(
                lambda s: s.ffill().bfill()
            )
            df[col] = df[col].fillna('')

        # ——— 2) Absorb bare accession rows ———
        # A "bare" row has uniprot_isoform == '' (no -N suffix) BUT
        # sibling rows with isoform suffixes exist for the same
        # uniprot_id. The bare row is an artifact of nodenorm/refseq
        # mapping the base accession. It typically has no ENSP.
        # We absorb it: transfer any unique data it carries to the
        # group, then drop it.
        bare_mask = df['uniprot_isoform'] == ''
        uids_with_isoforms = set(
            df.loc[~bare_mask, 'uniprot_id'].unique()
        )
        drop_idx = df.index[
            bare_mask & df['uniprot_id'].isin(uids_with_isoforms)
        ]

        if len(drop_idx) > 0:
            logging.info(
                "Absorbing %d bare accession rows (uniprot_id has "
                "isoform-suffixed siblings)", len(drop_idx)
            )
            # Before dropping, propagate any unique consolidated IDs
            # from bare rows to the group (refseq, symbol, etc.)
            for idx in drop_idx:
                uid = df.at[idx, 'uniprot_id']
                siblings = df[
                    (df['uniprot_id'] == uid)
                    & (df.index != idx)
                ].index
                for col in ['consolidated_refseq_protein',
                            'consolidated_symbol',
                            'combined_protein_name']:
                    bare_val = str(df.at[idx, col]).strip()
                    if bare_val and bare_val != 'nan':
                        for sib_idx in siblings:
                            sib_val = str(df.at[sib_idx, col]).strip()
                            if not sib_val or sib_val == 'nan':
                                df.at[sib_idx, col] = bare_val

            df = df.drop(index=drop_idx).reset_index(drop=True)

        # ——— 3) Extract Ensembl "-1" canonical tokens & clean IDs ———
        def split_ensembl(val):
            toks = [t.strip() for t in str(val).split('|') if t.strip()]
            canon = [t for t in toks if t.endswith('-1')]
            stripped = [re.sub(r'-1$', '', t) for t in toks]
            return "|".join(canon), "|".join(dict.fromkeys(stripped))

        ens = (
            df['consolidated_ensembl_protein_id']
              .apply(split_ensembl)
              .apply(pd.Series, index=['ensembl_canonical',
                                       'consolidated_ensembl_protein_id'])
        )
        df[['ensembl_canonical',
            'consolidated_ensembl_protein_id']] = ens

        # ——— 4) Mark isoforms ———
        df['is_canonical'] = True
        df['canonical_isoform'] = ''

        def choose_canonical(sub):
            # Tier 1: UniProt's own canonical flag
            sel = sub[sub['uniprot_is_canonical']]
            if not sel.empty:
                return sel.sort_values(
                    by=['uniprot_annotationScore',
                        'Total_Mapping_Score',
                        'protein_name_score'],
                    ascending=False
                ).index[0]
            # Tier 2: reviewed (Swiss-Prot) entry type
            reviewed = sub[
                sub['uniprot_entryType'] == "UniProtKB reviewed (Swiss-Prot)"
            ]
            if not reviewed.empty:
                return reviewed.sort_values(
                    by=['uniprot_annotationScore',
                        'Total_Mapping_Score',
                        'protein_name_score'],
                    ascending=False
                ).index[0]
            # Tier 3: has Ensembl canonical token
            ens_sel = sub[sub['ensembl_canonical'] != '']
            if not ens_sel.empty:
                return ens_sel.index[0]
            return sub.index[0]

        # 4a) within each uniprot_id group
        for uid, grp in df.groupby('uniprot_id'):
            if len(grp) < 2:
                continue
            keep = choose_canonical(grp)
            for idx in grp.index.drop(keep):
                df.at[idx, 'is_canonical'] = False
                df.at[idx, 'canonical_isoform'] = df.at[keep, 'ncats_protein_id']

        # 4b) across consolidated_symbol groups
        #     SKIP empty/NaN symbols — grouping unrelated proteins
        #     that happen to lack a gene symbol is incorrect.
        for sym, grp in df.groupby('consolidated_symbol'):
            if not sym or sym == '' or pd.isna(sym):
                continue
            if len(grp) < 2 or grp['uniprot_id'].nunique() < 2:
                continue
            keep = choose_canonical(grp)
            for idx in grp.index.drop(keep):
                df.at[idx, 'is_canonical'] = False
                df.at[idx, 'canonical_isoform'] = df.at[keep, 'ncats_protein_id']

        # ——— 5) Drop redundant columns & rename for clarity ———
        drop_cols = [
            'uniprot_id_full',
            'uniprot_id_canonical',
            'uniprot_is_canonical',              # all FALSE — UniProt flags isoform-suffixed entries only
            'ensembl_uniprot_transcript_mismatch', # all FALSE in current data
        ]
        for c in drop_cols:
            if c in df.columns:
                df = df.drop(columns=[c])

        rename_map = {
            'is_canonical': 'uniprot_canonical',           # TRUE/FALSE: is this the canonical protein for its gene?
            'canonical_isoform': 'canonical_ifx_id',       # IFXProtein ID of the canonical (for isoform rows)
            'ensembl_canonical': 'ensembl_canonical_token', # Ensembl "-1" canonical ENSP token (if present)
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        # ——— Wrap up ———
        total = len(df)
        n_can = int(df['is_canonical'].sum())
        n_iso = total - n_can
        n_bare = len(drop_idx) if len(drop_idx) > 0 else 0
        logging.info(
            "filter_isoforms: %d canonical, %d isoforms (out of %d); "
            "%d bare rows absorbed",
            n_can, n_iso, total, n_bare
        )
        duration = (datetime.now() - t0).total_seconds()

        self.protein_data = df
        self.add_metadata_step(
            "filter_isoforms",
            "Flagged canonical vs isoforms using UniProt and Ensembl rules",
            duration_seconds=duration,
            records=total,
            canonical_count=n_can,
            isoform_count=n_iso,
            bare_rows_absorbed=n_bare,
        )

    def qc_flag_canonical_mismatches(self, output_path='src/data/publicdata/target_data/qc/qc_canonical_mismatches.qc.csv'):
        """
        QC check: log how many canonical proteins are reviewed vs unreviewed.
        (The old uniprot_is_canonical column was dropped — it was always FALSE
        because UniProt only flags isoform-suffixed entries, not base accessions.)
        """
        if not self.cfg.get("global", {}).get("qc_mode", True):
            logging.info("QC mode disabled – skipping canonical QC")
            return
        df = self.protein_data
        canonicals = df[df['uniprot_canonical'] == True]
        n_reviewed = (canonicals['uniprot_entryType'] == "UniProtKB reviewed (Swiss-Prot)").sum()
        n_unreviewed = len(canonicals) - n_reviewed
        logging.info(
            "Canonical QC: %d total (%d reviewed, %d unreviewed)",
            len(canonicals), n_reviewed, n_unreviewed
        )

    # Columns to exclude from the slim protein_ids output
    # These heavy columns remain available in protein_provenance_mapping.csv
    EXCLUDED_COLUMNS = {
        'uniprot_sequence', 'uniprot_references',
        'uniprot_feature_Domain', 'uniprot_feature_Region',
        'uniprot_feature_Coiled_coil', 'uniprot_FUNCTION',
        'uniprot_secondaryAccessions', 'uniprot_uniProtkbId',
    }

    # Desired column order for protein_ids.tsv
    PROTEIN_IDS_COLUMN_ORDER = [
        # CORE
        'ncats_protein_id', 'uniprot_id',
        'consolidated_ensembl_protein_id', 'consolidated_ensembl_transcript_id',
        'consolidated_refseq_protein', 'consolidated_symbol', 'combined_protein_name',
        # FLAGS
        'uniprot_entryType', 'uniprot_canonical', 'canonical_ifx_id',
        'ensembl_canonical_token', 'uniprot_isoform',
        # SCORES
        'uniprot_annotationScore', 'ensembl_transcript_tsl', 'refseq_method',
        'protein_name_score', 'protein_name_method',
        # PROVENANCE
        'Ensembl_ID_Provenance', 'RefSeq_ID_Provenance', 'UniProt_ID_Provenance',
        'Ensembl_Mapping_Score', 'RefSeq_Mapping_Score', 'UniProt_Mapping_Score',
        'Total_Mapping_Score', 'Total_Mapping_Ratio',
        # NCBI/NODENORM
        'uniprot_NCBI_id', 'ensembl_refseq_MANEselect', 'NodeNorm_Protein', 'nodenorm_UMLS',
        # TIMESTAMPS
        'createdAt', 'updatedAt',
    ]

    def save_data(self):
        t0 = datetime.now()
        logging.info("STEP: save_data (write IFX cache)")
        path = self.protein_ids_path

        # Change extension if still .csv in config
        if path.endswith(".csv"):
            path = path.replace(".csv", ".tsv")

        # Drop heavy columns for the slim output
        drop_cols = [c for c in self.EXCLUDED_COLUMNS if c in self.protein_data.columns]
        slim = self.protein_data.drop(columns=drop_cols)
        if drop_cols:
            logging.info("Excluded %d heavy columns from protein_ids output: %s", len(drop_cols), drop_cols)

        # Reorder columns
        ordered = [c for c in self.PROTEIN_IDS_COLUMN_ORDER if c in slim.columns]
        remaining = [c for c in slim.columns if c not in ordered]
        slim = slim[ordered + remaining]

        os.makedirs(os.path.dirname(path), exist_ok=True)
        slim.to_csv(path, index=False, sep='\t')
        logging.info("Updated IFX cache (protein_ids_path) as TSV at %s", path)

        self.metadata["outputs"].append({
            "name": "ifx_protein_ids",
            "path": path,
            "records": len(slim),
            "excluded_columns": drop_cols
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
    parser.add_argument("--config", type=str,
               default="config/targets_config.yaml",
               help="YAML config (default: config/targets_config.yaml)")

    args = parser.parse_args()
    cfg = yaml.safe_load(open(args.config))
    processor = ProteinDataProcessor(cfg)
    processor.run()