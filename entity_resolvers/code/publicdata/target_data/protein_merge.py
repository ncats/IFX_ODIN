#!/usr/bin/env python
"""
protein_merge.py

Merge protein provenance from UniProt, Ensembl, RefSeq, and NodeNorm.
Uses UniProt as the "truth" and then layers Ensembl, RefSeq, and NodeNorm on top.
Outputs a harmonized table and flags mismatches for review.
"""

import os
import re
import json
import yaml
import logging
import argparse
import itertools
import pandas as pd
import numpy as np
from datetime import datetime

# suppress urllib3 warnings if available
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except ImportError:
    pass


def setup_logging(log_file=None):
    root = logging.getLogger()
    if not root.handlers:
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


def parse_versionless_ids(val):
    """Split on |, drop empty or 'nan', strip versions (.1) and return set."""
    if not isinstance(val, str) or not val.strip() or val.strip().lower() == "nan":
        return set()
    items = [x.strip() for x in val.split("|") if x.strip() and x.strip().lower() != "nan"]
    return {re.split(r"\.\d+$", it)[0] for it in items}


def process_ensembl_raw(iso: pd.DataFrame) -> pd.DataFrame:
    # 1) Keep only the columns you care about
    cols = [
        'ensembl_transcript_id_version',
        'ensembl_peptide_id_version',
        'ensembl_symbol',
        'ensembl_canonical',
        'ensembl_transcript_tsl',
        'ensembl_uniprot_id',
        'ensembl_trembl_id',
        'ensembl_uniprot_isoform',
        'ensembl_refseq_MANEselect',
        'ensembl_refseq_NP',
        'ensembl_description'
    ]
    df = iso.loc[:, [c for c in cols if c in iso.columns]].copy()

    # 2) Drop rows missing peptide ID
    df = df[df['ensembl_peptide_id_version'].notna() & (df['ensembl_peptide_id_version'] != '')]

    # 3) Consolidate trembl --> uniprot_id, duplicating if mismatch
    def expand_uniprot(row):
        up = row['ensembl_uniprot_id']
        tr = row['ensembl_trembl_id']
        if not up and tr:
            row['ensembl_uniprot_id'] = tr
            return [row]
        if up and tr:
            if up == tr:
                return [row]
            r1 = row.copy(); r1['ensembl_uniprot_id'] = up
            r2 = row.copy(); r2['ensembl_uniprot_id'] = tr
            return [r1, r2]
        return [row]

    df = pd.concat(
        df.apply(lambda r: pd.DataFrame(expand_uniprot(r)), axis=1).tolist(),
        ignore_index=True
    )

    # 4) Append '-1' for canonical flag
    def append_canonical(pid, flag):
        return f"{pid}-{int(flag)}" if str(flag) == '1' else pid

    df['ensembl_peptide_id_version'] = df.apply(
        lambda r: append_canonical(r['ensembl_peptide_id_version'], r['ensembl_canonical']),
        axis=1
    )

    # 5) Group by symbol, uniprot_id, description - keep isoforms separate
    isoform_mask = df['ensembl_uniprot_isoform'].notna() & (df['ensembl_uniprot_isoform'] != '')
    normal = df[~isoform_mask]
    isoforms = df[isoform_mask]

    def concat_group(g):
        return pd.Series({
            'ensembl_transcript_id_version': '|'.join(g['ensembl_transcript_id_version']),
            'ensembl_peptide_id_version'   : '|'.join(g['ensembl_peptide_id_version']),
            'ensembl_symbol'               : g['ensembl_symbol'].iat[0],
            'ensembl_transcript_tsl'       : g['ensembl_transcript_tsl'].iat[0],
            'ensembl_uniprot_id'           : g['ensembl_uniprot_id'].iat[0],
            'ensembl_uniprot_isoform'      : '',
            'ensembl_refseq_MANEselect'    : '|'.join(filter(None, g['ensembl_refseq_MANEselect'].fillna('').tolist())),
            'ensembl_refseq_NP'            : '|'.join(filter(None, g['ensembl_refseq_NP'].fillna('').tolist())),
            'ensembl_description'          : g['ensembl_description'].iat[0]
        })

    grouped = (
        normal
        .groupby(['ensembl_symbol','ensembl_uniprot_id','ensembl_description'], sort=False)
        .apply(concat_group)
        .reset_index(drop=True)
    )

    result = pd.concat([grouped, isoforms.drop(columns=['ensembl_canonical'])], ignore_index=True, sort=False)
    return result

class ProteinResolver:
    def __init__(self, config):
        self.cfg = config.get("protein_merge", config)
        setup_logging(self.cfg.get("log_file"))
        self._load_paths()
        self.qc_mode = config.get('global', {}).get('qc_mode', True)
        self.metadata = {"steps": [], "start": datetime.now().isoformat()}
        logging.info("Initializing ProteinResolver")
        self.ens_cols = {
            'ensembl':  'ensembl_peptide_id_version',
            'refseq':   'refseq_ensembl_protein_id',
            'uniprot':  'uniprot_ensembl_pro',
            'nodenorm': 'nodenorm_ensembl_protein_id'
        }
        self.ref_cols = {
            'ensembl': 'ensembl_refseq_NP',
            'refseq':  'refseq_protein_id_merged',
            'uniprot': 'uniprot_xref_RefSeq'
        }
        self.uni_cols = {
            'ensembl':  'ensembl_uniprot_id',
            'refseq':   'refseq_uniprot_id',
            'nodenorm': 'nodenorm_uniprot_id',
            'uniprot':  'uniprot_id'
        }

    def _load_paths(self):
        c = self.cfg
        self.paths = {
            'ensembl_isoform': c['ensembl_isoform_csv'],
            'refseq_uniprot':  c['refseq_uniprot_csv'],
            'refseq_ensembl':  c['refseq_ensembl_csv'],
            'uniprot_map':     c['uniprot_mapping_csv'],
            'uniprot_info':    c['uniprot_info_csv'],
            'nodenorm':        c['nodenorm_file'],
            'output':          c['transformed_data_path'],
            'metadata':        c['metadata_file']
        }

    def _log_step(self, name, desc, details=None):
        entry = {'step': name, 'desc': desc, 'time': datetime.now().isoformat()}
        if details:
            entry['details'] = details
        self.metadata['steps'].append(entry)

    def _get_str(self, val):
        if pd.isnull(val):
            return ''
        return str(val).strip()

    def run(self):
        iso, r2, r3, um, ui = self._read_sources()
        df = self._uniprot_centric(um, ui)
        df = self._merge_ensembl(df, iso)
        df = self._merge_refseq(df, r2, r3)
        df = self._merge_nodenorm(df)
        self._save_merge_qc_snapshot(df)
        df = self._fill_from_parent(df)
        df = self._merge_fuzzy_duplicates(df)
        df = self.apply_match_status_logic(df)
        df = self.calculate_mapping_scores(df)
        pruned = self.prune_columns(df)
        flagged = self.flag_for_review(pruned)
        self.count_match_statuses(flagged)
        self.count_match_statuses_multi(
            flagged,
            domain_definitions=[
                ("Ensembl", "ensembl_peptide_id_version", "refseq_ensembl_protein_id", "uniprot_ensembl_transcript", "nodenorm_ensembl_protein_id"),
                ("RefSeq",  "ensembl_refseq_NP", "refseq_protein_id_merged", "uniprot_xref_RefSeq"),
                ("UniProt", "ensembl_uniprot_id", "refseq_uniprot_id", "uniprot_id", "nodenorm_uniprot_id")
            ]
        )
        self._save(flagged)

    def _read_sources(self):
        logging.info("Reading input tables...")
        iso = pd.read_csv(self.paths['ensembl_isoform'], dtype=str).replace('nan', np.nan)
        r2  = pd.read_csv(self.paths['refseq_uniprot'], dtype=str).replace('nan', np.nan)
        r3  = pd.read_csv(self.paths['refseq_ensembl'], dtype=str).replace('nan', np.nan)
        um  = pd.read_csv(self.paths['uniprot_map'], dtype=str).replace('nan', np.nan)
        ui  = pd.read_csv(self.paths['uniprot_info'], dtype=str).replace('nan', np.nan)
        shapes = {
            'ensembl_isoform': iso.shape,
            'refseq_uniprot':   r2.shape,
            'refseq_ensembl':   r3.shape,
            'uniprot_map':      um.shape,
            'uniprot_info':     ui.shape
        }
        self._log_step("read_sources", "Loaded source tables", shapes)
        return iso, r2, r3, um, ui

    def _uniprot_centric(self, um, ui):
        logging.info("Starting with UniProt as our standard protein ID")
        cols = ['uniprot_id', 'uniprot_recommended_name','uniprot_ensembl_trs','uniprot_ensembl_pro',
                'uniprot_is_canonical','uniprot_entryType','uniprot_gene_name',
                'uniprot_secondaryAccessions','uniprot_annotationScore']
        info = ui[cols]
        merged = um[['uniprot_id','uniprot_xref_Ensembl','uniprot_xref_RefSeq','uniprot_NCBI_id']]\
                 .merge(info, on='uniprot_id', how='outer')
        merged['uniprot_ensembl_pro'] = merged['uniprot_ensembl_pro'].fillna('')
        def combine_trs(r):
            parts = []
            parts += str(r.get('uniprot_xref_Ensembl','')).split('|')
            parts += str(r.get('uniprot_ensembl_trs','')).split('|')
            clean = [p for p in sorted(set(parts)) if p and p.lower()!='nan']
            return '|'.join(clean)
        merged['uniprot_ensembl_transcript'] = merged.apply(combine_trs, axis=1)
        merged.drop(['uniprot_xref_Ensembl','uniprot_ensembl_trs'], axis=1, inplace=True)
        self._log_step('_uniprot_centric','Unified UniProt xrefs', {'rows': merged.shape[0]})
        return merged

    def _merge_ensembl(self, df, iso):
        logging.info("Starting Ensembl merge (peptide-first, fallback on uniprot_id)")

        # DIAGNOSTIC #1: raw file unique peptide IDs
        raw_peps = (
            iso['ensembl_peptide_id_version']
                .dropna()
                .astype(str)
                .str.split("|")
                .explode()
                .str.split("-").str[0]
        )
        logging.info(
            "Raw Ensembl file → %d total peptide entries, %d unique IDs",
            len(raw_peps),
            raw_peps.nunique()
        )

        # 1) Clean & tag raw Ensembl table
        processed = process_ensembl_raw(iso)

        # DIAGNOSTIC #2: post‐processing unique peptide IDs
        post_peps = (
            processed['ensembl_peptide_id_version']
                .str.split("|")
                .explode()
                .str.split("-").str[0]
        )
        logging.info(
            "Post-processing → %d total peptide entries, %d unique IDs",
            len(post_peps),
            post_peps.nunique()
        )

        # 2) Build base list (strip off any "-<digit>" suffix) and explode
        processed['pep_base_list'] = (
            processed['ensembl_peptide_id_version']
                .str.split("|")
                .apply(lambda parts: [p.split("-")[0] for p in parts])
        )
        proc_expl = processed.explode('pep_base_list').rename(columns={'pep_base_list': 'pep_id'})

        # 3) Explode the UniProt side on peptides
        df_pep = (
            df
            .assign(pep_id=df['uniprot_ensembl_pro'].fillna("").str.split("|"))
            .explode('pep_id')
            .query("pep_id != ''")
        )
        df_pep['_idx'] = df_pep.index

        # 4) First-pass merge on peptide
        m1 = pd.merge(df_pep, proc_expl, on='pep_id', how='outer', suffixes=("", "_ens"))
        m1['merge_method'] = 'peptide'

        # 5) Fallback on uniprot_id ⇄ ensembl_uniprot_id
        need_fb = m1['ensembl_symbol'].isna()
        fb = m1[need_fb].drop(columns=['pep_id'] + [c for c in m1.columns if c.endswith("_ens")], errors='ignore')
        proc2 = processed.rename(columns={'ensembl_uniprot_id': 'pep_id2'})
        m2 = pd.merge(
            fb,
            proc2,
            left_on='uniprot_id',
            right_on='pep_id2',
            how='outer',
            suffixes=("", "_ens2")
        )
        for c in processed.columns:
            if c == 'ensembl_peptide_id_version':
                continue
            src = f"{c}_ens2"
            if src in m2.columns:
                m2[c] = m2[src]
        m2['merge_method'] = 'uniprot'

        # 6) Combine both passes & pick highest-priority match
        allm = pd.concat([m1[~need_fb], m2], ignore_index=True, sort=False)
        allm['rank'] = allm['merge_method'].map({'peptide': 0, 'uniprot': 1})
        best = (
            allm
            .sort_values(['_idx', 'rank'])
            .groupby('_idx', sort=False)
            .first()
            .reset_index()
        )

        # 7) Drop helper columns
        best = best.drop(columns=['pep_id', 'pep_id2', 'merge_method', 'rank'], errors='ignore')

        # 8) Re-attach any original rows that never had a peptide match
        original = df.reset_index().rename(columns={'index': '_idx'})
        full = original.merge(best, how='left', on='_idx', suffixes=("", "_ens"))
        full = full.drop(columns=['_idx'], errors='ignore')

        logging.info(
            "Ensembl merge: %d rows matched, %d total after reattachment",
            best.shape[0],
            full.shape[0]
        )
        self._log_step(
            '_merge_ensembl',
            'Merged Ensembl (peptide-first, fallback on uniprot_id) with full reattachment',
            {'matched': best.shape[0], 'total': full.shape[0]}
        )

        return full

    def _merge_refseq(self, df, r2, r3):
        logging.info("Starting RefSeq merge")
        r2c = (r2.rename(columns={
                    '#NCBI_protein_accession':'refseq_protein_id',
                    'UniProtKB_protein_accession':'refseq_uniprot_id'
                 }).fillna('').astype(str)
               .groupby('refseq_uniprot_id')
               .agg(
                  refseq_protein_id_merged=pd.NamedAgg('refseq_protein_id',
                                                       lambda x: "|".join(sorted(set(x)))),
                  refseq_method=pd.NamedAgg('method','first')
               ).reset_index())

        out = df.merge(r2c,
                       left_on='uniprot_id',
                       right_on='refseq_uniprot_id',
                       how='left')

        xref = (r3.rename(columns={
                        'protein_accession.version':'refseq_protein_id',
                        'Ensembl_protein_identifier':'refseq_ensembl_protein_id'
                     }).fillna('').astype(str)
               [['refseq_protein_id','refseq_ensembl_protein_id']]
               .drop_duplicates())

        out = out.merge(xref,
                        left_on='refseq_protein_id_merged',
                        right_on='refseq_protein_id',
                        how='left')

        #logging.info("Columns after _merge_refseq: %s", out.columns.tolist())
        self._log_step('_merge_refseq', 'Merged RefSeq', {'rows': out.shape[0]})
        return out

    def _merge_nodenorm(self, df):
        logging.info("Starting NodeNorm merge")

        # 1) Read & clean NodeNorm, drop exact duplicates there
        nn = (
            pd.read_csv(self.paths['nodenorm'], dtype=str)
            .fillna('')
            .astype(str)
        )
        nn['nodenorm_ensembl_protein_id'] = (
            nn['nodenorm_ensembl_protein_id']
            .apply(lambda v: v.split("|")[-1])
        )
        nn = (
            nn[['nodenorm_uniprot_id',
                'NodeNorm_Protein',
                'nodenorm_name',
                'nodenorm_ensembl_protein_id',
                'nodenorm_UMLS']]
            .drop_duplicates()
        )
        logging.info("NodeNorm lookup: %d unique rows", nn.shape[0])

        # 2) Merge
        out = df.merge(
            nn,
            left_on='uniprot_id',
            right_on='nodenorm_uniprot_id',
            how='left'
        )

        # 3) Make any list-cells hashable by converting them to tuples
        for col in out.columns:
            if out[col].apply(lambda x: isinstance(x, list)).any():
                out[col] = out[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)

        # 4) Drop fully identical duplicates
        before = out.shape[0]
        out = out.drop_duplicates()
        after = out.shape[0]
        logging.info("After merge dedupe: %d → %d rows", before, after)

        self._log_step(
            '_merge_nodenorm',
            'Merged NodeNorm (deduped lookup + drop duplicates post-merge)',
            {'rows_before': before, 'rows_after': after}
        )
        return out

    def _save_merge_qc_snapshot(self, df):
        if not self.qc_mode:
            return
        """Save select columns from the merged dataframe before parent-filling."""
        cols = [
            'uniprot_id',
            'uniprot_recommended_name',
            'uniprot_is_canonical',
            'uniprot_entryType',
            'uniprot_gene_name',
            'ensembl_uniprot_id',
            'ensembl_uniprot_isoform',
            'ensembl_refseq_MANEselect'
        ]
        export_df = df[[col for col in cols if col in df.columns]].copy()
        out_path = "src/data/publicdata/target_data/qc/protein_merge.qc.csv"
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        export_df.to_csv(out_path, index=False)
        logging.info(f"Saved merge QC snapshot to {out_path}")

    def _fill_from_parent(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        1) Fill selected columns from parent → children as before.
        2) Then: for canonical child rows (uniprot_is_canonical == TRUE)
           that still have a null/empty uniprot_entryType, copy
           parent.uniprot_entryType → child and drop the parent.
        """
        df = df.copy()
        # ---- export ALL rows whose base_id is duplicated ---- #
        # derive “base” ID (strip off any “-N” suffix)
        df['__base_id'] = df['uniprot_id'].fillna('').apply(lambda x: x.split('-', 1)[0])
        # select every row that belongs to a duplicated base
        dup_df = df[df['__base_id'].duplicated(keep=False)]
        if not dup_df.empty:
            export_cols = [
                'uniprot_id',
                'uniprot_is_canonical',
                'uniprot_entryType',
                'uniprot_gene_name',
                'ensembl_uniprot_id',
                'ensembl_uniprot_isoform',
                'ensembl_refseq_MANEselect',
                '__base_id'
            ]
            export_df = dup_df[[col for col in export_cols if col in dup_df.columns]]
            qc_path = self.cfg.get(
                'qc_file',
                os.path.join(os.path.dirname(self.paths['output']), 'uniprot_baseid_duplicates.csv'))
            os.makedirs(os.path.dirname(qc_path), exist_ok=True)
        if self.qc_mode:
            logging.info(f"Duplicated base_id rows: {len(dup_df)}")
            export_df.to_csv(qc_path, index=False)
            logging.info(f"Exported {len(export_df)} rows with duplicated base_id to {qc_path}")
        else:
            logging.info("QC mode disabled – skipping export of duplicated base_id rows")

        # ---- Part 1: fill various cols from parent into children ---- #
        cols = [
            'uniprot_annotationScore', 'refseq_uniprot_id', 'nodenorm_uniprot_id',
            'uniprot_xref_RefSeq', 'refseq_protein_id_merged', 'refseq_method',
            'uniprot_ensembl_pro', 'ensembl_peptide_id_version',
            'refseq_ensembl_protein_id', 'nodenorm_ensembl_protein_id',
            'uniprot_gene_name', 'uniprot_NCBI_id', 'uniprot_recommended_name',
            'nodenorm_name', 'NodeNorm_Protein', 'nodenorm_UMLS'
        ]

        # derive “base” ID (strip off any “-N” suffix)
        df['__base_id'] = df['uniprot_id'].fillna('').apply(lambda x: x.split('-', 1)[0])

        # lookup parents (keep first if multiple)
        parents = (
            df[df['uniprot_id'] == df['__base_id']]
            .set_index('__base_id')
        ).loc[~pd.Index(df[df['uniprot_id'] == df['__base_id']]['__base_id']).duplicated()]

        def _fill(r):
            base = r['__base_id']
            if base in parents.index:
                p = parents.loc[base]
                for c in cols:
                    if (pd.isna(r.get(c)) or str(r.get(c)).strip() == '') and c in p:
                        r[c] = p[c]
            return r

        df = df.apply(_fill, axis=1)

        # ---- Part 2: STRICTLY promote entryType only into canonical child & drop parent ---- #
        to_drop = []
        for base, grp in df.groupby('__base_id'):
            # parent row
            parent_rows = grp[grp['uniprot_id'] == base]
            if parent_rows.empty:
                continue
            pi = parent_rows.index[0]

            # only consider children flagged canonical (1), missing entryType
            # robustly detect canonical children
            is_canon = grp['uniprot_is_canonical']\
                            .astype(str)\
                            .str.lower()\
                            .isin(['true','1','yes'])

            missing_et = grp['uniprot_entryType'].fillna('') == ''

            children = grp.loc[(grp['uniprot_id'] != base) & is_canon & missing_et]

            if children.empty:
                continue

            # copy entryType from parent into each canonical child
            entry_type = df.at[pi, 'uniprot_entryType']
            df.loc[children.index, 'uniprot_entryType'] = entry_type

            # schedule parent for removal
            to_drop.append(pi)

        if to_drop:
            df = df.drop(index=to_drop)

        # clean up helper and return
        return df.drop(columns='__base_id')

    def _merge_fuzzy_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Find rows that are identical on every column *except*
        refseq_ensembl_protein_id, and collapse them into one
        by concatenating all of their refseq_ensembl_protein_id values.
        """
        # 1) determine which columns to use as the “key”
        key_cols = [c for c in df.columns if c != 'refseq_ensembl_protein_id']

        # 2) find every row that has a duplicate on those keys
        dup_mask = df.duplicated(subset=key_cols, keep=False)
        dup_df   = df[dup_mask]
        if dup_df.empty:
            logging.info("No fuzzy duplicates on %s", key_cols)
            return df

        # 3) group them, concatenating refseq_ensembl_protein_id
        merged = (
            dup_df
            .groupby(key_cols, dropna=False, as_index=False)
            .agg({
                'refseq_ensembl_protein_id': 
                    lambda vals: "|".join(sorted(set(v for v in vals if v)))
            })
        )

        # 4) stitch back the non-duplicates + the merged rows
        nondup = df[~dup_mask]
        result = pd.concat([nondup, merged], ignore_index=True, sort=False)

        logging.info(
            "Fuzzy-merged %d duplicate rows into %d unique rows",
            len(dup_df), len(merged)
        )
        return result

    def apply_match_status_logic(self, df):
        def clean_ids(val):
            """
            Given a string like "ENSP00000485316.2-1|ENSP00000485491.1", 
            split on "|" and strip any trailing ".<digits>" or "-<digits>".
            """
            ids = set()
            for part in str(val).split("|"):
                p = part.strip()
                if not p or p.lower() == "nan":
                    continue
                # remove either .<digits> or -<digits> at the end
                p = re.sub(r'[-\.]\d+$', '', p)
                ids.add(p)
            return ids

        def get_provenance(r, cols_to_src):
            prov = {}
            # build source→set(ids) map
            for src, col in cols_to_src.items():
                ids = clean_ids(r.get(col, ""))
                if ids:
                    prov[src] = ids

            if not prov:
                return None
            if len(prov) == 1:
                # only one source had any IDs
                return next(iter(prov))

            # if any pair of sources shares at least one cleaned ID, it's a match
            for (s1, set1), (s2, set2) in itertools.combinations(prov.items(), 2):
                if set1 & set2:
                    # list _all_ the contributing sources
                    return ", ".join(sorted(prov.keys()))

            # no overlap between _any_ two sources → real mismatch
            return "error"

        # now apply to each domain
        df['Ensembl_ID_Provenance'] = df.apply(
            lambda r: get_provenance(r, self.ens_cols), axis=1
        )
        df['RefSeq_ID_Provenance'] = df.apply(
            lambda r: get_provenance(r, self.ref_cols), axis=1
        )
        df['UniProt_ID_Provenance'] = df.apply(
            lambda r: get_provenance(r, self.uni_cols), axis=1
        )

        self._log_step(
            'apply_provenance',
            'Computed provenance (stripping “.N” and “-N” tags) and only returning "error" when no two sources share any ID',
            {'rows': df.shape[0]}
        )
        return df

    def count_match_statuses(self, df, counts_csv=None):
        total_ens = len(df['ensembl_peptide_id_version'].dropna().unique())
        total_ref = len(df['refseq_ensembl_protein_id'].dropna().unique())
        total_uni = len(df['uniprot_id'].dropna().unique())
        counts = {
            'Total Rows': len(df),
            'Unique Ensembl Protein IDs': total_ens,
            'Unique RefSeq Protein IDs': total_ref,
            'Unique UniProt Protein IDs': total_uni
        }
        for k,v in counts.items():
            logging.info(f"{k}: {v}")
        
        if self.qc_mode:
            dfc = pd.DataFrame(list(counts.items()), columns=['Metric','Count'])
            if not counts_csv:
                counts_csv = os.path.join(os.path.dirname(self.paths['output']), 'protein_mappingstats.qc.csv')
            os.makedirs(os.path.dirname(counts_csv), exist_ok=True)
            dfc.to_csv(counts_csv, index=False)
            logging.info("Match status counts saved to %s", counts_csv)
        else:
            logging.info("QC mode disabled – skipping mapping stats write")

    def count_match_statuses_multi(self, df, domain_definitions, counts_csv=None):
        def count_nonempty(col):
            return df[col].notna().astype(int).sum()

        rows = []
        for definition in domain_definitions:
            label = definition[0]
            prov_col = f"{label}_ID_Provenance"
            if len(definition) == 4:
                _, ce, cr, cu = definition
                cols = [ce, cr, cu]
                sources = ['ensembl','refseq','uniprot']
            else:
                _, ce, cr, cu, cn = definition
                cols = [ce, cr, cu, cn]
                sources = ['ensembl','refseq','uniprot','nodenorm']

            mask_match = df[prov_col].notna() & (df[prov_col] != 'error')
            match_cnt = int(mask_match.sum())
            error_cnt = int((df[prov_col] == 'error').sum())
            only = {}
            for src, col in zip(sources, cols):
                mask_only = df.apply(
                    lambda r, c=col: (self._get_str(r[c])!='') and
                                    all(self._get_str(r[o])=='' for o in cols if o!=c),
                    axis=1
                )
                only[f"{src}_only"] = int(mask_only.sum())
            totals = {f"{src}_total": count_nonempty(col) for src,col in zip(sources, cols)}
            rows.append({
                'column': label,
                'match':  match_cnt,
                'error':  error_cnt,
                **only,
                **totals
            })

        result = pd.DataFrame(rows)

        if self.qc_mode:
            if counts_csv is None:
                counts_csv = os.path.join(
                    os.path.dirname(self.paths['output']),
                    'protein_multi_domain_match_stats.qc.csv'
                )
            os.makedirs(os.path.dirname(counts_csv), exist_ok=True)
            result.to_csv(counts_csv, index=False)
            logging.info("Computed multi-domain match stats and saved to %s", counts_csv)
        else:
            logging.info("QC mode disabled – skipping multi-domain stats write")

        return result

    def calculate_mapping_scores(self, df):
        def score(v, m):
            return 0 if not v or v=='error' else min(len(v.split(',')), m)

        df['Ensembl_Mapping_Score'] = df['Ensembl_ID_Provenance'].apply(lambda x: score(x,4))
        df['RefSeq_Mapping_Score']   = df['RefSeq_ID_Provenance'].apply(lambda x: score(x,3))
        df['UniProt_Mapping_Score']  = df['UniProt_ID_Provenance'].apply(lambda x: score(x,4))
        df['Total_Mapping_Score']    = df[
            ['Ensembl_Mapping_Score','RefSeq_Mapping_Score','UniProt_Mapping_Score']
        ].sum(axis=1)
        df['Total_Mapping_Ratio']    = df['Total_Mapping_Score']/11.0

        self._log_step('calculate_scores','Assigned numeric mapping scores')
        return df

    def prune_columns(self, df):
        keep = ['uniprot_entryType','uniprot_annotationScore','ensembl_transcript_tsl',
            'uniprot_id','ensembl_uniprot_id','refseq_uniprot_id','nodenorm_uniprot_id',
            'uniprot_xref_RefSeq','ensembl_refseq_NP',
            'refseq_protein_id_merged','refseq_method',
            'uniprot_ensembl_pro','ensembl_peptide_id_version',
            'refseq_ensembl_protein_id','nodenorm_ensembl_protein_id',
            'ensembl_symbol','uniprot_gene_name','uniprot_NCBI_id',
            'ensembl_refseq_MANEselect','ensembl_canonical',
            'ensembl_uniprot_isoform','SPARQL_uniprot_isoform',
            'uniprot_is_canonical','ensembl_transcript_id_version',
            'uniprot_ensembl_transcript', 'uniprot_recommended_name','ensembl_description',
            'nodenorm_name','NodeNorm_Protein','nodenorm_UMLS',
            'Ensembl_ID_Provenance','RefSeq_ID_Provenance','UniProt_ID_Provenance',
            'Ensembl_Mapping_Score','RefSeq_Mapping_Score','UniProt_Mapping_Score',
            'Total_Mapping_Score','Total_Mapping_Ratio'
        ]
        keep = [c for c in keep if c in df.columns]
        pruned = df[keep].copy()
        self._log_step('prune','Pruned to selected columns')
        return pruned

    def flag_for_review(self, df):
        # 1) identify mismatches by looking for "error" in any provenance column
        mask = (
            (df['Ensembl_ID_Provenance'] == 'error') |
            (df['RefSeq_ID_Provenance']  == 'error') |
            (df['UniProt_ID_Provenance'] == 'error')
        )
        errs = df.loc[mask, [
            'uniprot_id',
            'ensembl_uniprot_id',
            'refseq_uniprot_id',
            'nodenorm_uniprot_id',
            'uniprot_xref_RefSeq',
            'ensembl_refseq_NP',
            'refseq_protein_id_merged',
            'uniprot_ensembl_pro',
            'ensembl_peptide_id_version',
            'refseq_ensembl_protein_id',
            'nodenorm_ensembl_protein_id',
            'ensembl_transcript_id_version',
            'uniprot_ensembl_transcript',
            'Ensembl_ID_Provenance',
            'RefSeq_ID_Provenance',
            'UniProt_ID_Provenance',
        ]]

        # 2) write out only those columns
        out = os.path.join(
            os.path.dirname(self.paths['output']),
            'protein_flagged_for_review.csv'
        )
        os.makedirs(os.path.dirname(out), exist_ok=True)
        if self.qc_mode:
            errs.to_csv(out, index=False)

        self._log_step(
            'flag',
            'Flagged rows for QC based on "error" provenance',
            {'count': len(errs)}
        )
        return df

    def _save(self, df):
        # remove any fully‐identical duplicate rows
        df = df.drop_duplicates()
        os.makedirs(os.path.dirname(self.paths['output']), exist_ok=True)
        df.to_csv(self.paths['output'], index=False)
        with open(self.paths['metadata'], 'w') as f:
            json.dump(self.metadata, f, indent=2)
        logging.info("Saved merged data and metadata")

if __name__=='__main__':
    parser = argparse.ArgumentParser(
        description="Merge protein sources with provenance & metadata")
    parser.add_argument('--config', required=True, help='YAML config file')
    args = parser.parse_args()
    cfg = yaml.safe_load(open(args.config))
    ProteinResolver(cfg).run()