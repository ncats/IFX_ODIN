#!/usr/bin/env python
"""
gene_merge.py - Merge and map gene IDs across Ensembl, NCBI, HGNC, (optional) NodeNorm
with detailed metadata, logging, unified diffs, and progress bars.

Usage:
    python gene_merge.py --config path/to/your_config.yaml
"""
import os
import json
import yaml
import difflib
import logging
import argparse
import pandas as pd
from datetime import datetime
from logging.handlers import RotatingFileHandler
from tqdm import tqdm

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

def generate_diff(old_path, new_path, base_name):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    diff_txt  = f"{base_name}_{ts}.diff.txt"
    diff_html = f"{base_name}_{ts}.diff.html"
    with open(old_path, 'r', errors='ignore') as fo, open(new_path, 'r', errors='ignore') as fn:
        old_lines, new_lines = fo.readlines(), fn.readlines()
    uni = ''.join(difflib.unified_diff(old_lines, new_lines,
                                       fromfile=old_path, tofile=new_path))
    with open(diff_txt, 'w') as d:
        d.write(uni)
    html = difflib.HtmlDiff().make_file(
        old_lines, new_lines,
        fromdesc=old_path, todesc=new_path,
        context=True, numlines=0
    )
    with open(diff_html, 'w') as h:
        h.write(html)
    return diff_txt, diff_html

class GENEDataMerger:
    def __init__(self, full_cfg):
        logging.info("🚀 Initializing GENEDataMerger")
        self.config = full_cfg["gene_merge"] 
        self.qc_mode = full_cfg.get("global", {}).get("qc_mode", True) 
        logging.info(f"QC Mode: {self.qc_mode}")
        self.metadata = {
            "timestamp": {"start": str(datetime.now())},
            "data_sources": [],
            "processing_steps": [],
            "outputs": []
        }

    def read_and_clean_files(self):
        logging.info("STEP 1: read_and_clean_files")
        print("🔄 Reading and cleaning source CSVs…")
        ensembl_file = self.config['ensembl_file']
        ncbi_file     = self.config['ncbi_file']
        hgnc_file     = self.config['hgnc_file']
        nodenorm_file = self.config.get('nodenorm_file', None)

        ensembl_df = pd.read_csv(ensembl_file, dtype=str)
        ncbi_df    = pd.read_csv(ncbi_file,    dtype=str)
        hgnc_df    = pd.read_csv(hgnc_file,    dtype=str)
        if nodenorm_file:
            nodenorm_df = pd.read_csv(nodenorm_file, dtype=str)
            nodenorm_df['nodenorm_NCBI_id'] = nodenorm_df['nodenorm_NCBI_id'].fillna('').str.strip()
        else:
            nodenorm_df = None

        # --- your exact column‐filter + de‐dupe logic here ---
        ensembl_df = ensembl_df[[
            'ensembl_gene_id','ensembl_NCBI_id','ensembl_hgnc_id','ensembl_symbol',
            'ensembl_gene_type','ensembl_description','ensembl_location','ensembl_synonyms'
        ]].drop_duplicates()

        ncbi_df = ncbi_df[[
            'ncbi_ensembl_gene_id','ncbi_NCBI_id','ncbi_hgnc_id','ncbi_symbol',
            'ncbi_gene_type','ncbi_description','ncbi_location','ncbi_synonyms','ncbi_mim_id'
        ]].drop_duplicates().replace('-', '')

        hgnc_df = hgnc_df[[
            'hgnc_ensembl_gene_id','hgnc_NCBI_id','hgnc_hgnc_id','hgnc_symbol',
            'hgnc_gene_type','hgnc_description','hgnc_location','hgnc_synonyms','hgnc_omim_id'
        ]].drop_duplicates()

        # strip & normalize
        for df, col in [
            (ensembl_df,'ensembl_NCBI_id'),
            (ncbi_df,   'ncbi_NCBI_id'),
            (hgnc_df,   'hgnc_NCBI_id')
        ]:
            df[col] = df[col].fillna('').str.strip()
            df[col] = df[col].apply(lambda x: str(int(float(x))) if x.replace('.', '', 1).isdigit() else x)

        stats_csv = self.config['sources_file']
        if self.qc_mode:
            self.generate_cleaned_file_stats({
                "Ensembl":  ensembl_df,
                "NCBI":     ncbi_df,
                "HGNC":     hgnc_df,
                "NodeNorm": nodenorm_df
            }, stats_csv)
            logging.info(f"  ✅ Cleaned-source stats written to {stats_csv}")

        self.metadata['data_sources'] = [
            ensembl_file, ncbi_file, hgnc_file
        ] + ([nodenorm_file] if nodenorm_file else [])
        if self.qc_mode:
            self.metadata["outputs"].append({
                "name": "gene_source_stats",
                "path": stats_csv,
                "generated_at": str(datetime.now())
            })
        self.metadata["processing_steps"].append({
            "step": "read_and_clean_files",
            "duration_seconds": (datetime.now() - datetime.fromisoformat(
                self.metadata["timestamp"]["start"]
            )).total_seconds(),
            "records": {
                "ensembl": len(ensembl_df),
                "ncbi":    len(ncbi_df),
                "hgnc":    len(hgnc_df),
                "nodenorm": len(nodenorm_df) if nodenorm_df is not None else None
            }
        })

        print(f"  → Loaded Ensembl: {len(ensembl_df)} rows, "
              f"NCBI: {len(ncbi_df)} rows, HGNC: {len(hgnc_df)} rows")
        if nodenorm_df is not None:
            print(f"  → Loaded NodeNorm: {len(nodenorm_df)} rows")
        return ensembl_df, ncbi_df, hgnc_df, nodenorm_df

    def generate_cleaned_file_stats(self, dataframes, output_csv):
        stats_list = []
        for label, df in dataframes.items():
            if df is not None:
                for col in df.columns:
                    stats_list.append({
                        "file": label,
                        "column": col,
                        "unique_values": df[col].nunique()
                    })
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        pd.DataFrame(stats_list).to_csv(output_csv, index=False)

    def merge_ensembl_ncbi(self, df1, df2):
        logging.info("STEP 2: merge_ensembl_ncbi")
        print("🔄 Merging Ensembl ↔ NCBI…")
        merged = pd.merge(
            df1, df2,
            left_on='ensembl_gene_id', right_on='ncbi_ensembl_gene_id',
            how='outer', suffixes=('_ensembl','_ncbi')
        )[
            ['ensembl_gene_id','ncbi_ensembl_gene_id',
             'ensembl_NCBI_id','ncbi_NCBI_id',
             'ensembl_hgnc_id','ncbi_hgnc_id',
             'ensembl_symbol','ncbi_symbol',
             'ensembl_gene_type','ncbi_gene_type',
             'ensembl_description','ncbi_description',
             'ensembl_location','ncbi_location',
             'ensembl_synonyms','ncbi_synonyms','ncbi_mim_id']
        ]
       # out = "src/data/publicdata/target_data/qc/ncbi_ensembl_merge.csv"
        #os.makedirs(os.path.dirname(out), exist_ok=True)
        #merged.to_csv(out, index=False)
        #logging.info(f"  ✅ Ensembl↔NCBI merge saved to {out}")
        self.metadata["processing_steps"].append({
            "step": "merge_ensembl_ncbi",
            "duration_seconds": 0,
            "records": len(merged)
        })
        return merged

    def filter_and_clean_rows(self, merged_df):
        logging.info("  → Filtering mismatched Ensembl↔NCBI rows")
        mismatched = merged_df[
            (merged_df['ensembl_hgnc_id'] != merged_df['ncbi_hgnc_id']) &
            (~merged_df['ensembl_hgnc_id'].isnull()) &
            (~merged_df['ncbi_hgnc_id'].isnull())
        ]
        clean_df = merged_df.drop(mismatched.index)
        valid_ncbi = clean_df[clean_df['ncbi_hgnc_id'].isin(clean_df['ensembl_hgnc_id'])]
        filtered = valid_ncbi.loc[
            (valid_ncbi['ensembl_gene_id'].isnull()) &
            (valid_ncbi['ncbi_hgnc_id'].notnull()) &
            (valid_ncbi['ncbi_hgnc_id'].isin(valid_ncbi['ensembl_hgnc_id']))
        ]
        matched = clean_df[clean_df['ensembl_hgnc_id'].isin(filtered['ncbi_hgnc_id'])]
        return pd.concat([filtered, matched]).drop_duplicates()

    def fill_missing_ncbi_data(self, final_rows):
        logging.info("STEP 3: fill_missing_ncbi_data (vectorized)…")
        print("🔄 Filling missing NCBI data…")

        # build lookup on existing ncbi_hgnc_id
        lookup = (
            final_rows.loc[final_rows['ncbi_hgnc_id'].notna(),
                           ['ncbi_hgnc_id','ncbi_ensembl_gene_id',
                            'ncbi_NCBI_id','ncbi_symbol','ncbi_gene_type']]
            .drop_duplicates(subset=['ncbi_hgnc_id'])
            .set_index('ncbi_hgnc_id')
        )
        logging.info(f"  → Built lookup of {len(lookup)} HGNC→NCBI mappings")

        mask = (
            final_rows['ncbi_hgnc_id'].isna() &
            final_rows['ensembl_hgnc_id'].notna() &
            final_rows['ncbi_ensembl_gene_id'].isna()
        )
        to_fill = final_rows.loc[mask]
        n_to_fill = mask.sum()
        print(f"  → {n_to_fill} rows need NCBI fill")

        if n_to_fill:
            joined = to_fill.merge(
                lookup, left_on='ensembl_hgnc_id', right_index=True, how='left',
                suffixes=('','_lk')
            )
            for col in ['ncbi_ensembl_gene_id','ncbi_NCBI_id','ncbi_symbol','ncbi_gene_type']:
                final_rows.loc[mask, col] = joined[col + '_lk']
            # bridge the HGNC ID
            final_rows.loc[mask, 'ncbi_hgnc_id'] = joined['ensembl_hgnc_id']
            logging.info(f"  ✅ Filled {n_to_fill} rows of missing NCBI data")

        print("✔️ Completed fill_missing_ncbi_data")
        return final_rows

    def clean_and_update_merged_df(self, merged_df):
        logging.info("STEP 4: clean_and_update_merged_df")
        print("🔄 Filtering & QC of Ensembl↔NCBI mapping…")
        filtered = self.filter_and_clean_rows(merged_df)

        # Save filtered QC file only if qc_mode is True
        if self.qc_mode:
            qc_path = "src/data/publicdata/target_data/qc/unmapped_ensembl_ncbi_4qc.csv"
            os.makedirs(os.path.dirname(qc_path), exist_ok=True)
            filtered.to_csv(qc_path, index=False)
            print(f"  → QC unmapped saved to {qc_path}")

        merged_minus = merged_df.drop(filtered.index)
        filled = self.fill_missing_ncbi_data(filtered)
        final_rows = filled[~filled[['ensembl_gene_id','ensembl_NCBI_id','ensembl_hgnc_id']].isna().all(axis=1)]

        # Save final mapped .qc.csv file only if qc_mode is True
        if self.qc_mode:
            out = "src/data/publicdata/target_data/qc/ensembl_ncbi_mapped.qc.csv"
            os.makedirs(os.path.dirname(out), exist_ok=True)
            pd.concat([merged_minus, final_rows]).drop_duplicates().to_csv(out, index=False)
            print(f"  → Final Ensembl↔NCBI mapped saved to {out}")

        return pd.concat([merged_minus, final_rows]).drop_duplicates()

    def merge_hgnc(self, df, hgnc_df):
        logging.info("STEP 5: merge_hgnc")
        print("🔄 Merging HGNC…")
        merged = pd.merge(
            df, hgnc_df,
            left_on=['ensembl_gene_id','ncbi_NCBI_id'],
            right_on=['hgnc_ensembl_gene_id','hgnc_NCBI_id'],
            how='outer', suffixes=('_e_n','_hgnc')
        )
        return merged

    def unify_mapping_ids(self, df):
        logging.info("STEP 6: unify_mapping_ids")
        print("🔄 Unifying mapping IDs…")
        cols = [
            "ensembl_gene_id","ncbi_ensembl_gene_id","hgnc_ensembl_gene_id",
            "ensembl_NCBI_id","ncbi_NCBI_id","hgnc_NCBI_id",
            "ensembl_hgnc_id","ncbi_hgnc_id","hgnc_hgnc_id"
        ]
        return df.groupby(cols, dropna=False).first().reset_index()

    def merge_nodenorm(self, merged_df, nodenorm_df):
        logging.info("STEP 7: merge_nodenorm")
        print("🔄 Merging NodeNorm…")
        merged_df['ncbi_NCBI_id'] = merged_df['ncbi_NCBI_id'].fillna('').astype(str).str.strip()
        nodenorm_df['nodenorm_NCBI_id'] = nodenorm_df['nodenorm_NCBI_id'].fillna('').astype(str).str.strip()
        merged = pd.merge(
            merged_df, nodenorm_df,
            left_on='ncbi_NCBI_id', right_on='nodenorm_NCBI_id',
            how='left'
        )
        for col in ['hgnc_omim_id','nodenorm_OMIM']:
            merged[col] = merged[col].apply(
                lambda x: f"MIM:{x}" if pd.notnull(x)
                           and str(x).strip()
                           and not str(x).startswith("MIM:") else x
            )
        return merged

    def _normalize_all_compare_cols(self, df):
        """Turn every comparison column’s NaNs into '' and strip whitespace."""
        all_cols = [
            # ID columns
            'ensembl_gene_id','ncbi_ensembl_gene_id','hgnc_ensembl_gene_id',
            'ensembl_NCBI_id','ncbi_NCBI_id','hgnc_NCBI_id','nodenorm_NCBI_id',
            'ensembl_hgnc_id','ncbi_hgnc_id','hgnc_hgnc_id','nodenorm_HGNC',
            # OMIM/UMLS
            'hgnc_omim_id','ncbi_mim_id','nodenorm_OMIM','nodenorm_UMLS',
            # symbols
            'ensembl_symbol','ncbi_symbol','hgnc_symbol','nodenorm_symbol',
            # gene type
            'ensembl_gene_type','ncbi_gene_type','hgnc_gene_type',
            # location
            'ensembl_location','ncbi_location','hgnc_location',
            # description
            'ensembl_description','ncbi_description','hgnc_description',
            # synonyms
            'ensembl_synonyms','ncbi_synonyms','hgnc_synonyms'
        ]
        for c in all_cols:
            if c in df:
                df[c] = df[c].fillna('').astype(str).str.strip()
        return df

    def compare_ncbi_mappings(self, df):
        logging.info("STEP 8: compare_ncbi_mappings")
        print("🔄 Computing ID provenance…")

        # 1) normalize all of the columns once
        df = self._normalize_all_compare_cols(df)

        # 2) now compute provenance exactly as before
        def get_prov(row, cols, labs):
            non_empty = [(lab, row[c]) for c,lab in zip(cols,labs) if row[c] != ""]
            if not non_empty:
                return None
            vals = [v for _,v in non_empty]
            return ", ".join(lab for lab,_ in non_empty) if len(set(vals)) == 1 else "error"

        df['Ensembl_ID_Provenance'] = df.apply(
            lambda r: get_prov(r,
                              ['ensembl_gene_id','ncbi_ensembl_gene_id','hgnc_ensembl_gene_id'],
                              ['ensembl','ncbi','hgnc']),
            axis=1
        )
        df['NCBI_ID_Provenance'] = df.apply(
            lambda r: get_prov(r,
                              ['ensembl_NCBI_id','ncbi_NCBI_id','hgnc_NCBI_id','nodenorm_NCBI_id'],
                              ['ensembl','ncbi','hgnc','nodenorm']),
            axis=1
        )
        df['HGNC_ID_Provenance'] = df.apply(
            lambda r: get_prov(r,
                              ['ensembl_hgnc_id','ncbi_hgnc_id','hgnc_hgnc_id','nodenorm_HGNC'],
                              ['ensembl','ncbi','hgnc','nodenorm']),
            axis=1
        )
        df['OMIM_ID_Provenance'] = df.apply(
            lambda r: get_prov(r,
                              ['hgnc_omim_id','ncbi_mim_id','nodenorm_OMIM'],
                              ['hgnc','ncbi','nodenorm']),
            axis=1
        )
        df['UMLS_ID_Provenance'] = df.apply(
            lambda r: get_prov(r,
                              ['nodenorm_UMLS'],
                              ['nodenorm']),
            axis=1
        )
        return df

    def compare_symbol_mappings(self, df):
        logging.info("STEP 9: compare_symbol_mappings")
        print("🔄 Computing symbol provenance…")
        df = self._normalize_all_compare_cols(df)
        def sym(r):
            non_empty = [(lab, r[col]) for col,lab in [
                ('ensembl_symbol','ensembl'),
                ('ncbi_symbol','ncbi'),
                ('hgnc_symbol','hgnc')
            ] if r[col]]
            if not non_empty: return None
            vals = [v for _,v in non_empty]
            return ", ".join(l for l,_ in non_empty) if len(set(vals))==1 else "error"
        df['Symbol_Provenance'] = df.apply(sym, axis=1)

    def compare_location_mappings(self, df):
        logging.info("STEP 10: compare_location_mappings")
        print("🔄 Computing location provenance…")
        df = self._normalize_all_compare_cols(df)
        def loc(r):
            e,nc,h = r['ensembl_location'],r['ncbi_location'],r['hgnc_location']
            if not any([e,nc,h]): return None
            if nc==h and nc: return 'ncbi, hgnc'
            if e and not nc and not h: return 'ensembl'
            if nc and not h: return 'ncbi'
            if h and not nc: return 'hgnc'
            return 'error'
        df['Location_Provenance'] = df.apply(loc, axis=1)

    def compare_description_mappings(self, df):
        logging.info("STEP 11: compare_description_mappings")
        print("🔄 Computing description provenance…")
        def desc(r):
            e,nc,h = r['ensembl_description'],r['ncbi_description'],r['hgnc_description']
            if not any([e,nc,h]): return None
            if e==nc==h and e: return 'ensembl, ncbi, hgnc'
            if e==nc and e and not h: return 'ensembl, ncbi'
            if e==h and e and not nc: return 'ensembl, hgnc'
            if nc==h and nc and not e: return 'ncbi, hgnc'
            if e and not nc and not h: return 'ensembl'
            if nc and not e and not h: return 'ncbi'
            if h and not e and not nc: return 'hgnc'
            return 'error'
        df['Description_Provenance'] = df.apply(desc, axis=1)

    def calculate_Mapping_scores(self, df):
        logging.info("STEP 12: calculate_Mapping_scores")
        print("🔄 Calculating mapping scores…")
        def cnt(r,cols): return sum(bool(r[c]) for c in cols)
        def score(s):
            return {'ensembl, ncbi, hgnc':3,'ensembl, ncbi':2,'ensembl, hgnc':2,'ncbi, hgnc':2}.get(s,0)
        df['Ensembl_Mapping_Score']   = df.apply(lambda r: cnt(r,[
            'ensembl_gene_id','ncbi_ensembl_gene_id','hgnc_ensembl_gene_id']), axis=1)
        df['NCBI_Mapping_Score']      = df.apply(lambda r: cnt(r,[
            'ensembl_NCBI_id','ncbi_NCBI_id','hgnc_NCBI_id']), axis=1)
        df['HGNC_Mapping_Score']      = df.apply(lambda r: cnt(r,[
            'ensembl_hgnc_id','ncbi_hgnc_id','hgnc_hgnc_id']), axis=1)
        df['OMIM_Mapping_Score']      = df.apply(lambda r: cnt(r,[
            'hgnc_omim_id','ncbi_mim_id']), axis=1)
        df['Symbol_Mapping_Score']    = df.apply(lambda r: cnt(r,[
            'ensembl_symbol','ncbi_symbol','hgnc_symbol']), axis=1)
        df['Description_Mapping_Score'] = df['Description_Provenance'].map(score).fillna(0)
        df['Total_Mapping_Score']     = (
            df['Ensembl_Mapping_Score']+
            df['NCBI_Mapping_Score']+
            df['HGNC_Mapping_Score']+
            df['OMIM_Mapping_Score']+
            df['Symbol_Mapping_Score']+
            df['Description_Mapping_Score']
        )
        df['Total_Mapping_RATIO']     = df['Total_Mapping_Score']/24
        return df

    def flag_for_review(self, merged_df, comparison_df):
        logging.info("STEP 13: flag_for_review")
        print("🔄 Flagging rows for review…")
        dup = merged_df[merged_df.duplicated()]
        err = comparison_df[
            (comparison_df['Ensembl_ID_Provenance'] == 'error') |
            (comparison_df['NCBI_ID_Provenance'] == 'error') |
            (comparison_df['HGNC_ID_Provenance'] == 'error') |
            (comparison_df['OMIM_ID_Provenance'] == 'error') |
            (comparison_df['Symbol_Provenance'] == 'error')  
        ]
        flagged = pd.concat([dup, err]).drop_duplicates()

        if self.qc_mode:
            path = "src/data/publicdata/target_data/qc/genes_flagged_for_review.qc.csv"
            os.makedirs(os.path.dirname(path), exist_ok=True)
            flagged.to_csv(path, index=False)
            print(f"  → Flagged rows saved to {path}")

        return pd.concat([merged_df, flagged]).drop_duplicates()

    def count_match_statuses_multi(self, df, domain_definitions, counts_csv=None):
        logging.info("STEP 14: count_match_statuses_multi")
        print("🔄 Counting match/mismatch/only stats…")
        def nonempty(r,c): return bool(str(r.get(c,'')).strip())
        rows = []
        for definition in domain_definitions:
            label, *cols = definition
            sources = ['ensembl','ncbi','hgnc','nodenorm'][:len(cols)]
            def status(r):
                vals = {src:str(r.get(c,'')).strip()
                        for src,c in zip(sources,cols) if c}
                vals = {s:v for s,v in vals.items() if v}
                if not vals: return 'none'
                return 'match' if len(set(vals.values()))==1 else 'mismatch'
            ser = df.apply(status, axis=1)
            match    = (ser=='match').sum()
            mismatch = (ser=='mismatch').sum()
            only = {src:0 for src in sources}
            total= {}
            for i,src in enumerate(sources):
                col = cols[i]
                total[src] = df[col].apply(lambda x:bool(str(x).strip())).sum() if col else 0
                only[src]  = df.apply(
                    lambda r: nonempty(r,cols[i]) and all(not nonempty(r,c)
                                                         for j,c in enumerate(cols) if j!=i),
                    axis=1
                ).sum()
            row = {"column":label,"match":match,"mismatch":mismatch}
            for src in sources:
                row[f"{src}_only"]  = only[src]
                row[f"{src}_total"] = total[src]
            rows.append(row)

        result_df = pd.DataFrame(rows)
        if counts_csv and self.qc_mode:
            os.makedirs(os.path.dirname(counts_csv), exist_ok=True)
            result_df.to_csv(counts_csv, index=False)
            print(f"  → Mapping stats written to {counts_csv}")
        return result_df

    def unify_mapping_ids(self, df):
        logging.info("STEP 15: unify_mapping_ids")
        print("🔄 Grouping by mapping IDs…")
        cols = [
            "ensembl_gene_id","ncbi_ensembl_gene_id","hgnc_ensembl_gene_id",
            "ensembl_NCBI_id","ncbi_NCBI_id","hgnc_NCBI_id",
            "ensembl_hgnc_id","ncbi_hgnc_id","hgnc_hgnc_id"
        ]
        return df.groupby(cols, dropna=False).first().reset_index()

    def save_metadata(self):
        logging.info("STEP 16: save_metadata")
        self.metadata["timestamp"]["end"] = str(datetime.now())
        mf = self.config['metadata_file']
        os.makedirs(os.path.dirname(mf), exist_ok=True)
        with open(mf,'w') as f:
            json.dump(self.metadata, f, indent=2)
        print(f"✨ Metadata written to {mf}")
    
def process_gene_merge(config):
    print("🚀 Starting full GENEDataMerger pipeline\n")
    gm_cfg = config["gene_merge"]  
    merger = GENEDataMerger(config)
    en, nc, hg, nn = merger.read_and_clean_files()
    m1 = merger.merge_ensembl_ncbi(en, nc)
    c1 = merger.clean_and_update_merged_df(m1)
    m2 = merger.merge_hgnc(c1, hg)
    u1 = merger.unify_mapping_ids(m2)
    nnm = merger.merge_nodenorm(u1, nn) if nn is not None else u1

    comp = merger.compare_ncbi_mappings(nnm)
    merger.compare_symbol_mappings(comp)
    merger.compare_location_mappings(comp)
    merger.compare_description_mappings(comp)

    scored = merger.calculate_Mapping_scores(comp)
    domain_defs = [
        ("Ensembl", 'ensembl_gene_id', 'ncbi_ensembl_gene_id', 'hgnc_ensembl_gene_id', 'nodenorm_ensembl_gene_id'),
        ("NCBI",    'ensembl_NCBI_id',    'ncbi_NCBI_id',    'hgnc_NCBI_id',       'nodenorm_NCBI_id'),
        ("HGNC",    'ensembl_hgnc_id',    'ncbi_hgnc_id',    'hgnc_hgnc_id',       'nodenorm_HGNC'),
        ("OMIM",    None,                 'ncbi_mim_id',     'hgnc_omim_id',       'nodenorm_OMIM'),
        ("UMLS",    None,              None,               None,                  'nodenorm_UMLS'),
        ("symbol",  'ensembl_symbol',     'ncbi_symbol',     'hgnc_symbol',        'nodenorm_symbol'),
        ("gene_type","ensembl_gene_type","ncbi_gene_type","hgnc_gene_type"),
        ("location","ensembl_location","ncbi_location","hgnc_location"),
        ("description","ensembl_description","ncbi_description","hgnc_description"),
        ("synonyms","ensembl_synonyms","ncbi_synonyms","hgnc_synonyms")
    ]
    gm_cfg = config["gene_merge"]
    mapping_stats_csv = gm_cfg.get('mapping_stats_file', gm_cfg['stats_file'])

    if merger.qc_mode:
        stats_df = merger.count_match_statuses_multi(scored, domain_defs, mapping_stats_csv)
        print("\n📊 Mapping stats:\n", stats_df.to_string(index=False))
    else:
        stats_df = merger.count_match_statuses_multi(scored, domain_defs)
    final = merger.flag_for_review(scored, scored)
    gm_cfg = config["gene_merge"]
    outp = gm_cfg['output_file']
    os.makedirs(os.path.dirname(outp), exist_ok=True)
    final.to_csv(outp, index=False)
    print(f"\n🎯 Final merged file written to {outp}")

if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Merge gene ID sources with provenance and metadata"
    )
    p.add_argument("--config", type=str,
               default="config/targets_config.yaml",
               help="Path to YAML config file (default: config/targets_config.yaml)")

    args = p.parse_args()

    full_cfg = yaml.safe_load(open(args.config))
    setup_logging(full_cfg.get('gene_merge', {}).get('log_file', ""))
    process_gene_merge(full_cfg)  # Pass the full config so `global.qc_mode` is visible

