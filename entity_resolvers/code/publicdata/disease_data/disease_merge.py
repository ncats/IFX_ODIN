# disease_merge.py - TargetGraph modular disease source merging pipeline

import os
import yaml
import logging
import argparse
import pandas as pd
import numpy as np
from datetime import datetime
import gc

# ---------------- Unwanted Columns ----------------
DROP_COLS = [
    "GARD_provenance_mondo", "ICDO_provenance_mondo", "EFO_provenance_mondo", "ICD9CM_doid_mondo",
    "ICD10CM_provenance_mondo", "MESH_provenance_mondo", "UMLS_provenance_mondo", "synonyms_provenance_mondo",
    "definition_provenance_mondo", "preferred_label_provenance_mondo", "MESH_doid_mondo", "ICD9CM_doid_mondo",
    "SNOMEDCT_US_2020_03_01_doid_mondo", "SNOMEDCT_US_2020_09_01_doid_mondo",
    "SNOMEDCT_US_2021_07_31_doid_mondo", "SNOMEDCT_US_2021_09_01_doid", "SNOMEDCT_US_2022_03_01_doid",
    "SNOMEDCT_US_2022_07_31_doid_mondo", "SNOMEDCT_US_2023_09_01_doid_mondo",
    "SNOMEDCT_US_2023_10_01_doid_mondo", "SNOMEDCT_US_2023_11_01_doid_mondo", "SNOMEDCT_US_2024_03_01_doid_mondo",
    "ICDO_doid_mondo", "preferred_label_doid_mondo", "definition_doid_mondo", "synonyms_doid_mondo", "ORPHANET_provenance_mondo",
    "database_cross_reference_doid_mondo", "EFO_doid_mondo", "GARD_doid_mondo", "ICD10CM_doid_mondo", "UMLS_CUI_mondo"
]


def setup_logging(log_file):
    root = logging.getLogger()
    if not root.handlers:
        root.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        root.addHandler(sh)
        if log_file:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            fh = logging.FileHandler(log_file)
            fh.setFormatter(fmt)
            root.addHandler(fh)

def clean_df(df):
    for col in df.select_dtypes(include='object'):
        df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)
    return df.replace(r'^\s*$', np.nan, regex=True)

class DiseaseDataMerger:
    def __init__(self, config):
        self.config = config["disease_merge"]
        self.qc_mode = config.get("global", {}).get("qc_mode", False)

    def _save_qc(self, df, label):
        if self.qc_mode:
            out_path = f"src/data/publicdata/disease_data/qc/merged_{label}.qc.csv"
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            df.to_csv(out_path, index=False)
            logging.info(f"✅ Saved {label} merged file to {out_path}")
            return out_path
        return None

    def _reload_qc(self, path):
        if self.qc_mode and path and os.path.exists(path):
            logging.info(f"🔄 Reloading {path} for next merge step")
            df = pd.read_csv(path, dtype=str)
            return clean_df(df)
        return None

    def load_and_clean(self, path):
        return clean_df(pd.read_csv(path, dtype=str))

    def safe_merge_concat(self, base_df, incoming_df, left_on, right_on, label):
        # Filter only rows with joinable keys
        mapped = pd.merge(
            base_df[base_df[left_on].notna()],
            incoming_df[incoming_df[right_on].notna()],
            how="outer", left_on=left_on, right_on=right_on
        )
        # Keep unmatched rows from the incoming set
        unmatched = incoming_df[incoming_df[right_on].isna()]
        merged_df = pd.concat([mapped, unmatched], ignore_index=True)

        logging.info(f"📊 Merged with {label}: {merged_df.shape}")
        path = self._save_qc(merged_df, label)
        del base_df, incoming_df, mapped, unmatched
        gc.collect()
        return self._reload_qc(path) or merged_df

    def merge_sources(self):
        cfg = self.config

        # MONDO + DOID
        logging.info("🧬 Loading MONDO and DOID...")
        df_mondo = self.load_and_clean(cfg['mondo_cleaned_file'])
        df_doid = self.load_and_clean(cfg['doid_cleaned_file'])
        df = pd.merge(df_mondo, df_doid, how="outer", left_on="mondo_doid", right_on="doid_DOID")
        logging.info(f"📊 Merged MONDO + DOID: {df.shape}")
        path = self._save_qc(df, "mondo_doid")
        df_tmp = self._reload_qc(path)
        if df_tmp is not None:
            df = df_tmp
        del df_mondo, df_doid, df_tmp
        gc.collect()

        # MedGen
        logging.info("🧬 Merging medgen...")
        df_medgen = self.load_and_clean(cfg['medgen_cleaned_file'])
        mapped = pd.merge(
            df[df["mondo_id"].notna()],
            df_medgen[df_medgen["medgen_MONDO"].notna()],
            how="outer",
            left_on="mondo_id",
            right_on="medgen_MONDO"
        )
        unmatched = df_medgen[df_medgen["medgen_MONDO"].isna()]
        df = pd.concat([mapped, unmatched], ignore_index=True)
        logging.info(f"📊 Merged with medgen: {df.shape}")
        path = self._save_qc(df, "medgen")
        df_tmp = self._reload_qc(path)
        if df_tmp is not None:
            df = df_tmp
        del df_medgen, df_tmp, mapped, unmatched
        gc.collect()

        # Orphanet
        logging.info("🧬 Merging orphanet...")
        df_orph = self.load_and_clean(cfg['orphanet_cleaned_file'])
        mapped = pd.merge(
            df,
            df_orph,
            how="outer",
            left_on="mondo_orphanet",
            right_on="orphanet_Orphanet_ID"
        )
        logging.info(f"📊 Merged with orphanet: {mapped.shape}")
        path = self._save_qc(mapped, "orphanet")
        df_tmp = self._reload_qc(path)
        df = df_tmp if df_tmp is not None else mapped
        del df_orph, df_tmp, mapped
        gc.collect()

        # OMIM
        logging.info("🧬 Merging omim...")
        df_omim = self.load_and_clean(cfg['omim_cleaned_file'])
        mapped = pd.merge(
            df,
            df_omim,
            how="outer",
            left_on="mondo_omim",
            right_on="omim_OMIM"
        )
        logging.info(f"📊 Merged with omim: {mapped.shape}")
        path = self._save_qc(mapped, "omim")
        df_tmp = self._reload_qc(path)
        df = df_tmp if df_tmp is not None else mapped
        del df_omim, df_tmp, mapped
        gc.collect()

        # NodeNorm Merging: MONDO → ORPHANET → UMLS
        logging.info("🧬 Merging NodeNorm with precedence: MONDO → ORPHANET → UMLS")
        df_nodenorm = self.load_and_clean(cfg['nodenorm_cleaned_file'])
        df_original = df.copy()

        # Step 1: MONDO match
        mondo_merge = pd.merge(
            df,
            df_nodenorm[df_nodenorm["nodenorm_MONDO"].notna()],
            how="left",
            left_on="mondo_id",
            right_on="nodenorm_MONDO",
            indicator=False
        )
        matched_mondo = mondo_merge[mondo_merge["nodenorm_MONDO"].notna()].copy()
        matched_mondo["nodenorm_match_source"] = "MONDO"
        remaining = mondo_merge[mondo_merge["nodenorm_MONDO"].isna()].drop(
            columns=[col for col in df_nodenorm.columns if col.startswith("nodenorm_") or col.endswith("_id")],
            errors="ignore"
        )

        # Step 2: ORPHANET match (only unmatched from MONDO)
        orph_merge = pd.merge(
            remaining,
            df_nodenorm[df_nodenorm["nodenorm_orphanet"].notna()],
            how="left",
            left_on="orphanet_Orphanet_ID",
            right_on="nodenorm_orphanet",
            indicator=False
        )
        matched_orph = orph_merge[orph_merge["nodenorm_orphanet"].notna()].copy()
        matched_orph["nodenorm_match_source"] = "ORPHANET"
        remaining = orph_merge[orph_merge["nodenorm_orphanet"].isna()].drop(
            columns=[col for col in df_nodenorm.columns if col.startswith("nodenorm_") or col.endswith("_id")],
            errors="ignore"
        )

        # Step 3: UMLS match (only unmatched from above)
        umls_merge = pd.merge(
            remaining,
            df_nodenorm[df_nodenorm["nodenorm_UMLS"].notna()],
            how="left",
            left_on="medgen_UMLS",
            right_on="nodenorm_UMLS",
            indicator=False
        )
        matched_umls = umls_merge[umls_merge["nodenorm_UMLS"].notna()].copy()
        matched_umls["nodenorm_match_source"] = "UMLS"
        remaining = umls_merge[umls_merge["nodenorm_UMLS"].isna()].drop(
            columns=[col for col in df_nodenorm.columns if col.startswith("nodenorm_") or col.endswith("_id")],
            errors="ignore"
        )

        # Collect used identifiers
        used_mondo = matched_mondo["nodenorm_MONDO"].dropna().unique()
        used_orph = matched_orph["nodenorm_orphanet"].dropna().unique()
        used_umls = matched_umls["nodenorm_UMLS"].dropna().unique()

        # Unmatched NodeNorm-only entries
        unmatched_nodenorm = df_nodenorm[
            ~df_nodenorm["nodenorm_MONDO"].isin(used_mondo) &
            ~df_nodenorm["nodenorm_orphanet"].isin(used_orph) &
            ~df_nodenorm["nodenorm_UMLS"].isin(used_umls)
        ].copy()
        unmatched_nodenorm["nodenorm_match_source"] = "none_nodenorm"

        # Unmatched df entries
        df_merge_id = "mondo_id"
        merged_ids = pd.concat([
            matched_mondo[df_merge_id],
            matched_orph[df_merge_id],
            matched_umls[df_merge_id]
        ], ignore_index=True).dropna().unique()

        unmatched_df = df_original[~df_original[df_merge_id].isin(merged_ids)].copy()
        unmatched_df["nodenorm_match_source"] = "none_df"

        # Final union
        df = pd.concat([
            matched_mondo,
            matched_orph,
            matched_umls,
            unmatched_nodenorm,
            unmatched_df
        ], ignore_index=True)

        df = df.loc[:, ~df.columns.str.endswith(("_x", "_y"))]
        logging.info(f"📊 Final merged NodeNorm file shape: {df.shape}")

        # Optional unmatched QC
        if self.qc_mode:
            still_unmatched = df[df["nodenorm_match_source"].str.startswith("none")]
            unmatched_path = "src/data/publicdata/disease_data/qc/unmatched_nodenorm.qc.csv"
            still_unmatched.to_csv(unmatched_path, index=False)
            logging.info(f"🛑 Saved unmatched NodeNorm rows to {unmatched_path}")

        path = self._save_qc(df, "nodenorm")
        df_tmp = self._reload_qc(path)
        df = df_tmp if df_tmp is not None else df

        # Cleanup
        del df_nodenorm, df_tmp, df_original, mondo_merge, orph_merge, umls_merge
        del matched_mondo, matched_orph, matched_umls, unmatched_nodenorm, unmatched_df, still_unmatched
        gc.collect()
        return df

    def consolidate_duplicates_on_consolidated(self, df):
        logging.info("🔄 Consolidating only truly duplicated MONDO IDs (post-score)")

        key = "mondo_id"
        # 1) find which consolidated IDs appear multiple times
        counts = df[key].value_counts()
        dupes = counts[counts > 1].index.tolist()
        if not dupes:
            logging.info("✅ No duplicates in '%s' — skipping consolidation", key)
            return df

        # 2) helper to flatten "|" and de-dupe per column
        def concat_unique(series):
            bits = set()
            for val in series.dropna().astype(str):
                bits.update(x.strip() for x in val.split("|") if x.strip())
            return "|".join(sorted(bits)) if bits else pd.NA

        # 3) build an agg dict for every column except the grouping key
        agg_dict = {col: concat_unique for col in df.columns if col != key}

        # 4) aggregate only the duplicated groups
        grouped = (
            df[df[key].isin(dupes)]
            .groupby(key, dropna=False)
            .agg(agg_dict)
            .reset_index()
        )

        # 5) pull through the singleton rows untouched
        singles = df[~df[key].isin(dupes)]

        # 6) stitch back together
        final = pd.concat([singles, grouped], ignore_index=True)

        logging.info("✅ Consolidated %d duplicate IDs into %d rows; total now %d rows",
                    len(dupes), grouped.shape[0], final.shape[0])
        return final

    def consolidate_and_score(self, df):
        logging.info("🧮 Consolidating columns and calculating scores")

        MANUAL_GROUPS = [
            ("mondo", "mondo_id", "medgen_MONDO", "nodenorm_MONDO"),
            ("DOID", "doid_DOID", "mondo_doid", "nodenorm_DOID"),
            ("EFO", "mondo_efo", "doid_EFO", "nodenorm_EFO"),
            ("GARD", "mondo_gard", "doid_GARD", "medgen_GARD"),
            ("HP", "mondo_hp", "medgen_HPO", "nodenorm_HP"),
            ("ICD10", "mondo_icd10cm", "doid_ICD10CM", "orphanet_ICD-10", "nodenorm_ICD10"),
            ("ICD9", "mondo_icd9", "doid_ICD9CM", "nodenorm_ICD9"),
            ("ICD11", "doid_ICD11", "orphanet_ICD-11", "nodenorm_icd11"),
            ("ICD11f", "mondo_icd11.foundation", "nodenorm_icd11.foundation"),
            ("ICDO", "mondo_icdo", "doid_ICDO"),
            ("KEGG", "doid_KEGG", "nodenorm_KEGG"),
            ("MESH", "mondo_mesh", "doid_MESH", "medgen_MeSH", "orphanet_MeSH", "nodenorm_MESH"),
            ("MEDDRA", "mondo_meddra", "doid_MEDDRA", "orphanet_MedDRA", "nodenorm_MEDDRA"),
            ("NCIT", "mondo_ncit", "doid_NCI", "nodenorm_NCIT"),
            ("OMIM", "omim_OMIM", "mondo_omim", "doid_MIM", "medgen_OMIM", "orphanet_OMIM", "nodenorm_OMIM"),
            ("OMIMPS", "mondo_omimps", "medgen_OMIM Phenotypic Series", "nodenorm_OMIMPS"),
            ("SNOMEDCT", "doid_SNOMEDCT", "medgen_SNOMEDCT_US", "nodenorm_SNOMEDCT"),
            ("UMLS", "mondo_umls", "doid_UMLS_CUI", "medgen_UMLS", "orphanet_UMLS", "nodenorm_UMLS"),
            ("orphanet", "orphanet_Orphanet_ID", "mondo_orphanet", "medgen_Orphanet", "nodenorm_orphanet"),
            ("medgen", "medgen_MedGen", "mondo_medgen", "nodenorm_medgen")
        ]

        def strip_base(col, base):
            return col.lower().replace(base.lower() + "_", "")

        def split_and_clean(val):
            return set(v.strip() for v in str(val).split("|") if v.strip())

        new_cols = {}
        score_cols = []

        for base, *cols in MANUAL_GROUPS:
            consolidated, provenance, score = [], [], []

            for _, row in df.iterrows():
                values = {}
                for col in cols:
                    raw = row.get(col)
                    if pd.notna(raw):
                        tag = strip_base(col, base)
                        values[tag] = split_and_clean(raw)

                if not values:
                    consolidated.append(pd.NA)
                    provenance.append(pd.NA)
                    score.append(0)
                    continue

                all_sets = list(values.values())
                all_union = set.union(*all_sets)
                all_tags = list(values.keys())

                unique_sets = [s for s in all_sets if s]
                overlaps = all(s == unique_sets[0] for s in unique_sets)
                partial = any(s1 & s2 for i, s1 in enumerate(unique_sets) for j, s2 in enumerate(unique_sets) if i < j) and not overlaps

                if overlaps:
                    prov = "&".join(all_tags) if len(all_tags) > 1 else all_tags[0] + "_only"
                    consolidated.append("|".join(sorted(all_union)))
                    provenance.append(prov)
                    score.append(len(all_tags))
                elif partial:
                    consolidated.append("|".join(sorted(all_union)))
                    provenance.append("partial_" + "&".join(all_tags))
                    score.append(0.5)
                else:
                    consolidated.append("|".join(sorted(all_union)))
                    provenance.append("mismatch")
                    score.append(0)

            new_cols[f"{base}_consolidated"] = consolidated
            new_cols[f"{base}_provenance"] = provenance
            new_cols[f"{base}_score"] = score
            score_cols.append(f"{base}_score")

        df = pd.concat([df, pd.DataFrame(new_cols)], axis=1)
        df["mapping_score_total"] = df[score_cols].sum(axis=1)
        df["mapping_score_ratio"] = df["mapping_score_total"] / (len(score_cols) * 4)
        # Rearrange columns to group identifiers and descriptions together
        id_groups = []
        for base, *_ in MANUAL_GROUPS:
            id_groups.extend([
                f"{base}_consolidated",
                f"{base}_provenance",
                f"{base}_score"
            ])

        static_score_cols = ["mapping_score_total", "mapping_score_ratio"]
        description_cols = [
            c for c in df.columns if any(x in c.lower() for x in ["preferred", "definition", "synonym"]) and c not in id_groups
        ]
        other_cols = [c for c in df.columns if c not in id_groups + static_score_cols + description_cols]

        new_order = id_groups + static_score_cols + description_cols + other_cols
        df = df[[c for c in new_order if c in df.columns]]
        if self.qc_mode:
            mismatch_df = df.filter(like="_provenance").eq("mismatch")
            mismatch_rows = df[mismatch_df.any(axis=1)]
            mismatch_path = "src/data/publicdata/disease_data/qc/disease_mismatches.qc.csv"
            os.makedirs(os.path.dirname(mismatch_path), exist_ok=True)
            mismatch_rows.to_csv(mismatch_path, index=False)
            logging.info(f"⚠️  Saved mismatch rows to {mismatch_path}")

        return df
    
    def reorder_columns_strictly(self, df):
        """Reorder DataFrame columns based on strict manual order defined by user."""
        final_column_order = [
            # === Core IDs and mappings ===
            "mondo_id", "medgen_MONDO", "nodenorm_MONDO", "mondo_consolidated", "mondo_provenance", "mondo_score",
            "mondo_doid", "doid_DOID", "nodenorm_DOID", "DOID_consolidated", "DOID_provenance", "DOID_score",
            "mondo_efo", "doid_EFO", "nodenorm_EFO", "EFO_consolidated", "EFO_provenance", "EFO_score",
            "mondo_gard", "doid_GARD", "medgen_GARD", "GARD_consolidated", "GARD_provenance", "GARD_score",
            "mondo_hp", "medgen_HPO", "nodenorm_HP", "HP_consolidated", "HP_provenance", "HP_score",
            "mondo_icd10cm", "doid_ICD10CM", "orphanet_ICD-10", "nodenorm_ICD10", "ICD10_consolidated", "ICD10_provenance", "ICD10_score",
            "mondo_icd9", "doid_ICD9CM", "nodenorm_ICD9", "ICD9_consolidated", "ICD9_provenance", "ICD9_score",
            "doid_ICD11", "orphanet_ICD-11", "nodenorm_icd11", "ICD11_consolidated", "ICD11_provenance", "ICD11_score",
            "mondo_icd11.foundation", "nodenorm_icd11.foundation", "ICD11f_consolidated", "ICD11f_provenance", "ICD11f_score",
            "mondo_icdo", "doid_ICDO", "ICDO_consolidated", "ICDO_provenance", "ICDO_score",
            "doid_KEGG", "nodenorm_KEGG", "KEGG_consolidated", "KEGG_provenance", "KEGG_score",
            "mondo_mesh", "doid_MESH", "medgen_MeSH", "orphanet_MeSH", "nodenorm_MESH", "MESH_consolidated", "MESH_provenance", "MESH_score",
            "mondo_meddra", "doid_MEDDRA", "orphanet_MedDRA", "nodenorm_MEDDRA", "MEDDRA_consolidated", "MEDDRA_provenance", "MEDDRA_score",
            "mondo_ncit", "doid_NCI", "nodenorm_NCIT", "NCIT_consolidated", "NCIT_provenance", "NCIT_score",
            "mondo_omim", "omim_OMIM", "doid_MIM", "medgen_OMIM", "orphanet_OMIM", "nodenorm_OMIM", "OMIM_consolidated", "OMIM_provenance", "OMIM_score",
            "mondo_omimps", "medgen_OMIM Phenotypic Series", "nodenorm_OMIMPS", "OMIMPS_consolidated", "OMIMPS_provenance", "OMIMPS_score",
            "doid_SNOMEDCT", "medgen_SNOMEDCT_US", "nodenorm_SNOMEDCT", "SNOMEDCT_consolidated", "SNOMEDCT_provenance", "SNOMEDCT_score",
            "mondo_umls", "doid_UMLS_CUI", "medgen_UMLS", "orphanet_UMLS", "nodenorm_UMLS", "UMLS_consolidated", "UMLS_provenance", "UMLS_score",
            "mondo_orphanet", "orphanet_Orphanet_ID", "medgen_Orphanet", "nodenorm_orphanet", "orphanet_consolidated", "orphanet_provenance", "orphanet_score",
            "mondo_medgen", "medgen_MedGen", "nodenorm_medgen", "medgen_consolidated", "medgen_provenance", "medgen_score",

            # === Mapping scores ===
            "mapping_score_total", "mapping_score_ratio",

            # === Labels, definitions, synonyms ===
            "mondo_preferred_label", "mondo_definition", "mondo_synonyms",
            "doid_preferred_label", "doid_definition", "doid_synonyms",
            "medgen_Preferred_Name", "orphanet_Definition", "omim_preferred_label", "orphanet_Disease_Name",
            "omim_prefix", "omim_alternative_labels", "omim_included_labels",
            "nodenorm_Nodenorm_name",

            # === MONDO/DOID extra metadata ===
            "mondo_parents", "mondo_birnlex", "mondo_csp", "mondo_decipher", "mondo_gtr", "mondo_hgnc",
            "mondo_icd10exp", "mondo_icd10who", "mondo_icd9cm", "mondo_ido", "mondo_mfomd", "mondo_mpath",
            "mondo_mth", "mondo_nando", "mondo_ndfrt", "mondo_nlxdys", "mondo_nord", "mondo_obi",
            "mondo_ogms", "mondo_omia", "mondo_oncotree", "mondo_pmid", "mondo_scdo", "mondo_sctid",
            "mondo_wikipedia", "doid_ORDO", "medgen_OMIM allelic variant", "medgen_OMIM included",
            "nodenorm_MP", "NodeNorm_id", "biolinkType", "nodenorm_match_source"
        ]

        # Preserve any extra columns at the end
        cols_in_df = [col for col in final_column_order if col in df.columns]
        extras = [col for col in df.columns if col not in cols_in_df]
        final_cols = cols_in_df + extras

        missing = [col for col in final_column_order if col not in df.columns]
        if missing:
            logging.warning(f"⚠️ Missing {len(missing)} expected columns in final output: {missing[:5]}{'...' if len(missing) > 5 else ''}")

        return df[final_cols]

    def process(self):
        df = self.merge_sources()
        #df = self.consolidate_duplicates_on_consolidated(df)
        df = self.consolidate_and_score(df)
        df = self.reorder_columns_strictly(df)

        output = self.config['merged_output']
        os.makedirs(os.path.dirname(output), exist_ok=True)
        df.to_csv(output, index=False)
        logging.info(f"✅ Final merged file saved to {output}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    setup_logging(config["disease_merge"].get("log_file"))
    DiseaseDataMerger(config).process()