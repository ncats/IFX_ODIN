#!/usr/bin/env python
"""
uniprot_transform.py

Transform UniProt data:
- Reads the decompressed JSON (from uniprot_download.py)
- Flattens entries into two CSVs (mapping and reviewed info)
- Applies original cleaning: column renames and provenance prefixes
- Incorporates precomputed isoform→Ensembl mappings
- Computes MD5 hashes of inputs/outputs
- Records detailed metadata (paths, hashes, counts, durations)
- Writes entity-level QC diffs instead of noisy column diffs
- Archives transformed outputs for version tracking
"""

import os
import json
import hashlib
import logging
import argparse
import pandas as pd
import yaml
from datetime import datetime
from collections import defaultdict
from pathlib import Path


def setup_logging(log_file):
    handlers = [logging.StreamHandler()]
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        handlers.insert(0, logging.FileHandler(log_file, mode='a'))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
        force=True
    )


class UniProtTransformer:
    def __init__(self, full_config):
        cfg = full_config.get("uniprot_data", {})
        self.cfg = cfg

        # Paths
        self.input_json = cfg["decompressed_path"]
        self.mapping_output = cfg["mapping_output"]
        self.reviewed_output = cfg["reviewed_info_output"]
        self.idmap_output = cfg.get("idmap_output")
        self.canon_path = cfg["canonical_isoforms_output"]

        setup_logging(cfg.get("log_file"))

        self.metadata_file = os.path.abspath(
            cfg.get(
                "tf_metadata_file",
                "src/data/publicdata/target_data/metadata/tf_uniprot_metadata.json"
            )
        )

        self.entity_diff_file = os.path.abspath(
            cfg.get(
                "entity_diff_output",
                "src/data/publicdata/target_data/qc/uniprot_entity_diff.qc.json"
            )
        )

        self.transform_archive_dir = os.path.abspath(
            cfg.get(
                "transform_archive_dir",
                "src/data/publicdata/target_data/archive/cleaned/uniprot"
            )
        )

        self.metadata = {
            "timestamp": {"start": datetime.now().isoformat()},
            "input_json": os.path.abspath(self.input_json),
            "hash_input": None,
            "processing_steps": [],
            "outputs": [],
            "summary": {}
        }

    def compute_hash(self, path):
        h = hashlib.md5()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                h.update(chunk)
        return h.hexdigest()

    def load_json(self):
        logging.info(f"Reading JSON → {self.input_json}")
        t0 = datetime.now()
        with open(self.input_json, 'r', encoding='utf-8') as f:
            data = json.load(f)
        t1 = datetime.now()

        results = data.get('results', [])
        self.metadata['processing_steps'].append({
            'step': 'read_json',
            'records': len(results),
            'duration_seconds': (t1 - t0).total_seconds()
        })
        self.metadata['hash_input'] = self.compute_hash(self.input_json)
        return results

    def write_mapping(self, results):
        logging.info("Generating uniprot xref mapping CSV")
        t0 = datetime.now()
        rows = []

        for e in results:
            row = {
                'entryType': e.get('entryType'),
                'primaryAccession': e.get('primaryAccession'),
                'secondaryAccessions': '|'.join(e.get('secondaryAccessions', []))
            }

            names = [g['geneName']['value'] for g in e.get('genes', []) if g.get('geneName')]
            row['gene_name'] = ', '.join(names)

            for x in e.get('uniProtKBCrossReferences', []):
                db = x.get('database')
                if db:
                    row[f"xref_{db}"] = x.get('id')

            rows.append(row)

        df = pd.DataFrame(rows)

        cols = {
            'primaryAccession': 'uniprot_id',
            'gene_name': 'uniprot_symbol',
            'xref_GeneID': 'uniprot_NCBI_id',
            'xref_CCDS': 'uniprot_ccds_id',
            'xref_MIM': 'uniprot_omim_id',
            'xref_HGNC': 'uniprot_hgnc_id'
        }
        df.rename(columns=cols, inplace=True)
        df.columns = [f"uniprot_{c}" if not c.startswith('uniprot_') else c for c in df.columns]

        os.makedirs(os.path.dirname(self.mapping_output), exist_ok=True)
        df.to_csv(self.mapping_output, index=False)

        t1 = datetime.now()
        self.metadata['processing_steps'].append({
            'step': 'write_mapping',
            'records': len(df),
            'duration_seconds': (t1 - t0).total_seconds()
        })
        self.metadata['outputs'].append({
            'name': 'mapping_csv',
            'path': os.path.abspath(self.mapping_output),
            'records': len(df)
        })
        return df

    def write_reviewed(self, results):
        logging.info("Generating uniprot reviewed-info CSV")
        t0 = datetime.now()
        rows = []

        for e in results:
            row = self.extract_and_flatten_entry_for_reviewed_info_updated(e)
            rows.append(row)

        df = pd.DataFrame(rows)

        cols = {
            'primaryAccession': 'uniprot_id',
            'gene_name': 'uniprot_gene_name',
            'alternative_names': 'proteinDesc_alternative_names'
        }
        df.rename(columns=cols, inplace=True)
        df.columns = [f"uniprot_{c}" if not c.startswith('uniprot_') else c for c in df.columns]
        df.columns = [c.replace('uniprot_comment_', 'uniprot_') for c in df.columns]

        os.makedirs(os.path.dirname(self.reviewed_output), exist_ok=True)
        df.to_csv(self.reviewed_output, index=False)

        t1 = datetime.now()
        self.metadata['processing_steps'].append({
            'step': 'write_reviewed',
            'records': len(df),
            'duration_seconds': (t1 - t0).total_seconds()
        })
        self.metadata['outputs'].append({
            'name': 'reviewed_csv_initial',
            'path': os.path.abspath(self.reviewed_output),
            'records': len(df)
        })
        return df

    def merge_idmapping_isoforms(self, df_rev):
        idmap_path = self.cfg["idmap_output"]
        if not os.path.exists(idmap_path):
            logging.warning("…idmapping CSV not found [%s], skipping", idmap_path)
            return df_rev

        logging.info("Merging Ensembl_PRO/TRS from idmap (isoform-driven merge)")
        t0 = datetime.now()

        idmap = (
            pd.read_csv(idmap_path, dtype=str)[
                ["uniprot_id", "Ensembl_PRO", "Ensembl_TRS"]
            ].rename(columns={
                "Ensembl_PRO": "uniprot_ensembl_pro",
                "Ensembl_TRS": "uniprot_ensembl_trs"
            })
        )

        before = len(idmap)
        merged = idmap.merge(
            df_rev,
            on="uniprot_id",
            how="left",
            suffixes=("", "_json")
        )
        flagged = merged["uniprot_ensembl_trs"].notna().sum()
        dur = (datetime.now() - t0).total_seconds()

        logging.info(
            "Isoform-driven merge: %d/%d idmap rows got Ensembl_TRS in %.2fs",
            int(flagged), before, dur
        )

        self.metadata["processing_steps"].append({
            "step": "merge_idmapping_isoforms",
            "source": idmap_path,
            "rows_in_idmap": before,
            "rows_flagged": int(flagged),
            "duration_seconds": dur
        })

        return merged

    def extract_and_flatten_entry_for_reviewed_info_updated(self, entry):
        row_data = {
            'entryType': entry.get('entryType'),
            'gene_name': self.extract_gene_name_updated(entry.get('genes')),
            'primaryAccession': entry.get('primaryAccession'),
            'secondaryAccessions': '|'.join(entry.get('secondaryAccessions', [])),
            'uniProtkbId': entry.get('uniProtkbId'),
            'sequence': entry.get('sequence', {}).get('value'),
            'annotationScore': entry.get('annotationScore'),
            'recommended_name': self.extract_protein_name(entry.get('proteinDescription', {})),
            'proteinDesc_alternative_names': self.extract_proteinDesc_alternative_names(entry.get('proteinDescription', {})),
            'references': self.extract_references_from_list(entry.get('references'))
        }

        row_data.update(self.extract_features_data(entry.get('features', [])))
        row_data.update(self.extract_comment_content_updated(entry.get('comments')))

        for kw in entry.get('keywords', []):
            cat = kw.get('category')
            if cat:
                row_data[f"keyword_{cat}"] = kw.get('name')

        return row_data

    @staticmethod
    def extract_gene_name_updated(genes_data):
        if genes_data and isinstance(genes_data, list):
            return ', '.join([
                g.get('geneName', {}).get('value', '')
                for g in genes_data if g.get('geneName')
            ])
        return ''

    @staticmethod
    def extract_protein_name(protein_description):
        return protein_description.get('recommendedName', {}).get('fullName', {}).get('value')

    @staticmethod
    def extract_proteinDesc_alternative_names(protein_description):
        alts = protein_description.get('alternativeName', [])
        if alts:
            return '; '.join([a.get('fullName', {}).get('value', '') for a in alts])
        return None

    @staticmethod
    def extract_features_data(features_data_list):
        d = {}
        for f in features_data_list or []:
            t = f.get('type')
            if t:
                d[f"feature_{t.replace(' ', '_')}"] = f.get('description', '')
        return d

    @staticmethod
    def extract_references_from_list(references_list):
        if isinstance(references_list, list):
            return '|'.join([
                f"{r.get('citation', {}).get('citationType', '')}:{r.get('citation', {}).get('id', '')}"
                for r in references_list
            ])
        return ''

    @staticmethod
    def extract_comment_content_updated(comments_data_list):
        cdict = defaultdict(str)
        for c in comments_data_list or []:
            ctype = c.get('commentType')
            if not ctype:
                continue

            vals = []
            if 'texts' in c:
                vals.extend([t.get('value', '') for t in c['texts']])
            if 'reaction' in c:
                vals.append(c['reaction'].get('name', ''))
            if 'subcellularLocations' in c:
                vals.extend([l['location'].get('value', '') for l in c['subcellularLocations']])

            if ctype == 'ALTERNATIVE PRODUCTS':
                for iso in c.get('isoforms', []):
                    name = iso.get('name', {}).get('value', '')
                    evs = iso.get('synonyms', [{}])[0].get('evidences', [])
                    codes = '|'.join([e.get('evidenceCode', '') for e in evs])
                    pubs = '|'.join([e.get('id', '') for e in evs if e.get('source') == 'PubMed'])
                    iids = '|'.join(iso.get('isoformIds', []))
                    sids = '|'.join(iso.get('sequenceIds', []))
                    vals.append(f"{name},{codes},{pubs},{iids},{sids}")

            cdict[ctype] = '|'.join(vals)
        return cdict

    def merge_canonical_flags(self, df):
        if not os.path.exists(self.canon_path):
            logging.warning("No canonical_isoforms file at %s, skipping", self.canon_path)
            return df

        logging.info("Merging isCanonical flags from %s", self.canon_path)
        t0 = datetime.now()

        canon = (
            pd.read_csv(self.canon_path, dtype=str)
              .loc[:, ["isoform", "isCanonical"]]
              .rename(columns={
                  "isoform": "uniprot_id",
                  "isCanonical": "uniprot_is_canonical"
              })
        )

        before = len(df)
        df = df.merge(canon, on='uniprot_id', how='left')
        flagged = df['uniprot_is_canonical'].notna().sum()
        elapsed = (datetime.now() - t0).total_seconds()

        logging.info("Added isCanonical flag: %d/%d rows in %.2fs", int(flagged), before, elapsed)
        self.metadata['processing_steps'].append({
            'step': 'merge_canonical_flags',
            'source': self.canon_path,
            'rows_before': before,
            'rows_flagged': int(flagged),
            'duration_seconds': elapsed
        })
        return df

    def fill_isoform_sequences(self, df):
        if not os.path.exists(self.canon_path):
            logging.warning("No canonical_isoforms file found at %s, skipping isoform sequence fill", self.canon_path)
            return df

        logging.info("Filling missing uniprot_sequence from canonical_isoforms SPARQL file")

        isoform_seqs = (
            pd.read_csv(self.canon_path, dtype=str)
            .loc[:, ["uniprot_id", "uniprot_sequence"]]
            .dropna(subset=["uniprot_sequence"])
            .drop_duplicates()
        )

        before = df["uniprot_sequence"].isna().sum()
        df = df.merge(isoform_seqs, on="uniprot_id", how="left", suffixes=("", "_sparql"))
        df["uniprot_sequence"] = df["uniprot_sequence"].combine_first(df["uniprot_sequence_sparql"])
        df.drop(columns=["uniprot_sequence_sparql"], inplace=True)
        after = df["uniprot_sequence"].isna().sum()
        logging.info(f"Filled {before - after} missing isoform sequences")

        logging.info("Propagating annotations for isoforms with identical canonical sequences")
        df["__base_id"] = df["uniprot_id"].str.replace(r"-\d+$", "", regex=True)

        exclude = {"uniprot_id", "uniprot_sequence", "__base_id", "uniprot_is_canonical", "uniprot_entryType"}
        fill_cols = [col for col in df.columns if col not in exclude]

        fill_count = 0
        grouped = df.groupby("__base_id")
        for base_id, group in grouped:
            parent = group[group["uniprot_id"] == base_id]
            if parent.empty:
                continue

            parent_row = parent.iloc[0]
            for idx, row in group.iterrows():
                if row["uniprot_id"] == base_id:
                    continue

                if row["uniprot_sequence"] == parent_row["uniprot_sequence"]:
                    for col in fill_cols:
                        if pd.isna(row[col]) or row[col] == "":
                            df.at[idx, col] = parent_row[col]
                    fill_count += 1

        logging.info(f"Propagated full annotation for {fill_count} isoforms with matching canonical sequence")
        self.metadata["processing_steps"].append({
            "step": "fill_isoform_sequences + propagate_identical_isoforms",
            "filled_sequences": int(before - after),
            "propagated_annotations": int(fill_count),
            "remaining_nulls": int(after)
        })

        df.drop(columns=["__base_id"], inplace=True)
        return df

    def compute_entity_diff(self, old_df, new_df):
        old_df = old_df.fillna("").copy()
        new_df = new_df.fillna("").copy()

        if "uniprot_id" not in old_df.columns or "uniprot_id" not in new_df.columns:
            return None

        old_ids = set(old_df["uniprot_id"])
        new_ids = set(new_df["uniprot_id"])

        old_ids.discard("")
        new_ids.discard("")

        added = sorted(list(new_ids - old_ids))
        removed = sorted(list(old_ids - new_ids))

        old_idx = old_df.set_index("uniprot_id")
        new_idx = new_df.set_index("uniprot_id")

        common_ids = sorted(list(old_ids & new_ids))
        field_changes = []

        compare_cols = [
            "uniprot_gene_name",
            "uniprot_sequence",
            "uniprot_is_canonical",
            "uniprot_ensembl_pro",
            "uniprot_ensembl_trs",
            "uniprot_annotationScore",
            "uniprot_recommended_name"
        ]

        for uid in common_ids:
            for col in compare_cols:
                if col not in old_idx.columns or col not in new_idx.columns:
                    continue

                old_val = str(old_idx.at[uid, col])
                new_val = str(new_idx.at[uid, col])

                if old_val != new_val:
                    field_changes.append({
                        "uniprot_id": uid,
                        "field": col,
                        "old": old_val,
                        "new": new_val
                    })

        return {
            "added_ids": added,
            "removed_ids": removed,
            "field_changes": field_changes,
            "n_added_ids": len(added),
            "n_removed_ids": len(removed),
            "n_field_changes": len(field_changes)
        }

    def archive_output(self, df):
        version = datetime.now().strftime("%Y%m%d")
        archive_dir = Path(self.transform_archive_dir) / version
        archive_dir.mkdir(parents=True, exist_ok=True)

        archive_file = archive_dir / Path(self.reviewed_output).name
        df.to_csv(archive_file, index=False)
        return str(archive_file)

    @staticmethod
    def make_json_serializable(obj):
        import numpy as np

        if isinstance(obj, dict):
            return {k: UniProtTransformer.make_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [UniProtTransformer.make_json_serializable(v) for v in obj]
        elif isinstance(obj, (np.integer, pd.Int64Dtype)):
            return int(obj)
        elif isinstance(obj, (np.floating, pd.Float64Dtype)):
            return float(obj)
        elif isinstance(obj, (np.bool_,)):
            return bool(obj)
        return obj

    def process(self):
        results = self.load_json()

        # 1) write flat mapping
        self.write_mapping(results)

        # 2) write base reviewed-info
        df_rev = self.write_reviewed(results)

        # 3) merge idmapping isoforms
        df = self.merge_idmapping_isoforms(df_rev)

        # 4) merge canonical flags
        df = self.merge_canonical_flags(df)

        # 5) fill isoform sequences and propagate annotations
        df = self.fill_isoform_sequences(df)

        # 6) write final reviewed file
        os.makedirs(os.path.dirname(self.reviewed_output), exist_ok=True)
        df.to_csv(self.reviewed_output, index=False)

        # 7) entity-level diff instead of column diff
        qc_dir = "src/data/publicdata/target_data/qc"
        os.makedirs(qc_dir, exist_ok=True)
        backup_path = os.path.join(qc_dir, "uniprot_reviewed_info.backup.csv")

        diff_summary = None
        if os.path.exists(backup_path):
            try:
                old_df = pd.read_csv(backup_path, dtype=str)
                diff_summary = self.compute_entity_diff(old_df, df)

                if diff_summary is not None:
                    with open(self.entity_diff_file, "w") as f:
                        json.dump(diff_summary, f, indent=2)
                    logging.info(f"✅ Entity diff written to {self.entity_diff_file}")
            except Exception as e:
                logging.warning(f"⚠️ Error generating entity diff: {e}")

        df.to_csv(backup_path, index=False)

        # 8) archive transformed output
        archive_path = self.archive_output(df)
        logging.info(f"Archived transform → {archive_path}")

        # 9) metadata summary
        self.metadata["outputs"].append({
            "name": "reviewed_csv",
            "path": os.path.abspath(self.reviewed_output),
            "records": len(df)
        })
        self.metadata["outputs"].append({
            "name": "archived_reviewed_csv",
            "path": archive_path,
            "records": len(df)
        })

        self.metadata["timestamp"]["end"] = datetime.now().isoformat()
        self.metadata["summary"] = {
            "records_output": len(df),
            "archived_output": archive_path,
            "n_added_ids": diff_summary["n_added_ids"] if diff_summary else 0,
            "n_removed_ids": diff_summary["n_removed_ids"] if diff_summary else 0,
            "n_field_changes": diff_summary["n_field_changes"] if diff_summary else 0,
            "entity_diff_file": self.entity_diff_file if diff_summary else None
        }

    def save(self):
        logging.info(f"Writing metadata → {self.metadata_file}")
        os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
        serializable_metadata = self.make_json_serializable(self.metadata)
        with open(self.metadata_file, 'w') as mf:
            json.dump(serializable_metadata, mf, indent=2)

    def run(self):
        self.process()
        self.save()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Transform UniProt data")
    parser.add_argument(
        "--config",
        type=str,
        default="config/targets_config.yaml",
        help="YAML config (default: config/targets_config.yaml)"
    )

    args = parser.parse_args()
    cfg = yaml.safe_load(open(args.config))
    UniProtTransformer(cfg).run()