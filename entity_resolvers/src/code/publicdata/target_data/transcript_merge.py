#!/usr/bin/env python
"""
transcript_merge.py - Merge transcript sources with provenance & metadata

This script reads BioMart TSVs, SPARQL isoform xrefs, RefSeq↔Ensembl and RefSeq data,
merges them step by step applying provenance logic, flags issues for review,
saves final CSV and a metadata JSON file summarizing each processing step.
"""

import os
import json
import yaml
import logging
import argparse
import pandas as pd
import requests
from datetime import datetime
from tqdm import tqdm
import urllib3
from requests.exceptions import HTTPError
from urllib.parse import quote
import hashlib
import difflib
import shutil


# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from publicdata.target_data.download_utils import setup_logging
from publicdata.target_data.shared.output_versioning import save_versioned_output

class TranscriptResolver:
    def __init__(self, full_cfg):
        # use only the transcript_merge section if present
        self.qc_mode = full_cfg.get("global", {}).get("qc_mode", True)
        self.cfg = full_cfg.get("transcript_merge", full_cfg)
        setup_logging(self.cfg.get("log_file", ""))
        logging.info("🚀 Initializing TranscriptMergeProcessor")
        self.metadata = {
            "timestamp": {"start": datetime.now().isoformat()},
            "processing_steps": [],
            "outputs": []
        }
        # Paths from config
        self.biomart_csv          = self.cfg["biomart_output"]
        self.isoform_csv          = self.cfg["isoform_file"]
        self.refseq_ensembl_csv   = self.cfg["refseq_ensembl_file"]
        self.refseq_csv           = self.cfg["refseq_file"]
        self.transformed_path     = self.cfg["transformed_data_path"]
        self.metrics_file         = self.cfg["metrics_file"]
        self.metadata_file        = self.cfg["metadata_file"]

    def log_provenance(self, action, description, details=None, start_time=None, end_time=None):
        entry = {
            "action": action,
            "description": description,
            "details": details or {},
            "timestamp": datetime.now().isoformat()
        }
        if start_time and end_time:
            entry["start_time"] = start_time.isoformat()
            entry["end_time"]   = end_time.isoformat()
            entry["duration_seconds"] = (end_time - start_time).total_seconds()
        self.metadata["processing_steps"].append(entry)

    def _compute_hash(self, path):
        h = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                h.update(chunk)
        return h.hexdigest()

    def fetch_biomart_data(self):
        start = datetime.now()
        logging.info("STEP 1: fetch_biomart_data")

        output_csv = self.biomart_csv
        temp_csv   = output_csv + ".tmp"

        # build the query string
        bm_query = quote("""
            <!DOCTYPE Query><Query virtualSchemaName="default" formatter="TSV" header="1"
                    uniqueRows="1" count="" datasetConfigVersion="0.6">
                <Dataset name="hsapiens_gene_ensembl" interface="default">
                    <Attribute name="ensembl_gene_id"/>
                    <Attribute name="external_transcript_name"/>
                    <Attribute name="external_gene_name"/>
                    <Attribute name="ensembl_transcript_id"/>
                    <Attribute name="ensembl_transcript_id_version"/>
                    <Attribute name="transcript_biotype"/>
                    <Attribute name="transcript_start"/>
                    <Attribute name="transcript_end"/>
                    <Attribute name="transcription_start_site"/>
                    <Attribute name="transcript_length"/>
                </Dataset>
            </Query>
        """)
        url = f"http://www.ensembl.org/biomart/martservice?query={bm_query}"

        # 1) download into temp_csv
        try:
            print(f"  ⬇ Downloading BioMart data from ensembl.org …")
            with requests.get(url, stream=True, verify=False) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("content-length", 0)) or None
                with tqdm(total=total, unit="B", unit_scale=True, unit_divisor=1024,
                          desc=os.path.basename(output_csv),
                          miniters=1, dynamic_ncols=True) as bar, \
                     open(temp_csv, "wb") as tmpf:
                    for chunk in resp.iter_content(65536):
                        tmpf.write(chunk)
                        bar.update(len(chunk))
        except HTTPError as e:
            logging.error("Failed to fetch BioMart data: %s", e)
            if os.path.exists(temp_csv):
                os.remove(temp_csv)
            self.log_provenance("fetch_biomart_data", "ERROR", details={"error": str(e)},
                                start_time=start, end_time=datetime.now())
            return

        # 2) validate download — BioMart may return HTML or plain-text error pages
        with open(temp_csv, "r", encoding="utf-8", errors="ignore") as f:
            first_line = f.readline().strip()

        first_line_lower = first_line.lower()
        biomart_error = (
            first_line.startswith("<")
            or first_line_lower.startswith("<!doctype")
            or first_line_lower.startswith("query error")
            or "biomart::exception" in first_line_lower
            or "could not connect to mysql database" in first_line_lower
            or "can't connect to mysql server" in first_line_lower
        )

        if biomart_error:
            logging.error(
                "BioMart returned an error response instead of TSV data. First line: %s",
                first_line
            )
            print("  ⚠️  BioMart returned an error response — skipping update, keeping existing data.")
            os.remove(temp_csv)
            self.log_provenance(
                "fetch_biomart_data",
                "BioMart error response",
                details={"status": "error_response", "first_line": first_line[:500]},
                start_time=start,
                end_time=datetime.now()
            )
            return

        # 3) compute hashes
        new_hash = self._compute_hash(temp_csv)
        old_hash = None
        if os.path.exists(output_csv):
            old_hash = self._compute_hash(output_csv)

        # 4) decide action
        status = None
        diff_txt  = None
        diff_html = None

        if old_hash is not None and old_hash == new_hash:
            logging.info("No updates detected for %s; skipping", output_csv)
            os.remove(temp_csv)
            status = "skipped"

        else:
            # if there's an old file, back it up and diff
            if old_hash is not None:
                logging.info("Update detected for %s; creating backup + diffs", output_csv)
                backup = output_csv + ".backup"
                shutil.copy2(output_csv, backup)

                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                base = os.path.splitext(os.path.basename(output_csv))[0]
                qc_dir = "src/data/publicdata/target_data/qc"
                os.makedirs(qc_dir, exist_ok=True)
                diff_txt = os.path.join(qc_dir, f"{base}_diff_{ts}.txt")
                diff_html = os.path.join(qc_dir, f"{base}_diff_{ts}.html")

                try:
                    MAX_DIFF_LINES = 50_000  # skip diffs on very large files
                    with open(backup,  "r", encoding="utf-8", errors="ignore") as fo:
                        old_lines = fo.readlines()
                    with open(temp_csv, "r", encoding="utf-8", errors="ignore") as fn:
                        new_lines = fn.readlines()

                    if max(len(old_lines), len(new_lines)) > MAX_DIFF_LINES:
                        logging.info(
                            "Files too large for diff (%d / %d lines, limit %d). "
                            "Writing line-count summary only.",
                            len(old_lines), len(new_lines), MAX_DIFF_LINES
                        )
                        with open(diff_txt, "w", encoding="utf-8") as f:
                            f.write(f"old: {len(old_lines)} lines\n"
                                    f"new: {len(new_lines)} lines\n"
                                    f"Diff skipped (>{MAX_DIFF_LINES} lines).\n")
                        diff_html = None
                    else:
                        # unified diff, zero-context
                        print("  generating unified diff …")
                        dtext = "".join(difflib.unified_diff(
                            old_lines, new_lines,
                            fromfile="old", tofile="new", n=0
                        ))
                        with open(diff_txt, "w", encoding="utf-8") as f:
                            f.write(dtext)

                        # HTML diff
                        print("  generating HTML diff …")
                        html = difflib.HtmlDiff().make_file(
                            old_lines, new_lines,
                            fromdesc="old", todesc="new",
                            context=True, numlines=0
                        )
                        with open(diff_html, "w", encoding="utf-8") as f:
                            f.write(html)

                    logging.info("Diffs written: %s, %s", diff_txt, diff_html)
                except Exception as e:
                    logging.error("Error generating diff: %s", e)
                    diff_txt = diff_html = None

                status = "updated"

            else:
                logging.info("%s not found; saving new file", output_csv)
                status = "new"

            # replace
            os.makedirs(os.path.dirname(output_csv), exist_ok=True)
            os.replace(temp_csv, output_csv)

        # 4) record in metadata
        details = {"status": status}
        if diff_txt:  details["diff_txt"]  = diff_txt
        if diff_html: details["diff_html"] = diff_html

        self.log_provenance(
            "fetch_biomart_data",
            "Downloaded and diffed BioMart TSV",
            details=details,
            start_time=start,
            end_time=datetime.now()
        )

    def process_biomart_csv(self):
        start = datetime.now()
        logging.info("STEP 2: process_biomart_csv")

        df = pd.read_csv(self.biomart_csv, sep="\t", dtype=str)
        print("BioMart original columns:", df.columns.tolist())

        # Support both BioMart GUI/display headers and raw attribute headers
        renames = {
            # BioMart display labels
            "Gene stable ID": "ensembl_gene_id",
            "Transcript stable ID": "ensembl_transcript_id",
            "Transcript stable ID version": "ensembl_transcript_id_version",
            "Transcript type": "ensembl_transcript_type",
            "Transcript start (bp)": "ensembl_trans_bp_start",
            "Transcript end (bp)": "ensembl_trans_bp_end",
            "Transcription start site (TSS)": "ensembl_trans_start_site",
            "Transcript length (including UTRs and CDS)": "ensembl_trans_length",
            "Gene name": "ensembl_symbol",
            "Transcript name": "ensembl_transcript_name",

            # BioMart attribute-style headers
            "ensembl_gene_id": "ensembl_gene_id",
            "ensembl_transcript_id": "ensembl_transcript_id",
            "ensembl_transcript_id_version": "ensembl_transcript_id_version",
            "transcript_biotype": "ensembl_transcript_type",
            "transcript_start": "ensembl_trans_bp_start",
            "transcript_end": "ensembl_trans_bp_end",
            "transcription_start_site": "ensembl_trans_start_site",
            "transcript_length": "ensembl_trans_length",
            "external_gene_name": "ensembl_symbol",
            "external_transcript_name": "ensembl_transcript_name",
        }

        df.rename(columns=renames, inplace=True)
        logging.info("Renamed BioMart columns")

        # Fail fast if BioMart returned junk or an unexpected schema
        required = [
            "ensembl_gene_id",
            "ensembl_transcript_id",
            "ensembl_transcript_id_version",
        ]
        missing = [c for c in required if c not in df.columns]

        if missing:
            msg = (
                f"BioMart file is invalid or missing required columns: {missing}. "
                f"Observed columns: {df.columns.tolist()}"
            )
            logging.error(msg)
            self.log_provenance(
                "process_biomart_csv",
                "ERROR",
                details={
                    "missing_columns": missing,
                    "observed_columns": df.columns.tolist()
                },
                start_time=start,
                end_time=datetime.now()
            )
            raise ValueError(msg)

        # Normalize empties
        df = df.where(pd.notnull(df), None)

        end = datetime.now()
        self.log_provenance(
            "process_biomart_csv",
            "Renamed and validated BioMart columns",
            details={"columns": df.columns.tolist()},
            start_time=start,
            end_time=end
        )
        return df

    def merge_isoforms(self, df):
        start = datetime.now()
        logging.info("STEP 3: merge_isoforms")

        if "ensembl_transcript_id_version" not in df.columns:
            raise ValueError(
                "Cannot merge isoforms: 'ensembl_transcript_id_version' is missing from BioMart dataframe."
            )

        iso = pd.read_csv(self.isoform_csv, low_memory=False)
        keep = [
            'ensembl_transcript_id_version', 'ensembl_transcript_tsl',
            'ensembl_canonical', 'ensembl_refseq_NM',
            'ensembl_refseq_NR', 'ensembl_refseq_MANEselect'
        ]
        iso = iso[[c for c in keep if c in iso.columns]]

        if "ensembl_transcript_id_version" not in iso.columns:
            raise ValueError(
                "Cannot merge isoforms: 'ensembl_transcript_id_version' is missing from isoform file."
            )

        merged = pd.merge(df, iso, on='ensembl_transcript_id_version', how='left')
        logging.info("Merged isoform data (%d rows)", len(merged))

        end = datetime.now()
        self.log_provenance("merge_isoforms", "Merged isoform data", start_time=start, end_time=end)
        return merged

    def merge_refseq_ensembl(self, df):
        start = datetime.now()
        logging.info("STEP 4: merge_refseq_ensembl")
        xref = pd.read_csv(self.refseq_ensembl_csv, sep=None, engine="python", dtype=str)
        xref = xref.loc[:, ~xref.columns.duplicated()]
        lower = {c.lower(): c for c in xref.columns}
        rna_key = lower.get("rna_nucleotide_accession.version")
        ens_key = lower.get("ensembl_rna_identifier")
        if not rna_key or not ens_key:
            logging.warning("Skipping RefSeq↔Ensembl merge, missing columns")
            self.log_provenance("merge_refseq_ensembl", "skipped", details={"found_columns": list(xref.columns)}, start_time=start, end_time=datetime.now())
            return df
        x2 = xref[[rna_key, ens_key]].rename(columns={
            rna_key: "refseq_nuc_accession",
            ens_key: "refseq_ensembl_transcript"
        })
        merged = pd.merge(df, x2, left_on="ensembl_transcript_id_version", right_on="refseq_ensembl_transcript", how="left")
        logging.info("Performed RefSeq↔Ensembl merge (%d rows)", len(merged))
        end = datetime.now()
        self.log_provenance("merge_refseq_ensembl", "Performed RefSeq↔Ensembl merge", start_time=start, end_time=end)
        return merged

    def merge_refseq(self, df):
        start = datetime.now()
        logging.info("STEP 5: merge_refseq")
        r = pd.read_csv(self.refseq_csv, low_memory=False)
        keep = ['refseq_ncbi_id','refseq_status','refseq_rna_id','refseq_symbol']
        r = r[[c for c in keep if c in r.columns]].replace('-', pd.NA)

        if 'refseq_nuc_accession' not in df.columns:
            fallback_cols = [
                c for c in [
                    'ensembl_refseq_MANEselect',
                    'ensembl_refseq_NM',
                    'ensembl_refseq_NR',
                ] if c in df.columns
            ]
            if fallback_cols:
                df['refseq_nuc_accession'] = (
                    df[fallback_cols]
                    .apply(lambda row: next((v for v in row if pd.notna(v) and str(v).strip()), pd.NA), axis=1)
                )
                logging.info(
                    "Derived refseq_nuc_accession from fallback columns: %s",
                    fallback_cols,
                )
            else:
                df['refseq_nuc_accession'] = pd.NA

        wanted = set(df['refseq_nuc_accession'].dropna().unique())
        if wanted:
            r = r[r['refseq_rna_id'].isin(wanted)]
            logging.info("Filtered RefSeq to %d rows matching Ensembl transcripts", len(r))
        merged = pd.merge(df, r,
                          left_on='refseq_nuc_accession',
                          right_on='refseq_rna_id',
                          how='left', suffixes=('','_refseq')).drop_duplicates()
        logging.info("Merged with RefSeq (%d rows)", len(merged))
        end = datetime.now()
        self.log_provenance("merge_refseq", "Merged with RefSeq", details={"rows": len(merged)}, start_time=start, end_time=end)
        return merged

    def compute_provenance(self, df):
        start = datetime.now()
        logging.info("STEP 6: compute_provenance")
        # Ensembl vs RefSeq transcript provenance
        df['Ensembl_Transcript_ID_Provenance'] = df.apply(
            lambda r: None
                      if pd.isna(r['ensembl_transcript_id_version']) and pd.isna(r.get('refseq_ensembl_transcript'))
                      else 'ensembl, refseq' if (
                          pd.notna(r['ensembl_transcript_id_version']) and
                          r['ensembl_transcript_id_version'] == r.get('refseq_ensembl_transcript')
                      )
                      else 'ensembl' if pd.notna(r['ensembl_transcript_id_version'])
                      else 'refseq' if pd.notna(r.get('refseq_ensembl_transcript'))
                      else 'error',
            axis=1
        )
        df['RefSeq_Provenance'] = df.apply(
            lambda r: None
                      if pd.isna(r.get('ensembl_refseq_MANEselect')) and pd.isna(r.get('refseq_rna_id'))
                      else 'ensembl, refseq' if (
                          pd.notna(r.get('ensembl_refseq_MANEselect')) and
                          r['ensembl_refseq_MANEselect'] == r.get('refseq_rna_id')
                      )
                      else 'ensembl' if pd.notna(r.get('ensembl_refseq_MANEselect'))
                      else 'refseq' if pd.notna(r.get('refseq_rna_id'))
                      else 'error',
            axis=1
        )
        logging.info("Applied provenance logic")
        end = datetime.now()
        self.log_provenance("compute_provenance", "Applied provenance logic", start_time=start, end_time=end)
        return df

    def merge_symbols(self, df):
        start = datetime.now()
        logging.info("STEP 7: merge_symbols")
        df['symbol'] = df.apply(
            lambda r: r['ensembl_symbol']
                      if pd.notna(r['ensembl_symbol']) and r['ensembl_symbol'] == r.get('refseq_symbol','')
                      else '|'.join(filter(None, [
                          str(r['ensembl_symbol']) if pd.notna(r['ensembl_symbol']) else '',
                          str(r.get('refseq_symbol','')) if pd.notna(r.get('refseq_symbol','')) else ''
                      ])),
            axis=1
        )
        df.drop(['ensembl_symbol','refseq_symbol'], axis=1, inplace=True)
        logging.info("Merged symbol columns")
        end = datetime.now()
        self.log_provenance("merge_symbols", "Coalesced symbols", start_time=start, end_time=end)
        return df

    def count_metrics(self, df):
        start = datetime.now()
        logging.info("STEP 8: count_metrics")

        total_ens = df['ensembl_transcript_id_version'].dropna().nunique()
        total_ref = df['refseq_rna_id'].dropna().nunique()
        status_counts = df['refseq_status'].value_counts().to_dict()
        mapped_both = int((df['Ensembl_Transcript_ID_Provenance'] == 'ensembl, refseq').sum())

        metrics = {
            "Total Rows": len(df),
            "Unique Ensembl Transcripts": total_ens,
            "Unique RefSeq Transcripts": total_ref,
            "Mapped Ensembl & RefSeq": mapped_both,
            **status_counts
        }

        for k, v in metrics.items():
            print(f"{k}: {v}")

        if self.qc_mode and self.metrics_file:
            os.makedirs(os.path.dirname(self.metrics_file), exist_ok=True)
            pd.DataFrame(metrics.items(), columns=['Metric', 'Count']) \
                .to_csv(self.metrics_file, index=False)
            logging.info("Saved metrics to %s", self.metrics_file)
        else:
            logging.info("QC mode disabled – skipping metrics file write")

        end = datetime.now()
        self.log_provenance("count_metrics", "Calculated transcript metrics", details=metrics, start_time=start, end_time=end)

    def flag_review(self, df):
        start = datetime.now()
        logging.info("STEP 9: flag_review")

        dup = df[df.duplicated()]
        errs = df[df[['Ensembl_Transcript_ID_Provenance', 'RefSeq_Provenance']].isin(['error']).any(axis=1)]
        supp = df[df['refseq_status'] == 'SUPPRESSED']
        flagged = pd.concat([dup, errs, supp]).drop_duplicates()

        flagged_path = os.path.join(os.path.dirname(self.metrics_file), "transcripts_flagged_for_review.csv")

        if self.qc_mode:
            os.makedirs(os.path.dirname(flagged_path), exist_ok=True)
            flagged.to_csv(flagged_path, index=False)
            logging.info("Flagged rows saved to %s", flagged_path)
        else:
            logging.info("QC mode disabled – skipping flagged transcript write")

        clean = df.drop(flagged.index, errors='ignore')
        end = datetime.now()
        self.log_provenance("flag_review", "Flagged & removed bad transcripts", details={"flagged": len(flagged)}, start_time=start, end_time=end)
        return clean

    def save_outputs(self, df):
        start = datetime.now()
        logging.info("STEP 10: save_outputs")
        ver_result = save_versioned_output(
            df=df,
            output_path=self.transformed_path,
            id_col="ensembl_transcript_id_version",
            sep=",",
            output_kind="provenance_merge_table",
        )
        self.metadata["output_versioning"] = ver_result
        end = datetime.now()
        self.log_provenance("save_outputs", "Saved final CSV & metadata", start_time=start, end_time=end)
        with open(self.metadata_file, "w") as mf:
            json.dump(self.metadata, mf, indent=2)
        logging.info("Saved final data to %s and metadata to %s", self.transformed_path, self.metadata_file)

    def run(self):
        self.fetch_biomart_data()
        df = self.process_biomart_csv()
        df = self.merge_isoforms(df)
        df = self.merge_refseq_ensembl(df)
        df = self.merge_refseq(df)
        df = self.compute_provenance(df)
        df = self.merge_symbols(df)
        self.count_metrics(df)
        cleaned = self.flag_review(df)
        self.save_outputs(cleaned)
        logging.info("🎉 Transcript pipeline complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge transcript sources with provenance & metadata")
    parser.add_argument("--config", type=str,
               default="config/targets_config.yaml",
               help="YAML config (default: config/targets_config.yaml)")

    args = parser.parse_args()

    cfg_all = yaml.safe_load(open(args.config))
    processor = TranscriptResolver(cfg_all)
    processor.run()
