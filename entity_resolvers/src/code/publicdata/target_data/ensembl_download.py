#!/usr/bin/env python
"""
ensembl_download.py – Download Ensembl BioMart data using embedded queries with diff tracking and logging.
"""

import os
import time
import yaml
import logging
import argparse
from pathlib import Path
from datetime import datetime
from requests.exceptions import HTTPError
import requests
import pandas as pd
from tqdm import tqdm
import hashlib
import difflib
import shutil

def setup_logging(config):
    log_file = config.get("log_file")
    handlers = [logging.StreamHandler()]
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        handlers.insert(0, logging.FileHandler(log_file))
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s",
                        handlers=handlers)

class EnsemblDownloader:
    def __init__(self, full_config):
        self.full_config = full_config
        self.config = full_config["ensembl_data"]
        self.qc_mode = self.config.get("qc_mode", full_config.get("global", {}).get("qc_mode", True))
        setup_logging(self.config)

        # Embedded BioMart queries
        self.queries = [
            """
            <!DOCTYPE Query>
            <Query virtualSchemaName="default" formatter="TSV" header="1" uniqueRows="1" count="" datasetConfigVersion="0.6" >
                <Dataset name="hsapiens_gene_ensembl" interface="default" >
                    <Attribute name="ensembl_gene_id" />
                    <Attribute name="ensembl_gene_id_version" />
                    <Attribute name="ensembl_transcript_id" />
                    <Attribute name="ensembl_transcript_id_version" />
                    <Attribute name="ensembl_peptide_id" />
                    <Attribute name="ensembl_peptide_id_version" />
                    <Attribute name="external_gene_name" />
                    <Attribute name="gene_biotype" />
                    <Attribute name="transcript_is_canonical" />
                    <Attribute name="external_synonym" />
                    <Attribute name="transcript_tsl" />
                    <Attribute name="entrezgene_id" />
                    <Attribute name="hgnc_id" />
                </Dataset>
            </Query>
            """,
            """
            <!DOCTYPE Query>
            <Query virtualSchemaName="default" formatter="TSV" header="1" uniqueRows="1" count="" datasetConfigVersion="0.6" >
                <Dataset name="hsapiens_gene_ensembl" interface="default" >
                    <Attribute name="ensembl_gene_id" />
                    <Attribute name="ensembl_gene_id_version" />
                    <Attribute name="ensembl_transcript_id" />
                    <Attribute name="ensembl_transcript_id_version" />
                    <Attribute name="ensembl_peptide_id" />
                    <Attribute name="ensembl_peptide_id_version" />
                    <Attribute name="uniprotswissprot" />
                    <Attribute name="uniprotsptrembl" />
                    <Attribute name="uniprot_isoform" />
                </Dataset>
            </Query>
            """,
            """
            <!DOCTYPE Query>
            <Query virtualSchemaName="default" formatter="TSV" header="1" uniqueRows="1" count="" datasetConfigVersion="0.6" >
                <Dataset name="hsapiens_gene_ensembl" interface="default" >
                    <Attribute name="ensembl_gene_id" />
                    <Attribute name="ensembl_gene_id_version" />
                    <Attribute name="ensembl_transcript_id" />
                    <Attribute name="ensembl_transcript_id_version" /> 
                    <Attribute name="transcript_mane_select" />
                    <Attribute name="refseq_mrna" />
                    <Attribute name="refseq_ncrna" />
                    <Attribute name="refseq_peptide" />
                </Dataset>
            </Query>
            """,
            """
            <!DOCTYPE Query>
            <Query virtualSchemaName="default" formatter="TSV" header="1" uniqueRows="1" count="" datasetConfigVersion="0.6" >
                <Dataset name="hsapiens_gene_ensembl" interface="default" >
                    <Attribute name="ensembl_gene_id" />
                    <Attribute name="ensembl_gene_id_version" />
                    <Attribute name="description" /> 
                    <Attribute name="chromosome_name" />
                    <Attribute name="strand" />
                    <Attribute name="start_position" />
                    <Attribute name="end_position" />
                </Dataset>
            </Query>
            """
        ]

        self.output_paths = self.config["output_paths"]["biomart_csvs"]
        self.meta_log = self.config.get("dl_metadata_file", "ensembl_metadata.json")

        self.metadata = {
            "download_start": datetime.now().isoformat(),
            "source": {
                "name": "Ensembl BioMart",
                "url": "http://www.ensembl.org/biomart/martservice?query=",
                "queries": [" ".join(q.split()) for q in self.queries],
                "version": self.config.get("version", "unknown")
            },
            "outputs": [],
            "download_end": None
        }

    def is_stale(self, file_path, days=7):
        return not Path(file_path).exists() or (time.time() - os.path.getmtime(file_path)) / 86400 > days

    def compute_hash(self, file_path):
        h = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                h.update(chunk)
        return h.hexdigest()

    def fetch_biomart_data(self, query, output_csv):
        temp_csv = output_csv + ".temp"
        url = "http://www.ensembl.org/biomart/martservice?query="
        try:
            with requests.get(url + query, stream=True) as response:
                response.raise_for_status()
                total_size = int(response.headers.get("content-length", 0))
                block_size = 1024
                tqdm_bar = tqdm(total=total_size, unit="iB", unit_scale=True)
                chunks = []
                for chunk in response.iter_content(block_size):
                    chunks.append(chunk)
                    tqdm_bar.update(len(chunk))
                tqdm_bar.close()
                content = b"".join(chunks).decode("utf-8")
                lines = content.splitlines()
                header = lines[0].split("\t")
                rows = [line.split("\t") for line in lines[1:]]
                pd.DataFrame(rows, columns=header).to_csv(temp_csv, index=False)
        except HTTPError as e:
            logging.error(f"Failed query: {e}")
            return "error", None, None

        if os.path.exists(output_csv):
            if self.compute_hash(output_csv) == self.compute_hash(temp_csv):
                os.remove(temp_csv)
                return "skipped", None, None

            logging.info(f"Update found for {output_csv}")
            backup = output_csv + ".backup"
            shutil.copy2(output_csv, backup)

            base = os.path.splitext(os.path.basename(output_csv))[0]
            diff_txt = os.path.join(os.path.dirname(output_csv), f"{base}.diff.txt")
            diff_html = os.path.join(os.path.dirname(output_csv), f"{base}.diff.html")
            max_diff_lines = 100

            try:
                with open(backup, "r", encoding="utf-8", errors="ignore") as old, \
                     open(temp_csv, "r", encoding="utf-8", errors="ignore") as new:
                    old_lines = old.readlines()
                    new_lines = new.readlines()

                full_diff = list(difflib.unified_diff(old_lines, new_lines, fromfile="old", tofile="new"))
                limited_diff = full_diff[:max_diff_lines]
                if len(full_diff) > max_diff_lines:
                    limited_diff.append(f"... (truncated, total diff lines: {len(full_diff)})\n")

                with open(diff_txt, "w", encoding="utf-8") as t:
                    t.writelines(limited_diff)

                if len(full_diff) <= max_diff_lines:
                    with open(diff_html, "w", encoding="utf-8") as h:
                        h.write(difflib.HtmlDiff().make_file(
                            old_lines, new_lines, fromdesc="Old", todesc="New", context=True, numlines=2
                        ))
                else:
                    diff_html = None

                logging.info(f"Diff saved: {diff_txt}" + (f" and {diff_html}" if diff_html else ""))

            except Exception as e:
                logging.warning(f"Diff generation failed for {output_csv}: {e}")
                diff_txt, diff_html = None, None

            os.replace(temp_csv, output_csv)

            if not self.qc_mode:
                for f in [backup, diff_txt, diff_html]:
                    if f and os.path.exists(f):
                        os.remove(f)
            return "updated", diff_txt, diff_html

        else:
            os.replace(temp_csv, output_csv)
            return "new", None, None

    def fetch(self):
        for query, output_csv in zip(self.queries, self.output_paths):
            if os.path.exists(output_csv) and not self.is_stale(output_csv, days=1):
                logging.info(f"Skipping fresh file: {output_csv}")
                self.metadata["outputs"].append({
                    "output": output_csv,
                    "timestamp": datetime.now().isoformat(),
                    "source": "BioMart (cached)",
                    "status": "skipped"
                })
                continue

            status, diff_txt, diff_html = self.fetch_biomart_data(query, output_csv)
            self.metadata["outputs"].append({
                "output": output_csv,
                "timestamp": datetime.now().isoformat(),
                "source": "BioMart",
                "status": status,
                "diff_file_text": diff_txt,
                "diff_file_html": diff_html
            })

        self.metadata["download_end"] = datetime.now().isoformat()
        with open(self.meta_log, "w") as f:
            yaml.dump(self.metadata, f, default_flow_style=False, sort_keys=False)
        logging.info(f"Metadata written: {self.meta_log}")

    def run(self):
        self.fetch()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Ensembl BioMart data")
    parser.add_argument("--config", type=str, default="config/targets/targets_config.yaml")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    EnsemblDownloader(config).run()
    logging.info("✅ Ensembl download complete.")
