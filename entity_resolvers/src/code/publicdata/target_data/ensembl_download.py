#!/usr/bin/env python
"""
ensembl_download.py – Download Ensembl BioMart data using embedded queries.

Downloads 4 BioMart queries, detects Ensembl release version,
writes standardized dl_ensembl_metadata.json for the version manifest.

NO raw-file diffs — version tracking happens on cleaned output in the transformer.
"""

import os
import json
import yaml
import logging
import argparse
import hashlib
import re
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from requests.exceptions import HTTPError
from tqdm import tqdm


def setup_logging(config):
    log_file = config.get("download_log_file") or config.get("log_file")
    handlers = [logging.StreamHandler()]
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        handlers.insert(0, logging.FileHandler(log_file))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
        force=True,
    )


class EnsemblDownloader:
    def __init__(self, full_config):
        self.full_config = full_config
        self.config = full_config["ensembl_data"]
        setup_logging(self.config)

        # Embedded BioMart queries (self-contained, no external XML files needed)
        self.queries = [
            """<!DOCTYPE Query>
            <Query virtualSchemaName="default" formatter="TSV" header="1" uniqueRows="1" count="" datasetConfigVersion="0.6">
                <Dataset name="hsapiens_gene_ensembl" interface="default">
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
            </Query>""",
            """<!DOCTYPE Query>
            <Query virtualSchemaName="default" formatter="TSV" header="1" uniqueRows="1" count="" datasetConfigVersion="0.6">
                <Dataset name="hsapiens_gene_ensembl" interface="default">
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
            </Query>""",
            """<!DOCTYPE Query>
            <Query virtualSchemaName="default" formatter="TSV" header="1" uniqueRows="1" count="" datasetConfigVersion="0.6">
                <Dataset name="hsapiens_gene_ensembl" interface="default">
                    <Attribute name="ensembl_gene_id" />
                    <Attribute name="ensembl_gene_id_version" />
                    <Attribute name="ensembl_transcript_id" />
                    <Attribute name="ensembl_transcript_id_version" />
                    <Attribute name="transcript_mane_select" />
                    <Attribute name="refseq_mrna" />
                    <Attribute name="refseq_ncrna" />
                    <Attribute name="refseq_peptide" />
                </Dataset>
            </Query>""",
            """<!DOCTYPE Query>
            <Query virtualSchemaName="default" formatter="TSV" header="1" uniqueRows="1" count="" datasetConfigVersion="0.6">
                <Dataset name="hsapiens_gene_ensembl" interface="default">
                    <Attribute name="ensembl_gene_id" />
                    <Attribute name="ensembl_gene_id_version" />
                    <Attribute name="description" />
                    <Attribute name="chromosome_name" />
                    <Attribute name="strand" />
                    <Attribute name="start_position" />
                    <Attribute name="end_position" />
                </Dataset>
            </Query>""",
        ]

        self.output_paths = self.config["output_paths"]["biomart_csvs"]

        self.meta_log = self.config.get(
            "dl_metadata_file",
            "src/data/publicdata/target_data/metadata/dl_ensembl_metadata.json",
        )

        # Load previous metadata to compare release versions
        self.old_meta = {}
        if os.path.exists(self.meta_log):
            try:
                with open(self.meta_log) as f:
                    self.old_meta = json.load(f)
            except Exception:
                pass

        # Detect release for version manifest
        ver, rel_label = self._detect_ensembl_release()

        self.metadata = {
            "source_name": "Ensembl BioMart",
            "source_version": ver,
            "release_date": rel_label,
            "url": "https://www.ensembl.org/biomart/martservice",
            "download_start": datetime.now().isoformat(),
            "download_end": None,
            "outputs": [],
            "updated": False,
            "status": "unknown",
        }

    def _detect_ensembl_release(self):
        """Get release number and date from the Ensembl REST API."""
        try:
            resp = requests.get(
                "https://rest.ensembl.org/info/data?",
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            if resp.status_code == 200:
                data = resp.json()
                releases = data.get("releases", [])
                if releases:
                    ver = str(max(releases))
                    logging.info(f"Ensembl release detected: {ver}")
                    return ver, None
        except Exception as e:
            logging.warning(f"Could not detect Ensembl release via REST: {e}")

        # Fallback: scrape BioMart homepage
        try:
            homepage = requests.get("https://www.ensembl.org/biomart/martview", timeout=10).text
            m = re.search(r"Ensembl release\s+(\d+)\s*(?:-\s*([A-Za-z]+\s+\d{4}))?", homepage, re.IGNORECASE)
            if m:
                return m.group(1), m.group(2)
        except Exception:
            pass

        return "unknown", None

    def compute_hash(self, file_path):
        h = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                h.update(chunk)
        return h.hexdigest()

    def fetch_biomart_data(self, query, output_csv):
        """Execute a single BioMart query, write CSV. Returns status dict."""
        temp_csv = output_csv + ".tmp"
        url = "https://www.ensembl.org/biomart/martservice?query="

        os.makedirs(os.path.dirname(output_csv), exist_ok=True)

        try:
            with requests.get(url + query, stream=True, timeout=300) as response:
                response.raise_for_status()
                total_size = int(response.headers.get("content-length", 0))
                tqdm_bar = tqdm(total=total_size, unit="iB", unit_scale=True,
                                desc=os.path.basename(output_csv))
                chunks = []
                for chunk in response.iter_content(1024):
                    chunks.append(chunk)
                    tqdm_bar.update(len(chunk))
                tqdm_bar.close()
                content = b"".join(chunks).decode("utf-8", errors="replace")

                lines = content.splitlines()
                if not lines:
                    logging.error("Empty response from BioMart")
                    return {"status": "error", "records": 0}

                header = lines[0].split("\t")
                rows = [line.split("\t") for line in lines[1:]]
                pd.DataFrame(rows, columns=header).to_csv(temp_csv, index=False)

        except (HTTPError, Exception) as e:
            logging.error(f"BioMart query failed: {e}")
            return {"status": "error", "records": 0}

        # Compare hashes — skip if unchanged
        old_hash = self.compute_hash(output_csv) if os.path.exists(output_csv) else None
        new_hash = self.compute_hash(temp_csv)

        if old_hash and old_hash == new_hash:
            os.remove(temp_csv)
            return {"status": "skipped", "old_hash": old_hash, "new_hash": new_hash, "records": len(rows)}

        os.replace(temp_csv, output_csv)
        return {
            "status": "updated" if old_hash else "new",
            "old_hash": old_hash,
            "new_hash": new_hash,
            "records": len(rows),
        }

    def fetch(self):
        current_ver = self.metadata["source_version"]
        previous_ver = self.old_meta.get("source_version") or self.old_meta.get("version")
        all_files_exist = all(os.path.exists(p) for p in self.output_paths)

        # Skip ALL downloads if release version matches and all raw files exist
        if (current_ver and current_ver != "unknown"
                and previous_ver == current_ver
                and all_files_exist):
            logging.info(
                f"Ensembl release unchanged ({current_ver}) and all {len(self.output_paths)} "
                f"raw files present — skipping download."
            )
            for output_csv in self.output_paths:
                self.metadata["outputs"].append({
                    "path": output_csv,
                    "timestamp": datetime.now().isoformat(),
                    "status": "skipped",
                })
            self.metadata["download_end"] = datetime.now().isoformat()
            self.metadata["status"] = "no_change"

            os.makedirs(os.path.dirname(self.meta_log), exist_ok=True)
            with open(self.meta_log, "w") as f:
                json.dump(self.metadata, f, indent=2)
            logging.info(f"Metadata written: {self.meta_log}")
            return

        # New release or missing files — download everything
        if current_ver != previous_ver:
            logging.info(f"Ensembl release changed: {previous_ver} → {current_ver}")
        elif not all_files_exist:
            missing = [p for p in self.output_paths if not os.path.exists(p)]
            logging.info(f"Missing raw files: {missing}")

        for query, output_csv in zip(self.queries, self.output_paths):
            result = self.fetch_biomart_data(query, output_csv)
            if result["status"] in ("new", "updated"):
                self.metadata["updated"] = True

            self.metadata["outputs"].append({
                "path": output_csv,
                "timestamp": datetime.now().isoformat(),
                **result,
            })

        self.metadata["download_end"] = datetime.now().isoformat()
        self.metadata["status"] = "updated" if self.metadata["updated"] else "no_change"

        os.makedirs(os.path.dirname(self.meta_log), exist_ok=True)
        with open(self.meta_log, "w") as f:
            json.dump(self.metadata, f, indent=2)
        logging.info(f"Metadata written: {self.meta_log}")

    def run(self):
        self.fetch()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Ensembl BioMart data")
    parser.add_argument("--config", type=str, default="config/targets_config.yaml")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    EnsemblDownloader(config).run()
    logging.info("✅ Ensembl download complete.")