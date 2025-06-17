#!/usr/bin/env python
"""
uniprot_download.py - Download UniProt data with update detection,
 detailed metadata logging, MD5 hash checking, SPARQL isoform queries, diff generation,
 memory-efficient decompression, and UniProt idmapping download.
"""

import os
import sys
import json
import shutil
import hashlib
import logging
import argparse
import subprocess
import requests
import difflib
import yaml
import pandas as pd
from tqdm import tqdm
from datetime import datetime

def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    handlers = [
        logging.FileHandler(log_file, mode="a"),
        logging.StreamHandler(sys.stdout),
    ]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
        force=True,
    )

class UniprotDownloader:
    def __init__(self, full_config):
        self.full_config = full_config
        self.cfg = full_config["uniprot_data"]
        setup_logging(os.path.abspath(self.cfg["log_file"]))

        # download & output paths
        self.url               = self.cfg["download_url"]
        self.output_path       = self.cfg["output_path"]
        self.decompressed_path = self.cfg["decompressed_path"]
        self.metadata_file     = os.path.abspath(self.cfg["dl_metadata_file"])
        self.base_diff_file    = self.cfg["diff_file"]

        # SPARQL outputs
        self.canonical_isoforms     = self.cfg["canonical_isoforms_output"]
        self.computational_isoforms = self.cfg["comp_isoforms_output"]

        # UniProt idmapping download
        self.idmap_dir    = self.cfg["idmap_dir"]
        self.idmap_file   = self.cfg["idmap_file"]
        self.idmap_url    = self.cfg["idmap_url"]
        self.idmap_output = self.cfg["idmap_output"]

        # ensure directories
        for p in [
            self.output_path,
            self.decompressed_path,
            self.metadata_file,
            self.canonical_isoforms,
            self.computational_isoforms,
            self.idmap_output,
        ]:
            d = os.path.dirname(p)
            if d:
                os.makedirs(d, exist_ok=True)

    def compute_hash(self, path):
        h = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                h.update(chunk)
        return h.hexdigest()

    def _download(self, url, dest):
        tmp = dest + ".tmp"
        logging.info(f"Downloading {url} → {tmp}")
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        with open(tmp, "wb") as f, tqdm(total=total, unit="iB", unit_scale=True) as p:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
                p.update(len(chunk))
        return tmp

    def _replace_if_changed(self, tmp, dest):
        updated = False
        if os.path.exists(dest):
            old_md5 = self.compute_hash(dest)
            new_md5 = self.compute_hash(tmp)
            if old_md5 != new_md5:
                logging.info("Change detected, replacing archive")
                shutil.copy2(dest, dest + ".backup")
                os.replace(tmp, dest)
                updated = True
            else:
                logging.info("No change; discarding temp")
                os.remove(tmp)
        else:
            logging.info("First download; saving archive")
            os.replace(tmp, dest)
            updated = True
        return updated

    def _download_idmapping(self):
        gz_path = os.path.join(self.idmap_dir, self.idmap_file)
        # Prompt user before downloading
        if input("Download UniProt ID mapping archive? [y/N]: ").strip().lower() == "y":
            dl_start = datetime.now()
            tmp = self._download(self.idmap_url, gz_path)
            updated = self._replace_if_changed(tmp, gz_path)
        else:
            logging.info("Skipping UniProt ID mapping download at user request.")
            dl_start = datetime.now()
            updated = False
        if updated:
            logging.info(f"Parsing idmapping dat → {self.idmap_output}")
            # read and name columns
            df = pd.read_csv(
                gz_path,
                sep="\t",
                header=None,
                dtype=str,
                names=["uniprot_id", "db", "external_id"],
                compression="gzip"
            )
            # pivot / collapse
            df = (
                df
                .groupby(["uniprot_id", "db"])["external_id"]
                .agg(lambda ids: "|".join(sorted(set(ids))))
                .unstack(fill_value="")
                .reset_index()
            )
            df.to_csv(self.idmap_output, index=False)
        return updated

    def _decompress(self):
        if os.path.exists(self.decompressed_path):
            bak = self.decompressed_path + ".backup"
            shutil.copy2(self.decompressed_path, bak)
            logging.info(f"Backed up old decompressed file to {bak}")
        logging.info(f"Decompressing via gzip → {self.decompressed_path}")
        subprocess.run(
            ["gzip","-cdf",self.output_path], check=True,
            stdout=open(self.decompressed_path,"wb")
        )
        logging.info("Decompression complete")

    def _make_diff(self):
        bak = self.decompressed_path + ".backup"
        if not os.path.exists(bak):
            return None, None, None
        logging.info("Generating diff (zero-context)")
        try:
            with open(bak, "r", encoding="utf-8", errors="ignore") as old_f, \
                open(self.decompressed_path, "r", encoding="utf-8", errors="ignore") as new_f:
                old_lines = old_f.readlines()
                new_lines = new_f.readlines()
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            base = os.path.splitext(self.base_diff_file)[0]
            diff_txt = f"{base}_{ts}.txt"
            diff_html = f"{base}_{ts}.html"
            # write unified diff
            with open(diff_txt, "w", encoding="utf-8") as dt:
                dt.writelines(difflib.unified_diff(old_lines, new_lines, fromfile="old", tofile="new"))
            # write HTML diff
            with open(diff_html, "w", encoding="utf-8") as dh:
                dh.write(difflib.HtmlDiff().make_file(
                    old_lines, new_lines, fromdesc="Old", todesc="New", context=True, numlines=2
                ))
            logging.info(f"Diff saved: {diff_txt} and {diff_html}")
            return diff_txt, diff_html, "".join(old_lines[:10])  
        except Exception as e:
            logging.warning(f"Diff generation failed: {e}")
            return None, None, None

    def execute_sparql_query(self, query):
        logging.info("SPARQL query starting (POST)…")
        start = datetime.now()
        resp = requests.post(
            "https://sparql.uniprot.org/sparql",
            data={"query":query},
            headers={"Accept":"application/sparql-results+json"},
            timeout=(10,300), stream=True
        )
        resp.raise_for_status()
        buf = bytearray()
        for chunk in resp.iter_content(16384): buf.extend(chunk)
        data = json.loads(buf.decode())
        dur = (datetime.now()-start).total_seconds()
        cnt = len(data.get("results",{}).get("bindings",[]))
        logging.info(f"SPARQL complete in {dur:.1f}s, {cnt} rows")
        return data

    def process_sparql_results(self, results, out_csv):
        rows = []
        for b in results.get("results", {}).get("bindings", []):
            entry = b["entry"]["value"].rsplit("/", 1)[-1]
            seq_uri = b["sequence"]["value"].rsplit("/", 1)[-1]
            seq_val = b.get("sequenceValue", {}).get("value", "")
            iscan = b.get("isCanonical", {}).get("value", "0")
            rows.append({
                "entry": entry,
                "uniprot_id": seq_uri,
                "isoform": seq_uri,
                "uniprot_sequence": seq_val,
                "isCanonical": iscan
            })
        df = pd.DataFrame(rows)
        df.to_csv(out_csv, index=False)
        logging.info(f"Saved SPARQL isoforms → {out_csv} ({len(rows)} records)")
        return df

    def _retrieve_isoforms(self):
        stats = {}

        # canonical
        sparql1 = """PREFIX taxon: <http://purl.uniprot.org/taxonomy/>
        PREFIX up: <http://purl.uniprot.org/core/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

        SELECT ?entry ?sequence ?sequenceValue ?isCanonical
        WHERE {
        GRAPH <http://sparql.uniprot.org/uniprot> {
            ?entry a up:Protein ;
                up:organism taxon:9606 ;
                up:sequence ?sequence .
            ?sequence rdf:value ?sequenceValue .

            OPTIONAL { ?sequence a up:Simple_Sequence . BIND(true AS ?likelyIsCanonical) }
            OPTIONAL { FILTER(?likelyIsCanonical) ?sequence a up:External_Sequence . BIND(true AS ?isComplicated) }

            BIND(IF(?isComplicated, STRENDS(STR(?entry), STRBEFORE(STR(?sequence), '-')), ?likelyIsCanonical) AS ?isCanonical)
        }
        }"""
        df1 = self.process_sparql_results(
            self.execute_sparql_query(sparql1),
            self.canonical_isoforms
        )
        stats["canonical_count"] = len(df1)

        # computational
        qc_mode = self.full_config.get("global", {}).get("qc_mode", True)
        if qc_mode:
            sparql2 = """PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX up:   <http://purl.uniprot.org/core/>
        PREFIX taxon: <http://purl.uniprot.org/taxonomy/>
        SELECT ?entry ?sequence ?isCanonical WHERE {
          GRAPH <http://sparql.uniprot.org/uniprot> {
            ?entry a up:Protein ; up:organism taxon:9606 ; up:potentialSequence ?sequence .
            OPTIONAL { ?sequence a up:Simple_Sequence . BIND(true AS ?likelyIsCanonical) }
            OPTIONAL { FILTER(?likelyIsCanonical) ?sequence a up:External_Sequence . BIND(true AS ?isComplicated) }
            BIND(IF(?isComplicated, STRENDS(STR(?entry), STRBEFORE(STR(?sequence), '-')), ?likelyIsCanonical) AS ?isCanonical)
          }
        }"""
            df2 = self.process_sparql_results(
                self.execute_sparql_query(sparql2),
                self.computational_isoforms
            )
            stats["computational_count"] = len(df2)
        else:
            logging.info("QC mode disabled — skipping computational isoform SPARQL")
            stats["computational_count"] = 0
        return stats

    def _write_metadata(self, meta):
        with open(self.metadata_file, "w") as mf:
            json.dump(meta, mf, indent=2)
        logging.info(f"Metadata → {self.metadata_file}")

    def run(self):
        try:
            # Prompt to optionally skip JSON download
            if input("Download UniProt JSON archive? [y/N]: ").strip().lower() == "y":
                dl_start = datetime.now()
                tmp = self._download(self.url, self.output_path)
                updated = self._replace_if_changed(tmp, self.output_path)
            else:
                logging.info("Skipping JSON download at user request.")
                dl_start = datetime.now()
                updated = False

            # always download idmapping
            idmap_updated = self._download_idmapping()

            dec_start = datetime.now()
            self._decompress()
            dec_end = datetime.now()
            diff_txt, diff_html, diff_sum = self._make_diff()
            sparql_stats = self._retrieve_isoforms()

            # build metadata
            meta = {
                "download_url": self.url,
                "download_start": dl_start.isoformat(),
                "download_end": datetime.now().isoformat(),
                "updated": updated,
                "decompress_secs": (dec_end - dec_start).total_seconds(),
                "archive_size": os.path.getsize(self.output_path),
                "decompressed_path": self.decompressed_path,
                "diff_txt": diff_txt,
                "diff_html": diff_html,
                "diff_summary": diff_sum,
                **{f"sparql_{k}": v for k, v in sparql_stats.items()},
                "idmapping": {
                    "path": self.idmap_output,
                    "updated": idmap_updated
                }
            }
            self._write_metadata(meta)

        except Exception as e:
            logging.error("Pipeline failed: %s", e, exc_info=True)
            sys.exit(1)

if __name__=='__main__':
    parser = argparse.ArgumentParser(description="Download & process UniProt data")
    parser.add_argument("--config", required=True, help="YAML config path")
    args = parser.parse_args()
    cfg = yaml.safe_load(open(args.config))
    UniprotDownloader(cfg).run()
