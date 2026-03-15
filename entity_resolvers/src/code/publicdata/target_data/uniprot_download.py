#!/usr/bin/env python
"""
uniprot_download.py - Download UniProt data with update detection,
SPARQL isoform queries, memory-efficient decompression, and idmapping download.

NO raw-file diffs — version tracking happens on cleaned output in the transformer.
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
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s",
                        handlers=handlers, force=True)


class UniprotDownloader:
    def __init__(self, full_config):
        self.full_config = full_config
        self.cfg = full_config["uniprot_data"]
        setup_logging(os.path.abspath(
            self.cfg.get("download_log_file") or self.cfg.get("log_file", "uniprot_download.log")
        ))

        self.url = self.cfg["download_url"]
        self.output_path = self.cfg["output_path"]
        self.decompressed_path = self.cfg["decompressed_path"]
        self.metadata_file = os.path.abspath(self.cfg["dl_metadata_file"])

        # SPARQL outputs
        self.canonical_isoforms = self.cfg["canonical_isoforms_output"]
        self.computational_isoforms = self.cfg["comp_isoforms_output"]

        # UniProt idmapping
        self.idmap_dir = self.cfg["idmap_dir"]
        self.idmap_file = self.cfg["idmap_file"]
        self.idmap_url = self.cfg["idmap_url"]
        self.idmap_output = self.cfg["idmap_output"]

        # Ensure directories
        for p in [self.output_path, self.decompressed_path, self.metadata_file,
                  self.canonical_isoforms, self.computational_isoforms, self.idmap_output]:
            d = os.path.dirname(p)
            if d:
                os.makedirs(d, exist_ok=True)

        # Load previous metadata to compare versions
        self.old_meta = {}
        if os.path.exists(self.metadata_file):
            try:
                with open(self.metadata_file) as f:
                    self.old_meta = json.load(f)
            except Exception:
                pass

    def compute_hash(self, path):
        h = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                h.update(chunk)
        return h.hexdigest()

    def _download(self, url, dest):
        tmp = dest + ".tmp"
        logging.info(f"Downloading {url}")
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        with open(tmp, "wb") as f, tqdm(total=total, unit="iB", unit_scale=True) as p:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
                p.update(len(chunk))
        return tmp

    def detect_uniprot_release(self, timeout=15):
        import re
        probe_urls = [
            "https://rest.uniprot.org/uniprotkb/search?query=reviewed:true&size=1&fields=accession",
            "https://rest.uniprot.org/uniprotkb/stream?compressed=false&format=fasta&query=reviewed:true&size=1",
        ]
        for url in probe_urls:
            try:
                resp = requests.get(url, timeout=timeout, stream=True)
                resp.raise_for_status()
                h = {k.lower(): v for k, v in resp.headers.items()}
                rel = next((h[k] for k in ["x-release-number", "x-uniprot-release-number"] if k in h), None)
                rdate = next((h[k] for k in ["x-uniprot-release-date", "x-release-date"] if k in h), None)
                if rel:
                    m = re.search(r"\b(\d{4}_\d{2})\b", rel)
                    rel = m.group(1) if m else rel.strip()
                if rdate:
                    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%a, %d %b %Y %H:%M:%S %Z"):
                        try:
                            rdate = datetime.strptime(rdate, fmt).strftime("%Y-%m-%d")
                            break
                        except Exception:
                            pass
                if rel or rdate:
                    return (rel or "unknown", rdate or "unknown")
            except Exception:
                continue
        return ("unknown", "unknown")

    def _replace_if_changed(self, tmp, dest):
        if os.path.exists(dest):
            if self.compute_hash(dest) == self.compute_hash(tmp):
                logging.info("No change; discarding temp")
                os.remove(tmp)
                return False
            else:
                logging.info("Change detected, replacing archive")
                os.replace(tmp, dest)
                return True
        else:
            os.replace(tmp, dest)
            return True

    def _download_idmapping(self):
        gz_path = os.path.join(self.idmap_dir, self.idmap_file)
        if input("Download UniProt ID mapping archive? [y/N]: ").strip().lower() != "y":
            logging.info("Skipping UniProt ID mapping download at user request.")
            return False

        tmp = self._download(self.idmap_url, gz_path)
        updated = self._replace_if_changed(tmp, gz_path)
        if updated:
            logging.info(f"Parsing idmapping dat → {self.idmap_output}")
            df = pd.read_csv(gz_path, sep="\t", header=None, dtype=str,
                             names=["uniprot_id", "db", "external_id"], compression="gzip")
            df = (df.groupby(["uniprot_id", "db"])["external_id"]
                  .agg(lambda ids: "|".join(sorted(set(ids))))
                  .unstack(fill_value="").reset_index())
            df.to_csv(self.idmap_output, index=False)
        return updated

    def _decompress(self):
        logging.info(f"Decompressing via gzip → {self.decompressed_path}")
        with open(self.decompressed_path, "wb") as out:
            subprocess.run(["gzip", "-cdf", self.output_path], check=True, stdout=out)
        logging.info("Decompression complete")

    def execute_sparql_query(self, query):
        logging.info("SPARQL query starting…")
        start = datetime.now()
        resp = requests.post(
            "https://sparql.uniprot.org/sparql",
            data={"query": query},
            headers={"Accept": "application/sparql-results+json"},
            timeout=(10, 300), stream=True,
        )
        resp.raise_for_status()
        buf = bytearray()
        for chunk in resp.iter_content(16384):
            buf.extend(chunk)
        data = json.loads(buf.decode())
        cnt = len(data.get("results", {}).get("bindings", []))
        logging.info(f"SPARQL complete in {(datetime.now()-start).total_seconds():.1f}s, {cnt} rows")
        return data

    def process_sparql_results(self, results, out_csv):
        rows = []
        for b in results.get("results", {}).get("bindings", []):
            entry = b["entry"]["value"].rsplit("/", 1)[-1]
            seq_uri = b["sequence"]["value"].rsplit("/", 1)[-1]
            rows.append({
                "entry": entry, "uniprot_id": seq_uri, "isoform": seq_uri,
                "uniprot_sequence": b.get("sequenceValue", {}).get("value", ""),
                "isCanonical": b.get("isCanonical", {}).get("value", "0"),
            })
        df = pd.DataFrame(rows)
        os.makedirs(os.path.dirname(out_csv), exist_ok=True)
        df.to_csv(out_csv, index=False)
        logging.info(f"Saved SPARQL isoforms → {out_csv} ({len(rows)} records)")
        return df

    def _retrieve_isoforms(self):
        stats = {}
        sparql1 = """PREFIX taxon: <http://purl.uniprot.org/taxonomy/>
        PREFIX up: <http://purl.uniprot.org/core/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT ?entry ?sequence ?sequenceValue ?isCanonical
        WHERE {
        GRAPH <http://sparql.uniprot.org/uniprot> {
            ?entry a up:Protein ; up:organism taxon:9606 ; up:sequence ?sequence .
            ?sequence rdf:value ?sequenceValue .
            OPTIONAL { ?sequence a up:Simple_Sequence . BIND(true AS ?likelyIsCanonical) }
            OPTIONAL { FILTER(?likelyIsCanonical) ?sequence a up:External_Sequence . BIND(true AS ?isComplicated) }
            BIND(IF(?isComplicated, STRENDS(STR(?entry), STRBEFORE(STR(?sequence), '-')), ?likelyIsCanonical) AS ?isCanonical)
        }
        }"""
        df1 = self.process_sparql_results(self.execute_sparql_query(sparql1), self.canonical_isoforms)
        stats["canonical_count"] = len(df1)

        qc_mode = self.full_config.get("global", {}).get("qc_mode", True)
        if qc_mode:
            sparql2 = """PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX up: <http://purl.uniprot.org/core/>
            PREFIX taxon: <http://purl.uniprot.org/taxonomy/>
            SELECT ?entry ?sequence ?isCanonical WHERE {
              GRAPH <http://sparql.uniprot.org/uniprot> {
                ?entry a up:Protein ; up:organism taxon:9606 ; up:potentialSequence ?sequence .
                OPTIONAL { ?sequence a up:Simple_Sequence . BIND(true AS ?likelyIsCanonical) }
                OPTIONAL { FILTER(?likelyIsCanonical) ?sequence a up:External_Sequence . BIND(true AS ?isComplicated) }
                BIND(IF(?isComplicated, STRENDS(STR(?entry), STRBEFORE(STR(?sequence), '-')), ?likelyIsCanonical) AS ?isCanonical)
              }
            }"""
            df2 = self.process_sparql_results(self.execute_sparql_query(sparql2), self.computational_isoforms)
            stats["computational_count"] = len(df2)
        else:
            stats["computational_count"] = 0
        return stats

    def run(self):
        try:
            # Version-based skip
            release_num, release_date = self.detect_uniprot_release()
            previous_version = self.old_meta.get("source_version") or self.old_meta.get("version")
            if (release_num and release_num != "unknown"
                    and previous_version == release_num
                    and os.path.exists(self.decompressed_path)):
                logging.info(
                    f"UniProt release unchanged ({release_num}) and decompressed file present — skipping download."
                )
                meta = {
                    "source_name": "UniProt",
                    "source_version": release_num,
                    "release_date": release_date,
                    "url": self.url,
                    "download_start": datetime.now().isoformat(),
                    "download_end": datetime.now().isoformat(),
                    "updated": False,
                    "status": "no_change",
                }
                os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
                with open(self.metadata_file, "w") as mf:
                    json.dump(meta, mf, indent=2)
                return

            if input("Download UniProt JSON archive? [y/N]: ").strip().lower() == "y":
                dl_start = datetime.now()
                tmp = self._download(self.url, self.output_path)
                updated = self._replace_if_changed(tmp, self.output_path)
            else:
                logging.info("Skipping JSON download at user request.")
                dl_start = datetime.now()
                updated = False

            idmap_updated = self._download_idmapping()

            if updated:
                self._decompress()

            sparql_stats = self._retrieve_isoforms()
            release_num, release_date = self.detect_uniprot_release()

            meta = {
                "source_name": "UniProt",
                "source_version": release_num,
                "release_date": release_date,
                "url": self.url,
                "download_start": dl_start.isoformat(),
                "download_end": datetime.now().isoformat(),
                "updated": updated or idmap_updated,
                "status": "updated" if (updated or idmap_updated) else "no_change",
                **{f"sparql_{k}": v for k, v in sparql_stats.items()},
            }
            os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)
            with open(self.metadata_file, "w") as mf:
                json.dump(meta, mf, indent=2)
            logging.info(f"Metadata → {self.metadata_file}")

        except Exception as e:
            logging.error("Pipeline failed: %s", e, exc_info=True)
            sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download UniProt data")
    parser.add_argument("--config", type=str, default="config/targets_config.yaml")
    args = parser.parse_args()
    cfg = yaml.safe_load(open(args.config))
    UniprotDownloader(cfg).run()