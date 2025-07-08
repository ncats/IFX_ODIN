#!/usr/bin/env python
"""
ensembl_uniprot_isoform_xref.py - SPARQL fetch + comparison for Ensembl isoform xrefs
"""

import os
import logging
import requests
import pandas as pd
from tqdm import tqdm
from pathlib import Path
from requests.exceptions import HTTPError
import urllib3
import yaml

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class EnsemblUniProtIsoformXref:
    def __init__(self, config):
        cfg = config["ensembl_data"]["output_paths"]
        self.merged_df_path     = cfg["final_merged"]
        self.comparison_path    = cfg["comparison"]
        self.save_path          = "src/data/publicdata/target_data/qc/ensembl_uniprot_isoform_comparison_results.qc.csv"
        self.cache_path         = self.save_path
        self.batch_size         = 50
        self.logger             = logging.getLogger("IsoformXref")

        df = pd.read_csv(self.merged_df_path)
        self.transcript_ids = df["ensembl_transcript_id_version"].dropna().unique().tolist()
        self.merged_df      = df
        self.qc_mode = config.get("qc_mode", False)

    def split_batches(self):
        for i in range(0, len(self.transcript_ids), self.batch_size):
            yield self.transcript_ids[i:i + self.batch_size]

    def build_query(self, batch):
        values = "\n".join(f"    ensembltranscript:{tid}" for tid in batch)
        return f"""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX ensembltranscript: <http://rdf.ebi.ac.uk/resource/ensembl.transcript/>
        PREFIX up: <http://purl.uniprot.org/core/>
        PREFIX database: <http://purl.uniprot.org/database/>

        SELECT ?ensemblTranscript ?isoform
        WHERE {{
        VALUES ?ensemblTranscript {{
        {values}
        }}

        GRAPH <http://sparql.uniprot.org/uniprot> {{
            {{
            # Case 1: Direct mapping from Ensembl transcript to UniProt isoform
            ?ensemblTranscript up:database database:Ensembl ;
                                rdfs:seeAlso ?isoform .
            }}
            UNION {{
            # Case 2: Entry with Ensembl database mapping to isoform
            ?entry up:database database:Ensembl ;
                    rdfs:seeAlso ?ensemblTranscript ;
                    up:sequence ?isoform .
            OPTIONAL {{
                ?ensemblTranscript rdfs:seeAlso ?isoformSpecific .
            }}
            FILTER (!BOUND(?isoformSpecific))
            }}
        }}
        }}
        """

    def run_sparql(self, query):
        url = "https://sparql.uniprot.org/sparql/"
        headers = {"Accept": "application/sparql-results+json"}
        try:
            response = requests.post(url, data={"query": query}, headers=headers, verify=False)
            response.raise_for_status()
            return response.json()
        except HTTPError as e:
            self.logger.error(f"SPARQL query failed: {e}")
            return {"results": {"bindings": []}}

    def extract_results(self, results):
        data = []
        for row in results["results"]["bindings"]:
            tx = row["ensemblTranscript"]["value"].split("/")[-1]
            iso = row["isoform"]["value"].split("/")[-1]
            data.append((tx, iso))
        return pd.DataFrame(data, columns=["ensembl_transcript_id_version", "SPARQL_uniprot_isoform"])

    def fetch_sparql_results(self):
        if Path(self.cache_path).exists() and not self.qc_mode:
            self.logger.info(f"Using cached SPARQL results: {self.cache_path}")
            return pd.read_csv(self.cache_path)
        elif Path(self.cache_path).exists():
            self.logger.info("qc_mode is enabled, re-running SPARQL despite existing cache.")
        all_results = []
        is_first_batch = True
        with tqdm(total=len(self.transcript_ids), desc="SPARQL batches") as pbar:
            for i, batch in enumerate(self.split_batches(), start=1):
                self.logger.info(f"Submitting batch {i} ({len(batch)} IDs)")
                query = self.build_query(batch)
                results = self.run_sparql(query)
                df = self.extract_results(results)

                # Append each batch directly to file
                mode = "w" if is_first_batch else "a"
                header = is_first_batch
                df.to_csv(self.save_path, mode=mode, index=False, header=header)
                is_first_batch = False

                all_results.append(df)
                pbar.update(len(batch))

        final_df = pd.concat(all_results, ignore_index=True)

        # Enforce strict 1:1 mapping
        dupes = final_df["ensembl_transcript_id_version"].duplicated(keep=False)
        if dupes.any():
            self.logger.warning(f"{dupes.sum()} 1:many transcript→isoform mappings found; dropping ambiguous rows")
            final_df = final_df[~dupes]

            # Overwrite filtered results (removes 1:manys)
            final_df.to_csv(self.save_path, index=False)

        self.logger.info(f"SPARQL xref written to: {self.save_path}")
        return final_df

    def compare_isoform_columns(self, sparql_df):
        df = self.merged_df.merge(sparql_df, on="ensembl_transcript_id_version", how="left", suffixes=("", "_sparql"))

        subset = df[df[["ensembl_uniprot_isoform", "SPARQL_uniprot_isoform"]].notnull().any(axis=1)].copy()

        def match(row):
            a, b = row["ensembl_uniprot_isoform"], row["SPARQL_uniprot_isoform"]
            if pd.isna(a) and pd.isna(b): return None
            if a == b: return "mapped"
            if pd.notna(a) and pd.isna(b): return "ensembl"
            if pd.notna(b) and pd.isna(a): return "uniprot"
            return "error"

        subset["uniprot_isoform_status"] = subset.apply(match, axis=1)

        keep_cols = [
            "ensembl_transcript_id_version", "ensembl_symbol", "ensembl_canonical",
            "ensembl_uniprot_id", "ensembl_trembl_id", "ensembl_uniprot_isoform",
            "SPARQL_uniprot_isoform", "uniprot_isoform_status"
        ]
        final = subset[keep_cols]
        final.to_csv(self.comparison_path, index=False)
        self.logger.info(f"Isoform comparison written to: {self.comparison_path}")
        counts = subset["uniprot_isoform_status"].value_counts()
        self.logger.info(f"Isoform comparison summary:")
        for status in ["mapped", "ensembl", "uniprot", "error"]:
            count = counts.get(status, 0)
            self.logger.info(f"  {status}: {count}")
        return final

    def update_ensembl_output(self, comparison_df):
        self.logger.info("Updating Ensembl final output with SPARQL isoforms…")

        # Filter to rows where UniProt has a replacement for missing Ensembl isoform
        fill_df = comparison_df[
            (comparison_df["uniprot_isoform_status"] == "uniprot") &
            (comparison_df["SPARQL_uniprot_isoform"].notna())
        ][["ensembl_transcript_id_version", "SPARQL_uniprot_isoform"]]

        # Drop duplicates to enforce 1:1 mapping
        fill_df = fill_df.drop_duplicates(subset="ensembl_transcript_id_version").set_index("ensembl_transcript_id_version")

        updated_df = self.merged_df.copy()
        updated_df.set_index("ensembl_transcript_id_version", inplace=True)

        mask = updated_df.index.isin(fill_df.index) & updated_df["ensembl_uniprot_isoform"].isna()
        updated_df.loc[mask, "ensembl_uniprot_isoform"] = updated_df.loc[mask].index.map(fill_df["SPARQL_uniprot_isoform"])

        updated_df.reset_index(inplace=True)
        updated_df.to_csv(self.merged_df_path, index=False)
        self.logger.info(f"Final Ensembl data updated and written to: {self.merged_df_path}")

    def run(self):
        self.logger.info("Running Ensembl↔UniProt isoform SPARQL pipeline")
        if self.qc_mode:
            rerun = input("Re-run SPARQL queries? [y/N]: ").strip().lower() == "y"
        else:
            rerun = False

        # Always load or fetch SPARQL results
        sparql_df = self.fetch_sparql_results() if rerun else pd.read_csv(self.save_path)
        comparison_df = self.compare_isoform_columns(sparql_df)

        # ✅ Drop duplicates before final QC export
        comparison_df.drop_duplicates(inplace=True)

        comparison_df.to_csv(self.save_path, index=False)
        self.update_ensembl_output(comparison_df)
        self.logger.info(f"Final QC export written to: {self.save_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Ensembl-UniProt isoform SPARQL pipeline")
    parser.add_argument("--config", type=str, default="config/targets/targets_config.yaml")
    args = parser.parse_args()
    with open(args.config) as f:
        cfg = yaml.safe_load(f)
    EnsemblUniProtIsoformXref(cfg).run()
