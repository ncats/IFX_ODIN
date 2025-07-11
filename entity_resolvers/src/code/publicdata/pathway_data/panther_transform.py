#!/usr/bin/env python
import os
import pandas as pd
import logging
import yaml
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class PantherTransformer:
    def __init__(self, config):
        self.cfg = config["pathways"]["panther"]
        self.metadata = {
            "timestamp": {"start": str(datetime.now())},
            "steps": [],
            "outputs": []
        }

    def create_dataframe(self, raw_path):
        with open(raw_path, 'r') as f:
            lines = f.read().splitlines()
        data = [line.split('\t') for line in lines if line]
        df = pd.DataFrame(data[1:], columns=data[0])
        return df

    def transform(self, key, entry):
        df = self.create_dataframe(entry["raw_path"])
        if key == "sequence_classifications":
            df.columns = ["species", "uniprot_id", "gene_symbol", "panther_family_id", "panther_family_name", "protein", "molecular_function", "biological_process", "compartment", "protein", "unknown"]
            species_split = df["species"].str.split("|", expand=True)
            df["species"] = species_split[0]
            df["hgnc_id"] = species_split[1].str.replace("HGNC=", "")
            df["uniprot_id"] = species_split[2].str.replace("UniProtKB=", "")
        elif key == "sequence_association_pathway":
            # Manually define columns since file has no header
            df.columns = [
                "pathway_accession", "pathway_name", "unknown_1", "gene_name", "species",
                "protein", "GO_evidence_code", "pubmed_id", "pubmed_source",
                "panther_family_id", "panther_family_name"
            ]

            # Only keep HUMAN entries
            df = df[df["species"].str.startswith("HUMAN|HGNC")].copy()

            # Split and extract species, HGNC, UniProt
            species_split = df["species"].str.split("|", expand=True)
            df["species"] = species_split[0]
            df["hgnc_id"] = species_split[1].str.replace("HGNC=", "")
            df["uniprot_id"] = species_split[2].str.replace("UniProtKB=", "")

            # Optionally collapse pubmed_ids per pathway
            df["pubmed_id"] = df.groupby("pathway_accession")["pubmed_id"].transform(lambda x: ", ".join(x.unique()))

            # Drop duplicates at the pathway level (optional if 1 row per pathway desired)
            df = df.drop_duplicates(subset="pathway_accession")

            # Select relevant output columns
            df = df[["pathway_accession", "pathway_name", "uniprot_id", "hgnc_id", "panther_family_id", "panther_family_name", "pubmed_id"]]

        elif key == "hmm_classifications":
            df.columns = ["panther_family_id", "panther_family_name", "activity", "process", "compartment", "protein", "unknown"]

        os.makedirs(os.path.dirname(entry["csv_path"]), exist_ok=True)
        df.to_csv(entry["csv_path"], index=False)
        logging.info(f"✅ Saved cleaned {key} to {entry['csv_path']}")

        self.metadata["steps"].append({
            "step": f"transform_{key}",
            "input": entry["raw_path"],
            "output": entry["csv_path"],
            "records": len(df),
            "timestamp": str(datetime.now())
        })

        self.metadata["outputs"].append({
            "name": os.path.basename(entry["csv_path"]),
            "path": entry["csv_path"],
        })

    def run(self):
        for key, entry in self.cfg["files"].items():
            self.transform(key, entry)

        self.metadata["timestamp"]["end"] = str(datetime.now())
        with open(self.cfg["transform_metadata_file"], "w") as f:
            json.dump(self.metadata, f, indent=2)
        logging.info(f"📝 Transform metadata saved to {self.cfg['transform_metadata_file']}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    PantherTransformer(config).run()
