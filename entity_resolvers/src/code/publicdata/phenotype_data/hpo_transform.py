import os
import json
import yaml
import logging
import argparse
import pandas as pd
from datetime import datetime

class HPOPhenotypeTransformer:
    def __init__(self, config):
        self.cfg = config["phenotype"]["hpo"]
        self.qc_mode = config.get("global", {}).get("qc_mode", False)

        self.paths = {
            "hpoa": self.cfg["raw_phenotype_hpoa"],
            "genes_to_phenotype": self.cfg["raw_genes_to_phenotype"],
            "phenotype_to_genes": self.cfg["raw_phenotype_to_genes"],
            "obo": self.cfg["raw_obo"]
        }

        self.outputs = {
            "genes_to_phenotype": self.cfg["cleaned_genes_to_phenotype"],
            "phenotype_to_genes": self.cfg["cleaned_phenotype_to_genes"],
            "phenotype_disease": self.cfg["cleaned_phenotype_disease"],
            "resolved_hpoa": self.cfg["resolved_hpoa"],
            "hpo_ids": self.cfg["resolved_hpo_ids"]
        }

        self.meta_path = self.cfg["transform_metadata_file"]
        self.metadata = {
            "timestamp": {"start": str(datetime.now())},
            "steps": [],
            "outputs": [],
            "records": {},
        }

        os.makedirs(os.path.dirname(self.outputs["genes_to_phenotype"]), exist_ok=True)
        os.makedirs(os.path.dirname(self.meta_path), exist_ok=True)

        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    def run(self):
        self.transform_genes_to_phenotype()
        self.transform_phenotype_to_genes()
        self.transform_phenotype_disease()
        self.extract_all_phenotype_terms()

        self.metadata["timestamp"]["end"] = str(datetime.now())
        with open(self.meta_path, "w") as f:
            json.dump(self.metadata, f, indent=2)
        logging.info(f"📟 Metadata written to: {self.meta_path}")

    def transform_genes_to_phenotype(self):
        path = self.paths["genes_to_phenotype"]
        df = pd.read_csv(path, sep="\t", dtype=str, comment="#")
        df = df.rename(columns={
            "ncbi_gene_id": "gene_id",
            "gene_symbol": "gene_symbol",
            "hpo_id": "hpo_id",
            "hpo_name": "hpo_label",
            "frequency": "frequency",
            "disease_id": "disease_id"
        })

        df["hpo_id"] = df["hpo_id"].str.replace(r"\\..*", "", regex=True)
        df["gene_id"] = df["gene_id"].str.replace(r"\\..*", "", regex=True)

        df.to_csv(self.outputs["genes_to_phenotype"], index=False)
        self.metadata["steps"].append("Transformed genes_to_phenotype")
        self.metadata["outputs"].append(self.outputs["genes_to_phenotype"])
        self.metadata["records"]["genes_to_phenotype"] = len(df)
        logging.info(f"✅ genes_to_phenotype: {len(df)} records")

    def transform_phenotype_to_genes(self):
        path = self.paths["phenotype_to_genes"]
        df = pd.read_csv(path, sep="\t", dtype=str, comment="#")
        df = df.rename(columns={
            "hpo_id": "hpo_id",
            "hpo_name": "hpo_label",
            "ncbi_gene_id": "gene_id",
            "gene_symbol": "gene_symbol",
            "disease_id": "disease_id"
        })

        df["hpo_id"] = df["hpo_id"].str.replace(r"\\..*", "", regex=True)
        df["gene_id"] = df["gene_id"].str.replace(r"\\..*", "", regex=True)

        df.to_csv(self.outputs["phenotype_to_genes"], index=False)
        self.metadata["steps"].append("Transformed phenotype_to_genes")
        self.metadata["outputs"].append(self.outputs["phenotype_to_genes"])
        self.metadata["records"]["phenotype_to_genes"] = len(df)
        logging.info(f"✅ phenotype_to_genes: {len(df)} records")

    def transform_phenotype_disease(self):
        path = self.paths["hpoa"]
        rows = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("#"):
                    continue
                parts = line.strip().split("\t")
                if len(parts) < 10:
                    continue
                disease_id = parts[0]
                hpo_id = parts[1].split(".")[0]
                qualifier = parts[2]
                hpo_label = parts[3]
                onset = parts[4] if parts[4] else None
                frequency = parts[5] if parts[5] else None
                evidence = parts[8] if parts[8] else None

                if qualifier and qualifier.lower() == "not":
                    continue  # skip negated associations

                rows.append({
                    "disease_id": disease_id,
                    "hpo_id": hpo_id,
                    "hpo_label": hpo_label,
                    "onset": onset,
                    "frequency": frequency,
                    "evidence": evidence
                })

        df = pd.DataFrame(rows)
        df.to_csv(self.outputs["phenotype_disease"], index=False)
        self.metadata["steps"].append("Transformed phenotype_disease from phenotype.hpoa")
        self.metadata["outputs"].append(self.outputs["phenotype_disease"])
        self.metadata["records"]["phenotype_disease"] = len(df)
        logging.info(f"✅ phenotype_disease: {len(df)} records")

        # Save HPO label-to-ID mapping from this file (hpoa)
        hpo_ids = df[["hpo_id", "hpo_label"]].drop_duplicates()
        hpo_ids.to_csv(self.outputs["resolved_hpoa"], sep="\t", index=False)
        self.metadata["steps"].append("Saved HPOA-derived HPO IDs")
        self.metadata["outputs"].append(self.outputs["resolved_hpoa"])
        self.metadata["records"]["hpoa_ids"] = len(hpo_ids)
        logging.info(f"✅ hpoa.tsv: {len(hpo_ids)} unique HPO terms from phenotype.hpoa")

    def extract_all_phenotype_terms(self):
        path = self.paths["obo"]
        hpo_terms = []
        with open(path, "r", encoding="utf-8") as f:
            current_id, current_name = None, None
            for line in f:
                if line.strip() == "[Term]":
                    if current_id and current_name:
                        hpo_terms.append({"hpo_id": current_id, "hpo_label": current_name})
                    current_id, current_name = None, None
                elif line.startswith("id: HP:"):
                    current_id = line.strip().split("id: ")[1]
                elif line.startswith("name: "):
                    current_name = line.strip().split("name: ")[1]
            if current_id and current_name:
                hpo_terms.append({"hpo_id": current_id, "hpo_label": current_name})

        df = pd.DataFrame(hpo_terms).drop_duplicates()
        df.to_csv(self.outputs["hpo_ids"], sep="\t", index=False)
        self.metadata["steps"].append("Parsed all HPO terms from obo")
        self.metadata["outputs"].append(self.outputs["hpo_ids"])
        self.metadata["records"]["hpo_ids"] = len(df)
        logging.info(f"✅ hpo_ids.tsv: {len(df)} total ontology terms")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HPO phenotype transformer for TargetGraph/ODIN pipeline")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    HPOPhenotypeTransformer(config).run()
