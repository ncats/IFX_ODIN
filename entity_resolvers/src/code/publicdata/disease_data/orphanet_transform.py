# orphanet_transform.py - Parses Orphanet OWL and XML files, transforms to CSV

import os
import rdflib
import pandas as pd
import xml.etree.ElementTree as ET
import yaml
import json
import logging
from datetime import datetime
from pathlib import Path

def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, mode="a"),
            logging.StreamHandler()
        ],
        force=True,
    )

class OrphanetTransformer:
    def __init__(self, full_config):
        self.cfg = full_config["orphanet"]
        setup_logging(self.cfg["log_file"])
        self.gene_ids_output = Path(self.cfg["gene_ids_output"])
        self.disease_ids_output = Path(self.cfg["disease_ids_output"])
        self.owl_file = Path(self.cfg["owl_file"])
        self.xml_file = Path(self.cfg["xml_file"])
        self.xml_output = Path(self.cfg["xml_output"])
        self.metadata_file = Path(self.cfg["transform_metadata"])
        os.makedirs(self.disease_ids_output.parent, exist_ok=True)
        os.makedirs(self.gene_ids_output.parent, exist_ok=True)
        os.makedirs(self.metadata_file.parent, exist_ok=True)

    def parse_owl(self):
        logging.info(f"📖 Parsing OWL file: {self.owl_file}")
        g = rdflib.Graph()
        g.parse(self.owl_file, format="xml")

        ns = {
            "rdfs": rdflib.RDFS,
            "rdf": rdflib.RDF,
            "oboInOwl": rdflib.Namespace("http://www.geneontology.org/formats/oboInOwl#"),
            "efo": rdflib.Namespace("http://www.ebi.ac.uk/efo/"),
            "owl": rdflib.OWL,
        }

        records = []
        sources = set()
        for s in g.subjects(rdflib.RDF.type, ns["owl"].Class):
            s_str = str(s)
            if not s_str.startswith("http://www.orpha.net/ORDO/Orphanet_"):
                continue
            orpha_id = s_str.split("_")[-1]
            name = g.value(s, ns["rdfs"].label, default="Unknown")
            definition = g.value(s, ns["efo"].definition, default="")
            xrefs = [str(x) for x in g.objects(s, ns["oboInOwl"].hasDbXref)]
            for x in xrefs:
                if ":" in x:
                    sources.add(x.split(":")[0])
            records.append((orpha_id, name, definition, "|".join(xrefs)))

        df = pd.DataFrame(records, columns=["Orphanet_ID", "Disease_Name", "Definition", "Mappings"])

        detected_sources = sorted(sources)
        def extract_mapping(mapping_str, source):
            return "|".join([
                m.split(":", 1)[1] for m in mapping_str.split("|")
                if m.startswith(source + ":")
            ]) or None

        for source in detected_sources:
            df[source] = df["Mappings"].apply(lambda x: extract_mapping(x, source))

        df = df[~df["Disease_Name"].str.startswith("OBSOLETE:")].copy()
        df.drop(columns=["Mappings"], inplace=True)
        df["Orphanet_ID"] = "Orphanet:" + df["Orphanet_ID"].astype(str)

        prefix_dict = {
            "ICD-10": "ICD10CM:",
            "ICD-11": "ICD11:",
            "MeSH": "MESH:",
            "MedDRA": "MEDDRA:",
            "OMIM": "OMIM:"
        }

        for col, prefix in prefix_dict.items():
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: "|".join([f"{prefix}{v}" for v in x.split("|")]) if pd.notna(x) and str(x).strip() != "" else None
                )

        df.rename(columns={col: f"orphanet_{col}" for col in df.columns}, inplace=True)
        # Split into gene-related vs disease-only rows
        gene_cols = ["orphanet_Ensembl", "orphanet_Genatlas", "orphanet_HGNC"]
        gene_mask = df[gene_cols].notna().any(axis=1)

        gene_df = df[gene_mask].copy()
        disease_df = df[~gene_mask].copy()

        # Drop unwanted gene-related columns from disease_df
        drop_cols = [
            "orphanet_ClinVar", "orphanet_Ensembl", "orphanet_Genatlas", "orphanet_HGNC",
            "orphanet_IUPHAR", "orphanet_Reactome", "orphanet_SwissProt"
        ]
        disease_df.drop(columns=[col for col in drop_cols if col in disease_df.columns], inplace=True)

        # Save both
        logging.info(f"📂 Saving orphanet disease IDs to {self.disease_ids_output}")
        disease_df.to_csv(self.disease_ids_output, index=False)

        logging.info(f"📂 Saving orphanet gene-related rows to {self.gene_ids_output}")
        gene_df.to_csv(self.gene_ids_output, index=False)

        return df

    def parse_xml(self):
        logging.info(f"📖 Parsing XML file: {self.xml_file}")
        root = ET.parse(self.xml_file).getroot()
        records = []
        disorder_list = root.find("DisorderList")
        if disorder_list is None:
            return pd.DataFrame()

        for disorder in disorder_list.findall("Disorder"):
            did = disorder.findtext("OrphaCode")
            name = disorder.findtext("Name")
            assoc_list = disorder.find("DisorderGeneAssociationList")
            if assoc_list is None:
                continue
            for assoc in assoc_list.findall("DisorderGeneAssociation"):
                gene = assoc.find("Gene")
                if gene is None:
                    continue
                ref_map = {ref.findtext("Source"): ref.findtext("Reference")
                           for ref in gene.find("ExternalReferenceList") or []}
                records.append({
                    "Disorder_ID": f"Orphanet:{did}",
                    "Disorder_Name": name,
                    "Gene_Symbol": gene.findtext("Symbol"),
                    "Gene_Name": gene.findtext("Name"),
                    "Gene_Ensembl": ref_map.get("Ensembl"),
                    "Gene_HGNC": f"HGNC:{ref_map['HGNC']}" if "HGNC" in ref_map else None,
                    "Gene_OMIM": "|".join([f"OMIM:{v}" for v in ref_map["OMIM"].split("|")]) if "OMIM" in ref_map else None,
                    "Association_Type": assoc.findtext("DisorderGeneAssociationType/Name"),
                    "SourceOfValidation": assoc.findtext("SourceOfValidation")
                })

        df = pd.DataFrame(records)
        df.rename(columns={col: f"orphanet_{col}" for col in df.columns}, inplace=True)
        logging.info(f"📂 Saving gene associations to {self.xml_output}")
        df.to_csv(self.xml_output, index=False)
        return df

    def run(self):
        owl_df = self.parse_owl()
        xml_df = self.parse_xml()

        meta = {
            "timestamp": datetime.now().isoformat(),
            "owl_records": len(owl_df),
            "xml_records": len(xml_df),
            "disease_ids_output": str(self.disease_ids_output),
            "gene_ids_output": str(self.gene_ids_output),
            "xml_output": str(self.xml_output)
        }

        with open(self.metadata_file, "w") as f:
            json.dump(meta, f, indent=2)
        logging.info(f"🗜 Metadata saved → {self.metadata_file}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Transform Orphanet OWL + XML")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    args = parser.parse_args()
    cfg = yaml.safe_load(open(args.config))
    OrphanetTransformer(cfg).run()
