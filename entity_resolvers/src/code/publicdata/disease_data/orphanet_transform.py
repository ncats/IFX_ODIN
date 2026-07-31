# orphanet_transform.py - Parses Orphanet OWL and XML files, transforms to CSV with QC diffs

import os
import rdflib
import pandas as pd
import xml.etree.ElementTree as ET
import yaml
import json
import logging
from datetime import datetime
from pathlib import Path

try:
    from publicdata.disease_data.transform_diff_utils import compute_dataframe_diff, write_diff_json
except ImportError:
    from transform_diff_utils import compute_dataframe_diff, write_diff_json


def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file, mode="a"), logging.StreamHandler()],
        force=True,
    )


def normalize_mondo_id(mondo_str):
    if pd.isna(mondo_str) or str(mondo_str).strip() == "":
        return None

    ids = []
    for mondo_id in str(mondo_str).split("|"):
        mondo_id = mondo_id.strip()
        if not mondo_id:
            continue

        if mondo_id.startswith("MONDO:"):
            mondo_id = mondo_id.replace("MONDO:", "")

        if mondo_id.isdigit():
            mondo_id = mondo_id.zfill(7)

        ids.append(f"MONDO:{mondo_id}")

    ids = sorted(set(ids))
    return "|".join(ids) if ids else None


def prefix_pipe_values(val, prefix):
    if pd.isna(val) or str(val).strip() == "":
        return None
    vals = [f"{prefix}{v.strip()}" for v in str(val).split("|") if v.strip()]
    vals = sorted(set(vals))
    return "|".join(vals) if vals else None


def clean_obsolete_label(name):
    if pd.isna(name):
        return name
    name = str(name).strip()
    if name.startswith("OBSOLETE:"):
        return name.replace("OBSOLETE:", "", 1).strip()
    return name


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

        self.orphanet_obsolete_output = Path(
            self.cfg.get(
                "orphanet_obsolete_output",
                str(self.disease_ids_output.parent / "orphanet_obsolete_ids.csv")
            )
        )

        os.makedirs(self.disease_ids_output.parent, exist_ok=True)
        os.makedirs(self.gene_ids_output.parent, exist_ok=True)
        os.makedirs(self.xml_output.parent, exist_ok=True)
        os.makedirs(self.metadata_file.parent, exist_ok=True)
        os.makedirs(self.orphanet_obsolete_output.parent, exist_ok=True)

    def parse_owl(self):
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

            records.append((orpha_id, str(name), str(definition), "|".join(xrefs)))

        df = pd.DataFrame(records, columns=["Orphanet_ID", "Disease_Name", "Definition", "Mappings"])

        if df.empty:
            empty_df = pd.DataFrame()
            empty_df.to_csv(self.disease_ids_output, index=False)
            empty_df.to_csv(self.gene_ids_output, index=False)
            empty_df.to_csv(self.orphanet_obsolete_output, index=False)
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        detected_sources = sorted(sources)

        def extract_mapping(mapping_str, source):
            return "|".join([
                m.split(":", 1)[1]
                for m in str(mapping_str).split("|")
                if m.startswith(source + ":")
            ]) or None

        for source in detected_sources:
            df[source] = df["Mappings"].apply(lambda x: extract_mapping(x, source))

        df["Orphanet_ID"] = "Orphanet:" + df["Orphanet_ID"].astype(str)

        prefix_dict = {
            "ICD-10": "ICD10CM:",
            "ICD-11": "ICD11:",
            "MeSH": "MESH:",
            "MedDRA": "MEDDRA:",
            "OMIM": "OMIM:",
            "MONDO": "MONDO:"
        }

        for col, prefix in prefix_dict.items():
            if col in df.columns:
                if col == "MONDO":
                    df[col] = df[col].apply(normalize_mondo_id)
                else:
                    df[col] = df[col].apply(lambda x: prefix_pipe_values(x, prefix))

        df["is_obsolete"] = df["Disease_Name"].astype(str).str.startswith("OBSOLETE:", na=False)

        obsolete_df = df[df["is_obsolete"]].copy()
        if not obsolete_df.empty:
            obsolete_df["Disease_Name"] = obsolete_df["Disease_Name"].apply(clean_obsolete_label)

        active_df = df[~df["is_obsolete"]].copy()

        if "Mappings" in obsolete_df.columns:
            obsolete_df.drop(columns=["Mappings"], inplace=True)
        if "Mappings" in active_df.columns:
            active_df.drop(columns=["Mappings"], inplace=True)

        obsolete_df.rename(columns={col: f"orphanet_{col}" for col in obsolete_df.columns}, inplace=True)
        active_df.rename(columns={col: f"orphanet_{col}" for col in active_df.columns}, inplace=True)

        gene_cols = ["orphanet_Ensembl", "orphanet_Genatlas", "orphanet_HGNC"]
        for gc in gene_cols:
            if gc not in active_df.columns:
                active_df[gc] = pd.NA

        gene_mask = active_df[gene_cols].notna().any(axis=1)

        gene_df = active_df[gene_mask].copy()
        disease_df = active_df[~gene_mask].copy()

        drop_cols = [
            "orphanet_ClinVar",
            "orphanet_Ensembl",
            "orphanet_Genatlas",
            "orphanet_HGNC",
            "orphanet_IUPHAR",
            "orphanet_Reactome",
            "orphanet_SwissProt"
        ]
        disease_df.drop(columns=[col for col in drop_cols if col in disease_df.columns], inplace=True)

        return disease_df, gene_df, obsolete_df

    def parse_xml(self):
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

                ref_map = {
                    ref.findtext("Source"): ref.findtext("Reference")
                    for ref in gene.find("ExternalReferenceList") or []
                }

                records.append({
                    "Disorder_ID": f"Orphanet:{did}",
                    "Disorder_Name": name,
                    "Gene_Symbol": gene.findtext("Symbol"),
                    "Gene_Name": gene.findtext("Name"),
                    "Gene_Ensembl": ref_map.get("Ensembl"),
                    "Gene_HGNC": f"HGNC:{ref_map['HGNC']}" if "HGNC" in ref_map else None,
                    "Gene_OMIM": "|".join(
                        [f"OMIM:{v}" for v in ref_map["OMIM"].split("|")]
                    ) if "OMIM" in ref_map else None,
                    "Association_Type": assoc.findtext("DisorderGeneAssociationType/Name"),
                    "SourceOfValidation": assoc.findtext("SourceOfValidation")
                })

        df = pd.DataFrame(records)
        df.rename(columns={col: f"orphanet_{col}" for col in df.columns}, inplace=True)
        return df

    def run(self):
        disease_df, gene_df, obsolete_df = self.parse_owl()
        xml_df = self.parse_xml()

        prev_disease = self.disease_ids_output.with_suffix(".previous.csv")
        prev_obsolete = self.orphanet_obsolete_output.with_suffix(".previous.csv")

        if prev_disease.exists():
            old_df = pd.read_csv(prev_disease, dtype=str)
            diff = compute_dataframe_diff(
                old_df,
                disease_df,
                id_col="orphanet_Orphanet_ID",
                label_col="orphanet_Disease_Name",
                compare_cols=[c for c in disease_df.columns if c.startswith("orphanet_") and c not in ["orphanet_Orphanet_ID", "orphanet_Disease_Name"]]
            )
            write_diff_json(diff, self.disease_ids_output.with_name("orphanet_changes.qc.json"))

        if prev_obsolete.exists():
            old_obs = pd.read_csv(prev_obsolete, dtype=str)
            obs_diff = compute_dataframe_diff(
                old_obs,
                obsolete_df,
                id_col="orphanet_Orphanet_ID",
                label_col="orphanet_Disease_Name"
            )
            write_diff_json(obs_diff, self.orphanet_obsolete_output.with_name("orphanet_obsolete_changes.qc.json"))

        disease_df.to_csv(self.disease_ids_output, index=False)
        gene_df.to_csv(self.gene_ids_output, index=False)
        obsolete_df.to_csv(self.orphanet_obsolete_output, index=False)
        xml_df.to_csv(self.xml_output, index=False)

        disease_df.to_csv(prev_disease, index=False)
        obsolete_df.to_csv(prev_obsolete, index=False)

        meta = {
            "timestamp": datetime.now().isoformat(),
            "disease_records": len(disease_df),
            "gene_records": len(gene_df),
            "obsolete_records": len(obsolete_df),
            "xml_records": len(xml_df),
            "disease_ids_output": str(self.disease_ids_output),
            "gene_ids_output": str(self.gene_ids_output),
            "xml_output": str(self.xml_output),
            "orphanet_obsolete_output": str(self.orphanet_obsolete_output),
        }

        with open(self.metadata_file, "w") as f:
            json.dump(meta, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform Orphanet OWL + XML")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    args = parser.parse_args()

    cfg = yaml.safe_load(open(args.config))
    OrphanetTransformer(cfg).run()