# mondo_transform.py - Modular MONDO transformer script with QC diffs

import json
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
import argparse
import yaml
from rdflib import Graph, URIRef
import re

try:
    from publicdata.disease_data.transform_diff_utils import compute_dataframe_diff, write_diff_json
except ImportError:
    from transform_diff_utils import compute_dataframe_diff, write_diff_json


def setup_logging(log_file):
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file, mode="a"), logging.StreamHandler()],
        force=True
    )


class MondoTransformer:
    def __init__(self, config):
        self.cfg = config["mondo"]
        setup_logging(self.cfg["log_file"])

        self.input_file = Path(self.cfg["json_file"])
        self.cleaned_output = Path(self.cfg["cleaned_output"])
        self.metadata_output = Path(self.cfg["transform_metadata"])
        self.obsolete_output = Path(self.cfg["obsolete_output"])

        self.cleaned_output.parent.mkdir(parents=True, exist_ok=True)
        self.metadata_output.parent.mkdir(parents=True, exist_ok=True)
        self.obsolete_output.parent.mkdir(parents=True, exist_ok=True)

    def parse_mondo_json(self):
        with open(self.input_file, 'r') as f:
            mondo_data = json.load(f)

        terms = [t for t in mondo_data["graphs"][0]["nodes"] if "id" in t and "MONDO" in t["id"]]
        logging.info(f"Parsed {len(terms)} MONDO terms")

        records = []
        for term in terms:
            raw_id = term.get("id", "")
            node_id = raw_id.replace("http://purl.obolibrary.org/obo/", "").replace("_", ":") if raw_id else None
            lbl = term.get("lbl")
            meta = term.get("meta", {})
            defn = meta.get("definition", {}).get("val")
            synonyms = [s["val"] for s in meta.get("synonyms", [])] if "synonyms" in meta else []
            xrefs = [x["val"] for x in meta.get("xrefs", [])] if "xrefs" in meta else []
            parents = [r["obj"] for r in meta.get("basicPropertyValues", []) if r.get("pred") == "rdfs:subClassOf"]

            records.append({
                "mondo_id": node_id,
                "mondo_preferred_label": lbl,
                "mondo_definition": defn,
                "mondo_synonyms": "|".join(synonyms) if synonyms else None,
                "mondo_database_cross_reference": "|".join(xrefs) if xrefs else None,
                "mondo_parents": "|".join(parents) if parents else None
            })

        return pd.DataFrame(records)

    def parse_mondo_owl_relationships(self, owl_path, output_dir):
        logging.info(f"📖 Parsing MONDO OWL for parent-child relationships: {owl_path}")
        g = Graph()
        g.parse(owl_path)

        subClassOf = URIRef("http://www.w3.org/2000/01/rdf-schema#subClassOf")
        parent_to_children = {}
        child_to_parents = {}

        for subj, _, obj in g.triples((None, subClassOf, None)):
            subj_id = re.sub(r".*MONDO_", "MONDO:", str(subj)).replace("_", ":")
            obj_id = re.sub(r".*MONDO_", "MONDO:", str(obj)).replace("_", ":")

            if subj_id.startswith("MONDO:") and obj_id.startswith("MONDO:"):
                parent_to_children.setdefault(obj_id, set()).add(subj_id)
                child_to_parents.setdefault(subj_id, set()).add(obj_id)

        sibling_terms = {}
        for parent, children in parent_to_children.items():
            for child in children:
                sibling_terms[child] = list(children - {child})

        def save_json(obj, filename):
            with open(output_dir / filename, 'w') as f:
                json.dump({k: list(v) for k, v in obj.items()}, f, indent=2)

        save_json(parent_to_children, "parent_to_children.json")
        save_json(child_to_parents, "child_to_parents.json")
        with open(output_dir / "sibling_terms.json", 'w') as f:
            json.dump(sibling_terms, f, indent=2)

    def explode_and_pivot(self, df, id_col, cross_ref_col):
        df[cross_ref_col] = df[cross_ref_col].fillna("").astype(str)
        df[cross_ref_col] = df[cross_ref_col].apply(lambda x: x.split('|') if x else [])

        exploded = df[[id_col, cross_ref_col]].explode(cross_ref_col)
        exploded['source'] = exploded[cross_ref_col].apply(
            lambda x: x.split(':')[0].upper() if isinstance(x, str) and ':' in x else None
        )

        pivoted = exploded.pivot_table(
            index=id_col,
            columns='source',
            values=cross_ref_col,
            aggfunc=lambda x: '|'.join(x.dropna().unique())
        )
        pivoted.columns = [f"mondo_{col.lower()}" for col in pivoted.columns]
        pivoted.reset_index(inplace=True)

        df = df.drop(columns=[cross_ref_col])
        merged = pd.merge(df, pivoted, on=id_col, how='left')
        return merged

    def run(self):
        logging.info(f"📥 Reading MONDO from: {self.input_file.resolve()}")
        df = self.parse_mondo_json()

        df = df.dropna(subset=["mondo_id"]).sort_values("mondo_id")

        obsolete_df = df[df["mondo_preferred_label"].str.startswith("obsolete ", na=False)].copy()
        active_df = df[~df["mondo_preferred_label"].str.startswith("obsolete ", na=False)].copy()

        active_df = self.explode_and_pivot(active_df, "mondo_id", "mondo_database_cross_reference")

        if "mondo_umls" in active_df.columns:
            active_df["mondo_umls"] = active_df["mondo_umls"].str.replace("UMLS:", "", regex=False)

        if "mondo_meddra" in active_df.columns:
            active_df["mondo_meddra"] = active_df["mondo_meddra"].str.replace("MedDRA:", "MEDDRA:", regex=False)

        mondo_owl = Path(self.cfg["owl_file"])
        if mondo_owl.exists():
            self.parse_mondo_owl_relationships(mondo_owl, self.cleaned_output.parent)

        prev_active = self.cleaned_output.with_suffix(".previous.csv")
        prev_obsolete = self.obsolete_output.with_suffix(".previous.csv")

        if prev_active.exists():
            old_df = pd.read_csv(prev_active, dtype=str)
            diff = compute_dataframe_diff(
                old_df,
                active_df,
                id_col="mondo_id",
                label_col="mondo_preferred_label",
                compare_cols=[c for c in active_df.columns if c.startswith("mondo_") and c not in ["mondo_id", "mondo_preferred_label"]]
            )
            write_diff_json(diff, self.cleaned_output.with_name("mondo_changes.qc.json"))

        if prev_obsolete.exists():
            old_obs = pd.read_csv(prev_obsolete, dtype=str)
            obs_diff = compute_dataframe_diff(
                old_obs,
                obsolete_df,
                id_col="mondo_id",
                label_col="mondo_preferred_label"
            )
            write_diff_json(obs_diff, self.obsolete_output.with_name("mondo_obsolete_changes.qc.json"))

        active_df.to_csv(self.cleaned_output, index=False)
        obsolete_df.to_csv(self.obsolete_output, index=False)
        active_df.to_csv(prev_active, index=False)
        obsolete_df.to_csv(prev_obsolete, index=False)

        metadata = {
            "timestamp": datetime.now().isoformat(),
            "input_file": str(self.input_file.resolve()),
            "output_file": str(self.cleaned_output.resolve()),
            "obsolete_file": str(self.obsolete_output.resolve()),
            "records": len(active_df),
            "obsolete_records": len(obsolete_df)
        }
        with open(self.metadata_output, "w") as f:
            json.dump(metadata, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MONDO transformer")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    args = parser.parse_args()
    cfg = yaml.safe_load(open(args.config))
    MondoTransformer(cfg).run()