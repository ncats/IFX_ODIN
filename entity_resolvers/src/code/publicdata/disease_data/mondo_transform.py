# mondo_transform.py - Modular MONDO transformer script

import json
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
import argparse
import yaml

class MondoTransformer:
    def __init__(self, config):
        self.input_file = Path(config["mondo"]["mondo_file"])
        self.cleaned_output = Path(config["mondo"]["cleaned_output"])
        self.metadata_output = Path(config["mondo"]["transform_metadata"])
        self.obsolete_output = Path(config["mondo"]["obsolete_output"])

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

        # Split out obsolete terms
        obsolete_df = df[df["mondo_preferred_label"].str.startswith("obsolete ", na=False)].copy()
        df = df[~df["mondo_preferred_label"].str.startswith("obsolete ", na=False)].copy()

        if not obsolete_df.empty:
            self.obsolete_output.parent.mkdir(parents=True, exist_ok=True)
            obsolete_df.to_csv(self.obsolete_output, index=False)
            if not self.obsolete_output.exists():
                logging.error(f"❌ Failed to save obsolete MONDO terms to: {self.obsolete_output}")
            else:
                logging.info(f"💾 Saved {len(obsolete_df)} obsolete MONDO terms → {self.obsolete_output}")

        df = self.explode_and_pivot(df, "mondo_id", "mondo_database_cross_reference")

        if "mondo_umls" in df.columns:
            df["mondo_umls"] = df["mondo_umls"].str.replace("UMLS:", "", regex=False)

        if "mondo_meddra" in df.columns:
            df["mondo_meddra"] = df["mondo_meddra"].str.replace("MedDRA:", "MEDDRA:", regex=False)

        self.cleaned_output.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.cleaned_output, index=False)
        if not self.cleaned_output.exists():
            logging.error(f"❌ Failed to save cleaned MONDO to: {self.cleaned_output}")
        else:
            logging.info(f"💾 Saved cleaned MONDO → {self.cleaned_output}")

        self.metadata_output.parent.mkdir(parents=True, exist_ok=True)
        metadata = {
            "timestamp": datetime.now().isoformat(),
            "input_file": str(self.input_file.resolve()),
            "output_file": str(self.cleaned_output.resolve()),
            "obsolete_file": str(self.obsolete_output.resolve()),
            "records": len(df)
        }
        with open(self.metadata_output, "w") as f:
            json.dump(metadata, f, indent=2)

        if not self.metadata_output.exists():
            logging.error(f"❌ Failed to write metadata to: {self.metadata_output}")
        else:
            logging.info(f"📝 Saved metadata → {self.metadata_output}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MONDO transformer")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    args = parser.parse_args()
    cfg = yaml.safe_load(open(args.config))
    MondoTransformer(cfg).run()
