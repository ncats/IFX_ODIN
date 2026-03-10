#!/usr/bin/env python3
"""
UMLS transformer — normalize downloader outputs into:
  1) umls_concepts.csv
  2) umls_parent_child_edges.csv
and write transform metadata JSON.

Expected downloader columns (CSV) or JSON keys:
  cui, name, semantic_types, parents, children, parent_xrefs, child_xrefs
"""

import os
import json
import yaml
import logging
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---------------- logging ----------------

def setup_logging(log_file: str):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(log_file, mode="a")],
        force=True,
    )

# ---------------- utils ----------------

def _split_pipe(s: str):
    if pd.isna(s) or not str(s).strip():
        return []
    return [x for x in str(s).split("|") if x]

def _dedup_join(items):
    return "|".join(sorted(set([i for i in items if i])))

def _normalize_xref(x: str) -> str:
    """
    Normalize common source prefixes to match your convention (MedGen/Orphanet style).
    Examples:
      MedDRA:123  -> MEDDRA:123
      SNOMEDCT_US:111 -> SNOMEDCT:111
      MeSH:D01234 -> MESH:D01234
      ICD-10:...  -> ICD10CM:...
      Orphanet_12 -> Orphanet:12
    """
    if not x:
        return x
    x = str(x)

    # Orphanet_ → Orphanet:
    if x.startswith("Orphanet_"):
        return "Orphanet:" + x[len("Orphanet_"):]
    # SNOMEDCT_US → SNOMEDCT
    if x.startswith("SNOMEDCT_US:"):
        return "SNOMEDCT:" + x.split(":", 1)[1]
    # MedDRA → MEDDRA
    if x.startswith("MedDRA:"):
        return "MEDDRA:" + x.split(":", 1)[1]
    # MeSH → MESH
    if x.startswith("MeSH:"):
        return "MESH:" + x.split(":", 1)[1]
    # ICD-10 → ICD10CM
    if x.startswith("ICD-10:"):
        return "ICD10CM:" + x.split(":", 1)[1]
    # Already normalized (OMIM:, MESH:, MEDDRA:, SNOMEDCT:, GARD:, Orphanet:, ICD10CM:, UMLS:)
    return x

def _normalize_xrefs_pipe(xrefs_pipe: str) -> str:
    xs = [_normalize_xref(x) for x in _split_pipe(xrefs_pipe)]
    return _dedup_join(xs)

def _infer_defaults(cfg: dict) -> dict:
    """
    Allow minimal YAML by inferring outputs from cleaned/raw file locations if keys are missing.
    """
    out = dict(cfg)  # shallow copy
    cleaned = Path(cfg["cleaned_file"]).resolve()
    meta_dir = Path(cfg.get("transform_metadata_file", cfg.get("transform_metadata", cleaned.parent / "umls_transform_metadata.json"))).parent
    out.setdefault("concepts_output", str(cleaned.parent / "umls_concepts.csv"))
    out.setdefault("edges_output", str(cleaned.parent / "umls_parent_child_edges.csv"))
    out.setdefault("transform_metadata_file", cfg.get("transform_metadata_file", str(meta_dir / "umls_transform_metadata.json")))
    out.setdefault("log_file", cfg.get("log_file", str(meta_dir / "umls_transform.log")))
    return out

# ---------------- transformer ----------------

class UMLSTransformer:
    def __init__(self, full_config: dict):
        self.full_config = full_config
        # Accept either 'umls' at top-level or inside a diseases subsection; here assume top-level 'umls'
        self.cfg = _infer_defaults(full_config["umls"])

        self.raw_file = Path(self.cfg["raw_file"])
        self.cleaned_input = Path(self.cfg["cleaned_file"])
        self.concepts_output = Path(self.cfg["concepts_output"])
        self.edges_output = Path(self.cfg["edges_output"])
        self.transform_metadata_file = Path(self.cfg["transform_metadata_file"])
        self.log_file = Path(self.cfg["log_file"])

        setup_logging(str(self.log_file))

    def _load_source(self) -> pd.DataFrame:
        """
        Prefer CSV from downloader; fallback to raw JSON.
        """
        if self.cleaned_input.exists():
            logging.info(f"📥 Reading downloader CSV: {self.cleaned_input}")
            df = pd.read_csv(self.cleaned_input, dtype=str, keep_default_na=False)
            return df
        elif self.raw_file.exists():
            logging.info(f"📥 Reading downloader JSON: {self.raw_file}")
            with open(self.raw_file, "r") as f:
                data = json.load(f)
            return pd.DataFrame(data)
        else:
            raise FileNotFoundError(f"No input found. Checked: {self.cleaned_input} and {self.raw_file}")

    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rename to consistent umls_* columns and ensure presence.
        """
        rename_map = {
            "cui": "umls_CUI",
            "name": "umls_preferred_label",
            "semantic_types": "umls_semantic_types",
            "parents": "umls_parents",
            "children": "umls_children",
            "parent_xrefs": "umls_parent_xrefs",
            "child_xrefs": "umls_child_xrefs",
        }
        for src, dst in rename_map.items():
            if src in df.columns:
                df.rename(columns={src: dst}, inplace=True)
            elif dst not in df.columns:
                df[dst] = ""  # ensure columns exist

        # Normalize xrefs and build union
        df["umls_parent_xrefs"] = df["umls_parent_xrefs"].apply(_normalize_xrefs_pipe)
        df["umls_child_xrefs"] = df["umls_child_xrefs"].apply(_normalize_xrefs_pipe)

        all_xrefs = []
        for p, c in zip(df["umls_parent_xrefs"], df["umls_child_xrefs"]):
            xs = set()
            if p: xs.update(_split_pipe(p))
            if c: xs.update(_split_pipe(c))
            all_xrefs.append(_dedup_join(xs))
        df["umls_xrefs"] = all_xrefs

        # Basic cleanups
        df["umls_CUI"] = df["umls_CUI"].astype(str)
        df["umls_preferred_label"] = df["umls_preferred_label"].astype(str)
        df["umls_semantic_types"] = df["umls_semantic_types"].astype(str)

        # Keep a tight set of columns in a friendly order
        cols = [
            "umls_CUI", "umls_preferred_label", "umls_semantic_types",
            "umls_xrefs", "umls_parent_xrefs", "umls_child_xrefs",
            "umls_parents", "umls_children"
        ]
        # add any leftover cols at the end
        rest = [c for c in df.columns if c not in cols]
        return df[cols + rest]

    def _build_edges(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create child→parent rows from umls_parents.
        """
        rows = []
        for _, r in df.iterrows():
            child = r.get("umls_CUI", "")
            if not child:
                continue
            for parent in _split_pipe(r.get("umls_parents", "")):
                if not parent or not parent.startswith("C"):
                    continue
                rows.append({
                    "child_cui": child,
                    "parent_cui": parent,
                    "predicate": "rdfs:subClassOf",  # keep simple (can switch to biolink later)
                })
        if not rows:
            return pd.DataFrame(columns=["child_cui", "parent_cui", "predicate"])
        return pd.DataFrame(rows).drop_duplicates()

    def run(self):
        df = self._load_source()
        logging.info(f"🔧 Transforming {len(df)} UMLS rows")

        df_std = self._standardize_columns(df)
        edges_df = self._build_edges(df_std)

        # Save outputs
        self.concepts_output.parent.mkdir(parents=True, exist_ok=True)
        self.edges_output.parent.mkdir(parents=True, exist_ok=True)
        df_std.to_csv(self.concepts_output, index=False)
        edges_df.to_csv(self.edges_output, index=False)

        # Optionally read dl metadata (to carry release_info forward)
        release_info = None
        dl_meta_path = self.full_config["umls"].get("dl_metadata_file")
        if dl_meta_path and Path(dl_meta_path).exists():
            try:
                with open(dl_meta_path, "r") as f:
                    release_info = json.load(f).get("release_info")
            except Exception:
                release_info = None

        # Transform metadata
        meta = {
            "timestamp": datetime.now().isoformat(),
            "input_csv": str(self.cleaned_input.resolve()) if self.cleaned_input.exists() else None,
            "input_json": str(self.raw_file.resolve()) if self.raw_file.exists() else None,
            "concepts_output": str(self.concepts_output.resolve()),
            "edges_output": str(self.edges_output.resolve()),
            "records_concepts": int(len(df_std)),
            "records_edges": int(len(edges_df)),
            "carried_release_info": release_info,
        }
        self.transform_metadata_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.transform_metadata_file, "w") as f:
            json.dump(meta, f, indent=2)

        logging.info(f"💾 Saved concepts → {self.concepts_output}")
        logging.info(f"💾 Saved edges → {self.edges_output}")
        logging.info(f"📝 Transform metadata → {self.transform_metadata_file}")

# ---------------- CLI ----------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform UMLS downloader outputs into normalized tables")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    args = parser.parse_args()

    with open(args.config) as f:
        full_cfg = yaml.safe_load(f)

    UMLSTransformer(full_cfg).run()
