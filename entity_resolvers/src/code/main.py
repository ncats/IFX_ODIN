#!/usr/bin/env python
"""
main.py — ODIN Entity Resolution Pipeline CLI
==============================================

Orchestrates all modular source pipelines for each data category.

Usage
-----
  python src/code/main.py TARGETS --all
      Run the full TARGETS pipeline in dependency order.

  python src/code/main.py TARGETS --list
      Show all modules in the TARGETS pipeline with stage groupings.

  python src/code/main.py TARGETS --dry-run --all
      Preview what would run without executing anything.

  python src/code/main.py TARGETS --modules ensembl_download ncbi_download
      Run specific modules only.

  python src/code/main.py TARGETS --from-step nodenorm_gene_download
      Resume the pipeline from nodenorm_gene_download through the end.

  python src/code/main.py TARGETS --from-step nodenorm_gene_download --to-step gene_merge
      Run a slice of the pipeline (both endpoints inclusive).

  python src/code/main.py TARGETS --to-step uniprot_download
      Run from the beginning up to (and including) uniprot_download.

  python src/code/main.py TARGETS --all --skip-errors
      Continue running subsequent steps even if one fails.

  python src/code/main.py TARGETS --ensembl_download
      Run a single step via its flag.

Categories: TARGETS, DISEASES, DRUGS, GO, PPI, PHENOTYPES, PATHWAYS, METABOLITES
"""

import argparse
import os
import sys
import time
import traceback
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# TARGETS category imports
# ---------------------------------------------------------------------------
from publicdata.target_data.ensembl_download import EnsemblDownloader
from publicdata.target_data.ensembl_transform import EnsemblTransformer
from publicdata.target_data.ensembl_uniprot_isoform_xref import EnsemblUniProtIsoformXref
from publicdata.target_data.ncbi_download import NCBIDownloader
from publicdata.target_data.ncbi_transform import NCBITransformer
from publicdata.target_data.hgnc_download import HGNCDownloader
from publicdata.target_data.hgnc_transform import HGNCTransformer
from publicdata.target_data.refseq_download import RefSeqDownloader
from publicdata.target_data.refseq_transform import RefSeqTransformer
from publicdata.target_data.uniprot_download import UniprotDownloader
from publicdata.target_data.uniprot_transform import UniProtTransformer
from publicdata.target_data.nodenorm_gene_download import NodeNormGeneDownloader
from publicdata.target_data.nodenorm_gene_transform import NodeNormGeneTransformer
from publicdata.target_data.nodenorm_protein_download import NodeNormProteinDownloader
from publicdata.target_data.nodenorm_protein_transform import NodeNormProteinTransformer
from publicdata.target_data.gene_merge import GENEDataMerger
from publicdata.target_data.gene_ids import GeneDataProcessor
from publicdata.target_data.transcript_merge import TranscriptResolver
from publicdata.target_data.transcript_ids import TranscriptDataProcessor
from publicdata.target_data.protein_merge import ProteinResolver
from publicdata.target_data.protein_ids import ProteinDataProcessor
from publicdata.target_data.antibodypedia_scraper import AntibodypediaScraper
from publicdata.target_data.target_version import DownloadCatalogProcessor

# ---------------------------------------------------------------------------
# DISEASES category imports
# ---------------------------------------------------------------------------
from publicdata.disease_data.mondo_download import MondoDownloader
from publicdata.disease_data.mondo_transform import MondoTransformer
from publicdata.disease_data.doid_download import DOIDDownloader
from publicdata.disease_data.doid_transform import DOIDTransformer
from publicdata.disease_data.medgen_download import MedGenDownloader
from publicdata.disease_data.medgen_transform import MedGenTransformer
from publicdata.disease_data.orphanet_download import OrphanetDownloader
from publicdata.disease_data.orphanet_transform import OrphanetTransformer
from publicdata.disease_data.omim_download import OMIMDownloader
from publicdata.disease_data.omim_transform import OMIMTransformer
from publicdata.disease_data.umls_download import UMLSDownloader
from publicdata.disease_data.nodenorm_disease_download import NodeNormDiseaseDownloader
from publicdata.disease_data.nodenorm_disease_transform import NodeNormDiseaseTransformer
from publicdata.disease_data.disease_name_cluster import DiseaseNameClusterer
from publicdata.disease_data.disease_merge import DiseaseDataMerger
from publicdata.disease_data.jensen_download import JensenDiseaseDownloader
from publicdata.disease_data.jensen_transform import JensenDiseaseTransformer
from publicdata.disease_data.analyze_obsolete_driven_disease_flags import ObsoleteDrivenDiseaseFlagAnalyzer

# ---------------------------------------------------------------------------
# Other category imports
# ---------------------------------------------------------------------------
from publicdata.drug_data.gsrs_download import GSRSDownloader
from publicdata.drug_data.gsrs_transform import GSRSTransformer
from publicdata.GO_data.GO_download import GODownloader
from publicdata.GO_data.GO_transform import GOTransformer
from publicdata.PPI_data.string_download import StringPPIDownloader
from publicdata.PPI_data.string_transform import StringPPITransformer
from publicdata.phenotype_data.hpo_download import HPOPhenotypeDownloader
from publicdata.phenotype_data.hpo_transform import HPOPhenotypeTransformer
from publicdata.pathway_data.pathwaycommons_download import PathwayCommonsDownloader
from publicdata.pathway_data.pathwaycommons_transform import PathwayCommonsTransformer
from publicdata.pathway_data.panther_download import PantherDownloader
from publicdata.pathway_data.panther_transform import PantherTransformer
from publicdata.pathway_data.reactome_download import ReactomeDownloader
from publicdata.pathway_data.reactome_transform import ReactomeTransformer
from publicdata.pathway_data.wikipathway_download import WikiPathwaysDownloader
from publicdata.pathway_data.wikipathway_transform import WikiPathwaysTransformer
from publicdata.pathway_data.nodenorm_pathway_download import NodeNormPathwayDownloader
from publicdata.pathway_data.nodenorm_pathway_transform import NodeNormPathwayTransformer
from publicdata.pathway_data.pathways_merge import PathwayMergerTransformer
from publicdata.pathway_data.pathway_ids import PathwayIDGenerator
from publicdata.metabolite_data.hmdb_transform import HMDBTransformer

# ---------------------------------------------------------------------------
# Default config paths
# ---------------------------------------------------------------------------
DEFAULT_CONFIGS = {
    "TARGETS":     "config/targets_config.yaml",
    "DISEASES":    "config/diseases_config.yaml",
    "DRUGS":       "config/drugs_config.yaml",
    "GO":          "config/GO_config.yaml",
    "PPI":         "config/ppi_config.yaml",
    "PHENOTYPES":  "config/phenotypes_config.yaml",
    "PATHWAYS":    "config/pathways_config.yaml",
    "METABOLITES": "config/metabolites_config.yaml",
}

# ---------------------------------------------------------------------------
# Processor registry
# Each entry: name → (class, config_section_or_None, short_description)
# ---------------------------------------------------------------------------
PROCESSOR_MAP = {
    # TARGETS ── downloads
    "ensembl_download":           (EnsemblDownloader,          None, "Download Ensembl BioMart data (4 queries)"),
    "ncbi_download":              (NCBIDownloader,              None, "Download NCBI gene_info for Homo sapiens"),
    "hgnc_download":              (HGNCDownloader,              None, "Download HGNC complete gene set"),
    "refseq_download":            (RefSeqDownloader,            None, "Download RefSeq gene2refseq, gene2ensembl, gene_refseq_uniprotkb"),
    "uniprot_download":           (UniprotDownloader,           None, "Download UniProtKB JSON + ID mapping + SPARQL isoforms"),
    "nodenorm_gene_download":     (NodeNormGeneDownloader,      None, "Download NodeNorm gene compendium (human-filtered JSONL)"),
    "nodenorm_protein_download":  (NodeNormProteinDownloader,   None, "Download NodeNorm protein compendium (human-filtered JSONL)"),
    # TARGETS ── transforms
    "ensembl_transform":          (EnsemblTransformer,          None, "Merge 4 Ensembl BioMart parts → ensembl_data_with_isoforms.csv"),
    "ensembl_isoform":            (EnsemblUniProtIsoformXref,   None, "SPARQL cross-ref Ensembl ↔ UniProt isoforms; fill missing isoform IDs"),
    "ncbi_transform":             (NCBITransformer,             None, "Parse NCBI gene_info TSV, expand dbXrefs → ncbi_gene_info.csv"),
    "hgnc_transform":             (HGNCTransformer,             None, "Clean HGNC TSV, expand UniProt IDs → hgnc_complete_set.csv"),
    "refseq_transform":           (RefSeqTransformer,           None, "Rename RefSeq columns, build RNA/protein concat CSVs"),
    "uniprot_transform":          (UniProtTransformer,          None, "Flatten UniProt JSON → mapping + reviewed info CSVs with isoforms"),
    "nodenorm_gene_transform":    (NodeNormGeneTransformer,     None, "Parse NodeNorm gene JSONL → nodenorm_genes.csv"),
    "nodenorm_protein_transform": (NodeNormProteinTransformer,  None, "Parse NodeNorm protein JSONL → nodenorm_proteins.csv"),
    # TARGETS ── merges
    "gene_merge":                 (GENEDataMerger,              None, "Merge Ensembl + NCBI + HGNC + NodeNorm → gene_mapping_provenance.csv"),
    "transcript_merge":           (TranscriptResolver,          None, "Merge BioMart + RefSeq + Ensembl isoforms → transcript_mapping_provenance.csv"),
    "protein_merge":              (ProteinResolver,             None, "Merge UniProt + Ensembl + RefSeq + NodeNorm → protein_provenance_mapping.csv"),
    # TARGETS ── ID generation
    "gene_ids":                   (GeneDataProcessor,           None, "Mint/preserve stable NCATS gene IDs → gene_ids.tsv"),
    "transcript_ids":             (TranscriptDataProcessor,     None, "Mint/preserve stable NCATS transcript IDs → transcript_ids.tsv"),
    "protein_ids":                (ProteinDataProcessor,        None, "Mint/preserve stable NCATS protein IDs → protein_ids.tsv"),
    # TARGETS ── optional / post-processing
    "target_version":             (DownloadCatalogProcessor,    None, "Collect all dl_*_metadata.json → dl_catalog.csv version summary"),
    "antibodypedia":              (AntibodypediaScraper,        None, "[OPTIONAL] Scrape Antibodypedia for antibody counts (requires Chrome)"),

    # DISEASES
    "mondo_download":              (MondoDownloader,             None, "Download Mondo disease ontology"),
    "mondo_transform":             (MondoTransformer,            None, "Transform Mondo OWL → cleaned CSV"),
    "doid_download":               (DOIDDownloader,              None, "Download Disease Ontology (DOID)"),
    "doid_transform":              (DOIDTransformer,             None, "Transform DOID OWL → cleaned CSV"),
    "medgen_download":             (MedGenDownloader,            None, "Download NCBI MedGen concepts"),
    "medgen_transform":            (MedGenTransformer,           None, "Transform MedGen data"),
    "orphanet_download":           (OrphanetDownloader,          None, "Download Orphanet rare disease data"),
    "orphanet_transform":          (OrphanetTransformer,         None, "Transform Orphanet XML → CSV"),
    "omim_download":               (OMIMDownloader,              None, "Download OMIM gene-phenotype data"),
    "omim_transform":              (OMIMTransformer,             None, "Transform OMIM data"),
    "umls_download":               (UMLSDownloader,              None, "Download UMLS metathesaurus (requires UMLS account)"),
    "nodenorm_disease_download":   (NodeNormDiseaseDownloader,   None, "Download NodeNorm disease compendium"),
    "nodenorm_disease_transform":  (NodeNormDiseaseTransformer,  None, "Parse NodeNorm disease JSONL → CSV"),
    "disease_name_cluster":        (DiseaseNameClusterer,        None, "Cluster disease names for harmonization"),
    "disease_merge":               (DiseaseDataMerger,           None, "Merge all disease sources → harmonized disease table"),
    "jensen_download":             (JensenDiseaseDownloader,     None, "Download Jensen Lab disease associations"),
    "jensen_transform":            (JensenDiseaseTransformer,    None, "Transform Jensen disease data"),
    "obsolete_flag_analysis":      (ObsoleteDrivenDiseaseFlagAnalyzer, None, "Analyze mismatch/cardinality flags to detect obsolete-driven disease mappings"),

    # DRUGS
    "gsrs_download":  (GSRSDownloader,  None, "Download FDA GSRS substance registry"),
    "gsrs_transform": (GSRSTransformer, None, "Transform GSRS data"),

    # GO
    "go_download":  (GODownloader,  None, "Download Gene Ontology (GO) annotation data"),
    "go_transform": (GOTransformer, None, "Transform GO annotations"),

    # PPI
    "string_download":  (StringPPIDownloader,  None, "Download STRING protein interaction network"),
    "string_transform": (StringPPITransformer, None, "Transform STRING PPI data"),

    # PHENOTYPES
    "hpo_download":  (HPOPhenotypeDownloader, None, "Download HPO phenotype annotations"),
    "hpo_transform": (HPOPhenotypeTransformer, None, "Transform HPO data"),

    # PATHWAYS
    "pathwaycommons_download":  (PathwayCommonsDownloader,   None, "Download PathwayCommons SIF"),
    "pathwaycommons_transform": (PathwayCommonsTransformer,  None, "Transform PathwayCommons data"),
    "panther_download":         (PantherDownloader,          None, "Download PANTHER pathway data"),
    "panther_transform":        (PantherTransformer,         None, "Transform PANTHER data"),
    "reactome_download":        (ReactomeDownloader,         None, "Download Reactome pathways"),
    "reactome_transform":       (ReactomeTransformer,        None, "Transform Reactome data"),
    "wikipathway_download":     (WikiPathwaysDownloader,     None, "Download WikiPathways GPML"),
    "wikipathway_transform":    (WikiPathwaysTransformer,    None, "Transform WikiPathways data"),
    "nodenorm_pathway_download":  (NodeNormPathwayDownloader,  None, "Download NodeNorm pathway compendium"),
    "nodenorm_pathway_transform": (NodeNormPathwayTransformer, None, "Transform NodeNorm pathway JSONL"),
    "pathways_merge":           (PathwayMergerTransformer,   None, "Merge all pathway sources"),
    "pathway_ids":              (PathwayIDGenerator,         None, "Mint stable NCATS pathway IDs"),

    # METABOLITES
    "hmdb_transform": (HMDBTransformer, None, "Transform HMDB metabolite data"),
}

# ---------------------------------------------------------------------------
# Pipeline stage ordering per category
# --all uses these lists (order matters — dependencies must run first).
# antibodypedia is excluded from --all because it requires Selenium + Chrome.
# ---------------------------------------------------------------------------
CATEGORY_PIPELINE = {
    "TARGETS": [
        # ── Stage 1: Downloads ──────────────────────────────────────────
        "ensembl_download",
        "ncbi_download",
        "hgnc_download",
        "refseq_download",
        "uniprot_download",
        "nodenorm_gene_download",
        "nodenorm_protein_download",
        # ── Stage 2: Transforms ─────────────────────────────────────────
        "ensembl_transform",
        "ensembl_isoform",
        "ncbi_transform",
        "hgnc_transform",
        "refseq_transform",
        "uniprot_transform",
        "nodenorm_gene_transform",
        "nodenorm_protein_transform",
        # ── Stage 3: Merges ─────────────────────────────────────────────
        "gene_merge",
        "transcript_merge",
        "protein_merge",
        # ── Stage 4: ID Generation ──────────────────────────────────────
        "gene_ids",
        "transcript_ids",
        "protein_ids",
        # ── Stage 5: Catalog ────────────────────────────────────────────
        "target_version",
        # NOTE: 'antibodypedia' is intentionally excluded from --all.
        # Run it manually: python src/code/main.py TARGETS --antibodypedia
    ],
    "DISEASES": [
        "mondo_download", "mondo_transform",
        "doid_download", "doid_transform",
        "medgen_download", "medgen_transform",
        "orphanet_download", "orphanet_transform",
        "omim_download", "omim_transform",
        "umls_download",
        "nodenorm_disease_download", "nodenorm_disease_transform",
        "jensen_download", "jensen_transform",
        "disease_name_cluster",
        "disease_merge", "obsolete_flag_analysis",
    ],
    "DRUGS":      ["gsrs_download", "gsrs_transform"],
    "GO":         ["go_download", "go_transform"],
    "PPI":        ["string_download", "string_transform"],
    "PHENOTYPES": ["hpo_download", "hpo_transform"],
    "PATHWAYS":   [
        "pathwaycommons_download", "pathwaycommons_transform",
        "panther_download", "panther_transform",
        "reactome_download", "reactome_transform",
        "wikipathway_download", "wikipathway_transform",
        "nodenorm_pathway_download", "nodenorm_pathway_transform",
        "pathways_merge", "pathway_ids",
    ],
    "METABOLITES": ["hmdb_transform"],
}

# Map module → pipeline stage label for display
TARGETS_STAGES = {
    "ensembl_download":           "1: Download",
    "ncbi_download":              "1: Download",
    "hgnc_download":              "1: Download",
    "refseq_download":            "1: Download",
    "uniprot_download":           "1: Download",
    "nodenorm_gene_download":     "1: Download",
    "nodenorm_protein_download":  "1: Download",
    "ensembl_transform":          "2: Transform",
    "ensembl_isoform":            "2: Transform",
    "ncbi_transform":             "2: Transform",
    "hgnc_transform":             "2: Transform",
    "refseq_transform":           "2: Transform",
    "uniprot_transform":          "2: Transform",
    "nodenorm_gene_transform":    "2: Transform",
    "nodenorm_protein_transform": "2: Transform",
    "gene_merge":                 "3: Merge",
    "transcript_merge":           "3: Merge",
    "protein_merge":              "3: Merge",
    "gene_ids":                   "4: ID Generation",
    "transcript_ids":             "4: ID Generation",
    "protein_ids":                "4: ID Generation",
    "target_version":             "5: Catalog",
    "antibodypedia":              "6: Optional",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_config(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def _banner(text: str, char: str = "─", width: int = 70) -> str:
    return f"\n{char * width}\n  {text}\n{char * width}"


def _stage_banner(stage: str, current: int, total: int) -> str:
    return f"\n{'─' * 70}\n  [{current}/{total}] {stage}\n{'─' * 70}"


def list_pipeline(category: str) -> None:
    """Print the ordered pipeline for a category with stage groupings."""
    pipeline = CATEGORY_PIPELINE.get(category.upper(), [])
    if not pipeline:
        print(f"No pipeline defined for category '{category}'.")
        return

    print(_banner(f"ODIN Pipeline — {category}"))
    stage_map = TARGETS_STAGES if category.upper() == "TARGETS" else {}
    last_stage = None

    for i, name in enumerate(pipeline, 1):
        stage = stage_map.get(name, "")
        if stage and stage != last_stage:
            print(f"\n  ── {stage} ──")
            last_stage = stage
        desc = ""
        if name in PROCESSOR_MAP:
            desc = PROCESSOR_MAP[name][2]
        print(f"  {i:>2}. {name:<35} {desc}")

    optional = [k for k, v in PROCESSOR_MAP.items()
                if k not in pipeline and category.upper() in _infer_category(k)]
    if optional:
        print(f"\n  ── Optional (not in --all) ──")
        for name in optional:
            desc = PROCESSOR_MAP[name][2]
            print(f"      {name:<35} {desc}")

    print(f"\nTotal: {len(pipeline)} steps in --all pipeline")
    print(f"\nRun examples:")
    print(f"  python src/code/main.py {category} --all")
    print(f"  python src/code/main.py {category} --from-step nodenorm_gene_download")
    print(f"  python src/code/main.py {category} --from-step nodenorm_gene_download --to-step gene_merge")
    print(f"  python src/code/main.py {category} --modules ensembl_download ncbi_download")


def _infer_category(module_name: str) -> list:
    """Rough heuristic to associate a module with a category (for --list display)."""
    PREFIXES = {
        "TARGETS":     ["ensembl_", "ncbi_", "hgnc_", "refseq_", "uniprot_",
                         "nodenorm_gene_", "nodenorm_protein_", "gene_",
                         "transcript_", "protein_", "antibodypedia", "target_"],
        "DISEASES":    ["mondo_", "doid_", "medgen_", "orphanet_", "omim_",
                         "umls", "nodenorm_disease_", "disease_", "jensen_"],
        "DRUGS":       ["gsrs_"],
        "GO":          ["go_"],
        "PPI":         ["string_"],
        "PHENOTYPES":  ["hpo_"],
        "PATHWAYS":    ["pathwaycommons_", "panther", "reactome_", "wikipathway_",
                         "nodenorm_pathway_", "pathways_", "pathway_"],
        "METABOLITES": ["hmdb_"],
    }
    return [cat for cat, prefixes in PREFIXES.items()
            if any(module_name.startswith(p) for p in prefixes)]


def run_selected_processors(
    selected: list,
    config: dict,
    dry_run: bool = False,
    skip_errors: bool = False,
) -> dict:
    """
    Run processors in order. Returns summary dict.

    Parameters
    ----------
    selected    : ordered list of module names to run
    config      : full config dict
    dry_run     : if True, print what would run without executing
    skip_errors : if True, continue after a step failure; otherwise abort
    """
    results = {"succeeded": [], "failed": [], "skipped": []}
    total = len(selected)

    if dry_run:
        print(_banner(f"DRY RUN — {total} step(s) would be executed"))
        for i, key in enumerate(selected, 1):
            desc = PROCESSOR_MAP.get(key, (None, None, ""))[2]
            print(f"  [{i}/{total}] {key}  —  {desc}")
        print()
        return results

    print(_banner(f"Running {total} step(s)"))

    for i, key in enumerate(selected, 1):
        if key not in PROCESSOR_MAP:
            print(f"\n⚠️  Unknown module: '{key}' — skipping.")
            results["skipped"].append(key)
            continue

        cls_or_fn, section, desc = PROCESSOR_MAP[key]
        print(_stage_banner(f"{key}  —  {desc}", i, total))

        t_start = time.perf_counter()
        try:
            cfg_to_pass = config if section is None else config.get(section, {})
            instance = cls_or_fn(cfg_to_pass) if section is not None else cls_or_fn(config)

            if hasattr(instance, "run"):
                instance.run()
            elif hasattr(instance, "process"):
                instance.process()
            elif callable(instance):
                instance()

            elapsed = time.perf_counter() - t_start
            print(f"\n  ✅ {key} completed in {elapsed:.1f}s")
            results["succeeded"].append(key)

        except Exception as exc:
            elapsed = time.perf_counter() - t_start
            print(f"\n  ❌ {key} FAILED after {elapsed:.1f}s: {exc}")
            traceback.print_exc()
            results["failed"].append(key)
            if not skip_errors:
                print(f"\n  Pipeline aborted at step {i}/{total}.")
                print(f"  Use --skip-errors to continue past failures.\n")
                break

    # Final summary
    _print_summary(results, total)
    return results


def _print_summary(results: dict, total: int) -> None:
    n_ok  = len(results["succeeded"])
    n_fail = len(results["failed"])
    n_skip = len(results["skipped"])
    print(_banner("Pipeline Summary", "═"))
    print(f"  ✅ Succeeded : {n_ok}/{total}")
    if n_fail:
        print(f"  ❌ Failed    : {n_fail}/{total}")
        for name in results["failed"]:
            print(f"       • {name}")
    if n_skip:
        print(f"  ⚠️  Skipped   : {n_skip}")
        for name in results["skipped"]:
            print(f"       • {name}")
    status = "SUCCESS" if n_fail == 0 else "COMPLETED WITH ERRORS"
    print(f"\n  Status: {status}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        prog="python src/code/main.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "category",
        choices=["TARGETS", "DISEASES", "DRUGS", "GO", "PPI", "PHENOTYPES", "PATHWAYS", "METABOLITES"],
        help="Data category to process",
    )
    parser.add_argument("--config", type=str,
                        help="Path to YAML config file. Uses category default if omitted.")
    parser.add_argument("--all", action="store_true",
                        help="Run all modules in the pipeline (ordered by dependency)")
    parser.add_argument("--modules", nargs="+", metavar="MODULE",
                        help="Run specific modules by name (space-separated)")
    parser.add_argument("--from-step", metavar="MODULE",
                        help="Start from this step (inclusive); implies --all if no other mode given")
    parser.add_argument("--to-step", metavar="MODULE",
                        help="Stop after this step (inclusive); implies --all if no other mode given")
    parser.add_argument("--list", action="store_true",
                        help="List all modules in this category's pipeline and exit")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would run without executing anything")
    parser.add_argument("--skip-errors", action="store_true",
                        help="Continue running subsequent steps even if one fails")

    # Auto-generate --{processor_name} flags for running a single step
    for name in PROCESSOR_MAP:
        parser.add_argument(f"--{name}", action="store_true",
                            help=f"Run only `{name}`")

    args = parser.parse_args()
    category = args.category.upper()

    # ── --list ─────────────────────────────────────────────────────────
    if args.list:
        list_pipeline(category)
        sys.exit(0)

    # ── Load config ────────────────────────────────────────────────────
    cfg_path = Path(args.config) if args.config else Path(DEFAULT_CONFIGS[category])
    if not cfg_path.exists():
        print(f"❌ Config file not found: {cfg_path}")
        print(f"   Specify a path with --config, or create {cfg_path}")
        sys.exit(1)
    config = load_config(cfg_path)

    # ── Determine which modules to run ────────────────────────────────
    # --from-step and/or --to-step imply --all when no other mode given
    if args.from_step or args.to_step:
        if not args.all and not args.modules:
            args.all = True

    if args.all:
        to_run = list(CATEGORY_PIPELINE.get(category, []))

        # Slice: --from-step (inclusive start)
        if args.from_step:
            if args.from_step not in to_run:
                print(f"❌ --from-step '{args.from_step}' not found in {category} pipeline.")
                print(f"   Use --list to see valid module names.")
                sys.exit(1)
            start_idx = to_run.index(args.from_step)
            skipped = to_run[:start_idx]
            to_run = to_run[start_idx:]
            if skipped:
                print(f"  Skipping {len(skipped)} step(s) before '{args.from_step}':")
                for s in skipped:
                    print(f"    • {s}")

        # Slice: --to-step (inclusive end)
        if args.to_step:
            if args.to_step not in to_run:
                print(f"❌ --to-step '{args.to_step}' not found in {category} pipeline"
                      f"{' (after --from-step slice)' if args.from_step else ''}.")
                print(f"   Use --list to see valid module names.")
                sys.exit(1)
            end_idx = to_run.index(args.to_step)
            trimmed = to_run[end_idx + 1:]
            to_run = to_run[:end_idx + 1]
            if trimmed:
                print(f"  Stopping after '{args.to_step}' — skipping {len(trimmed)} later step(s).")

    elif args.modules:
        to_run = args.modules
    else:
        to_run = [name for name in PROCESSOR_MAP if getattr(args, name, False)]
        if not to_run:
            parser.error(
                "Must specify one of:\n"
                "  --all                          run the full pipeline\n"
                "  --modules M1 M2                run specific modules\n"
                "  --from-step M                  resume from step M to the end\n"
                "  --to-step M                    run from start up to step M\n"
                "  --from-step M1 --to-step M2    run a range of steps\n"
                "  --list                         list available modules\n"
                "  --<module_name>                run a single module\n\n"
                f"  Example: python src/code/main.py {category} --from-step nodenorm_gene_download"
            )

    results = run_selected_processors(
        to_run,
        config,
        dry_run=args.dry_run,
        skip_errors=args.skip_errors,
    )

    # Force immediate exit to avoid PyTorch MPS segfault during interpreter cleanup
    os._exit(1 if results.get("failed") else 0)


if __name__ == "__main__":
    main()
