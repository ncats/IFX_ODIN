#!/usr/bin/env python
"""
main.py — ODIN Entity Resolution Pipeline CLI
"""

import argparse
import importlib
import os
import sys
import time
import traceback
from pathlib import Path

import yaml
from threading import RLock

try:
    from tqdm import tqdm
    tqdm.set_lock(RLock())
except Exception:
    tqdm = None

os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")

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

PROCESSOR_MAP = {
    "ensembl_download":           ("publicdata.target_data.ensembl_download", "EnsemblDownloader",          None, "Download Ensembl BioMart data (4 queries)"),
    "ncbi_download":              ("publicdata.target_data.ncbi_download", "NCBIDownloader",             None, "Download NCBI gene_info for Homo sapiens"),
    "hgnc_download":              ("publicdata.target_data.hgnc_download", "HGNCDownloader",             None, "Download HGNC complete gene set"),
    "refseq_download":            ("publicdata.target_data.refseq_download", "RefSeqDownloader",           None, "Download RefSeq gene2refseq, gene2ensembl, gene_refseq_uniprotkb"),
    "uniprot_download":           ("publicdata.target_data.uniprot_download", "UniprotDownloader",          None, "Download UniProtKB JSON + ID mapping + SPARQL isoforms"),
    "nodenorm_gene_download":     ("publicdata.target_data.nodenorm_gene_download", "NodeNormGeneDownloader",     None, "Download NodeNorm gene compendium (human-filtered JSONL)"),
    "nodenorm_protein_download":  ("publicdata.target_data.nodenorm_protein_download", "NodeNormProteinDownloader",  None, "Download NodeNorm protein compendium (human-filtered JSONL)"),
    "ensembl_transform":          ("publicdata.target_data.ensembl_transform", "EnsemblTransformer",         None, "Merge 4 Ensembl BioMart parts → ensembl_data_with_isoforms.csv"),
    "ensembl_isoform":            ("publicdata.target_data.ensembl_uniprot_isoform_xref", "EnsemblUniProtIsoformXref",  None, "SPARQL cross-ref Ensembl ↔ UniProt isoforms; fill missing isoform IDs"),
    "ncbi_transform":             ("publicdata.target_data.ncbi_transform", "NCBITransformer",            None, "Parse NCBI gene_info TSV, expand dbXrefs → ncbi_gene_info.csv"),
    "hgnc_transform":             ("publicdata.target_data.hgnc_transform", "HGNCTransformer",            None, "Clean HGNC TSV, expand UniProt IDs → hgnc_complete_set.csv"),
    "refseq_transform":           ("publicdata.target_data.refseq_transform", "RefSeqTransformer",          None, "Rename RefSeq columns, build RNA/protein concat CSVs"),
    "uniprot_transform":          ("publicdata.target_data.uniprot_transform", "UniProtTransformer",         None, "Flatten UniProt JSON → mapping + reviewed info CSVs with isoforms"),
    "nodenorm_gene_transform":    ("publicdata.target_data.nodenorm_gene_transform", "NodeNormGeneTransformer",    None, "Parse NodeNorm gene JSONL → nodenorm_genes.csv"),
    "nodenorm_protein_transform": ("publicdata.target_data.nodenorm_protein_transform", "NodeNormProteinTransformer", None, "Parse NodeNorm protein JSONL → nodenorm_proteins.csv"),
    "gene_merge":                 ("publicdata.target_data.gene_merge", "GENEDataMerger",             None, "Merge Ensembl + NCBI + HGNC + NodeNorm → gene_mapping_provenance.csv"),
    "transcript_merge":           ("publicdata.target_data.transcript_merge", "TranscriptResolver",         None, "Merge BioMart + RefSeq + Ensembl isoforms → transcript_mapping_provenance.csv"),
    "protein_merge":              ("publicdata.target_data.protein_merge", "ProteinResolver",            None, "Merge UniProt + Ensembl + RefSeq + NodeNorm → protein_provenance_mapping.csv"),
    "gene_ids":                   ("publicdata.target_data.gene_ids", "GeneDataProcessor",          None, "Mint/preserve stable NCATS gene IDs → gene_ids.tsv"),
    "transcript_ids":             ("publicdata.target_data.transcript_ids", "TranscriptDataProcessor",    None, "Mint/preserve stable NCATS transcript IDs → transcript_ids.tsv"),
    "protein_ids":                ("publicdata.target_data.protein_ids", "ProteinDataProcessor",       None, "Mint/preserve stable NCATS protein IDs → protein_ids.tsv"),
    "target_version":             ("publicdata.target_data.target_version", "DownloadCatalogProcessor",   None, "Collect all dl_*_metadata.json → dl_catalog.csv version summary"),
    "antibodypedia":              ("publicdata.target_data.antibodypedia_scraper", "AntibodypediaScraper",       None, "[OPTIONAL] Scrape Antibodypedia for antibody counts (requires Chrome)"),

    "mondo_download":              ("publicdata.disease_data.mondo_download", "MondoDownloader",             None, "Download Mondo disease ontology"),
    "mondo_transform":             ("publicdata.disease_data.mondo_transform", "MondoTransformer",            None, "Transform Mondo OWL → cleaned CSV"),
    "doid_download":               ("publicdata.disease_data.doid_download", "DOIDDownloader",              None, "Download Disease Ontology (DOID)"),
    "doid_transform":              ("publicdata.disease_data.doid_transform", "DOIDTransformer",             None, "Transform DOID OWL → cleaned CSV"),
    "medgen_download":             ("publicdata.disease_data.medgen_download", "MedGenDownloader",            None, "Download NCBI MedGen concepts"),
    "medgen_transform":            ("publicdata.disease_data.medgen_transform", "MedGenTransformer",           None, "Transform MedGen data"),
    "orphanet_download":           ("publicdata.disease_data.orphanet_download", "OrphanetDownloader",          None, "Download Orphanet rare disease data"),
    "orphanet_transform":          ("publicdata.disease_data.orphanet_transform", "OrphanetTransformer",         None, "Transform Orphanet XML → CSV"),
    "omim_download":               ("publicdata.disease_data.omim_download", "OMIMDownloader",              None, "Download OMIM gene-phenotype data"),
    "omim_transform":              ("publicdata.disease_data.omim_transform", "OMIMTransformer",             None, "Transform OMIM data"),
    "nodenorm_disease_download":   ("publicdata.disease_data.nodenorm_disease_download", "NodeNormDiseaseDownloader",   None, "Download NodeNorm disease compendium"),
    "nodenorm_disease_transform":  ("publicdata.disease_data.nodenorm_disease_transform", "NodeNormDiseaseTransformer",  None, "Parse NodeNorm disease JSONL → CSV"),
    "disease_merge":               ("publicdata.disease_data.disease_merge", "DiseaseDataMerger",           None, "Merge all disease sources → harmonized disease table"),
    "jensen_download":             ("publicdata.disease_data.jensen_download", "JensenDiseaseDownloader",     None, "Download Jensen Lab disease associations"),
    "jensen_transform":            ("publicdata.disease_data.jensen_transform", "JensenDiseaseTransformer",    None, "Transform Jensen disease data"),
    "disease_ids":                 ("publicdata.disease_data.disease_id", "DiseaseIDProcessor",          None, "Mint/preserve stable NCATS disease IDs → disease_ids.tsv"),
    "rdip_deliverable":            ("publicdata.disease_data.rdip_deliverable", "RDIPDeliverable", None, "Generate RDIP rare disease harmonized xrefs"),

    "gsrs_download":  ("publicdata.drug_data.gsrs_download", "GSRSDownloader",  None, "Download FDA GSRS substance registry"),
    "gsrs_transform": ("publicdata.drug_data.gsrs_transform", "GSRSTransformer", None, "Transform GSRS data"),

    "go_download":  ("publicdata.GO_data.GO_download", "GODownloader",  None, "Download Gene Ontology (GO) annotation data"),
    "go_transform": ("publicdata.GO_data.GO_transform", "GOTransformer", None, "Transform GO annotations"),

    "string_download":  ("publicdata.PPI_data.string_download", "StringPPIDownloader",  None, "Download STRING protein interaction network"),
    "string_transform": ("publicdata.PPI_data.string_transform", "StringPPITransformer", None, "Transform STRING PPI data"),

    "hpo_download":  ("publicdata.phenotype_data.hpo_download", "HPOPhenotypeDownloader", None, "Download HPO phenotype annotations"),
    "hpo_transform": ("publicdata.phenotype_data.hpo_transform", "HPOPhenotypeTransformer", None, "Transform HPO data"),

    "pathwaycommons_download":    ("publicdata.pathway_data.pathwaycommons_download", "PathwayCommonsDownloader",   None, "Download PathwayCommons SIF"),
    "pathwaycommons_transform":   ("publicdata.pathway_data.pathwaycommons_transform", "PathwayCommonsTransformer",  None, "Transform PathwayCommons data"),
    "panther_download":           ("publicdata.pathway_data.panther_download", "PantherDownloader",          None, "Download PANTHER pathway data"),
    "panther_transform":          ("publicdata.pathway_data.panther_transform", "PantherTransformer",         None, "Transform PANTHER data"),
    "reactome_download":          ("publicdata.pathway_data.reactome_download", "ReactomeDownloader",         None, "Download Reactome pathways"),
    "reactome_transform":         ("publicdata.pathway_data.reactome_transform", "ReactomeTransformer",        None, "Transform Reactome data"),
    "wikipathway_download":       ("publicdata.pathway_data.wikipathway_download", "WikiPathwaysDownloader",     None, "Download WikiPathways GPML"),
    "wikipathway_transform":      ("publicdata.pathway_data.wikipathway_transform", "WikiPathwaysTransformer",    None, "Transform WikiPathways data"),
    "nodenorm_pathway_download":  ("publicdata.pathway_data.nodenorm_pathway_download", "NodeNormPathwayDownloader",  None, "Download NodeNorm pathway compendium"),
    "nodenorm_pathway_transform": ("publicdata.pathway_data.nodenorm_pathway_transform", "NodeNormPathwayTransformer", None, "Transform NodeNorm pathway JSONL"),
    "pathways_merge":             ("publicdata.pathway_data.pathways_merge", "PathwayMergerTransformer",   None, "Merge all pathway sources"),
    "pathway_ids":                ("publicdata.pathway_data.pathway_ids", "PathwayIDGenerator",         None, "Mint stable NCATS pathway IDs"),

    "hmdb_transform":             ("publicdata.metabolite_data.hmdb_transform", "HMDBTransformer",            None, "Transform HMDB metabolite data"),
}


def _resolve_processor(name: str):
    module_path, attr_name, section, desc = PROCESSOR_MAP[name]
    module = importlib.import_module(module_path)
    return getattr(module, attr_name), section, desc

CATEGORY_PIPELINE = {
    "TARGETS": [
        "ensembl_download",
        "ncbi_download",
        "hgnc_download",
        "refseq_download",
        "uniprot_download",
        "nodenorm_gene_download",
        "nodenorm_protein_download",
        "ensembl_transform",
        "ensembl_isoform",
        "ncbi_transform",
        "hgnc_transform",
        "refseq_transform",
        "uniprot_transform",
        "nodenorm_gene_transform",
        "nodenorm_protein_transform",
        "gene_merge",
        "transcript_merge",
        "protein_merge",
        "gene_ids",
        "transcript_ids",
        "protein_ids",
        "target_version",
    ],
    "DISEASES": [
        "mondo_download", "mondo_transform",
        "doid_download", "doid_transform",
        "medgen_download", "medgen_transform",
        "orphanet_download", "orphanet_transform",
        "omim_download", "omim_transform",
        "nodenorm_disease_download", "nodenorm_disease_transform",
        "jensen_download", "jensen_transform",
        "disease_merge", "rdip_deliverable",
    ],
    "DRUGS": ["gsrs_download", "gsrs_transform"],
    "GO": ["go_download", "go_transform"],
    "PPI": ["string_download", "string_transform"],
    "PHENOTYPES": ["hpo_download", "hpo_transform"],
    "PATHWAYS": [
        "pathwaycommons_download", "pathwaycommons_transform",
        "panther_download", "panther_transform",
        "reactome_download", "reactome_transform",
        "wikipathway_download", "wikipathway_transform",
        "nodenorm_pathway_download", "nodenorm_pathway_transform",
        "pathways_merge", "pathway_ids",
    ],
    "METABOLITES": ["hmdb_transform"],
}

TARGETS_STAGES = {
    "ensembl_download": "1: Download",
    "ncbi_download": "1: Download",
    "hgnc_download": "1: Download",
    "refseq_download": "1: Download",
    "uniprot_download": "1: Download",
    "nodenorm_gene_download": "1: Download",
    "nodenorm_protein_download": "1: Download",
    "ensembl_transform": "2: Transform",
    "ensembl_isoform": "2: Transform",
    "ncbi_transform": "2: Transform",
    "hgnc_transform": "2: Transform",
    "refseq_transform": "2: Transform",
    "uniprot_transform": "2: Transform",
    "nodenorm_gene_transform": "2: Transform",
    "nodenorm_protein_transform": "2: Transform",
    "gene_merge": "3: Merge",
    "transcript_merge": "3: Merge",
    "protein_merge": "3: Merge",
    "gene_ids": "4: ID Generation",
    "transcript_ids": "4: ID Generation",
    "protein_ids": "4: ID Generation",
    "target_version": "5: Catalog",
    "antibodypedia": "6: Optional",
}


def load_config(path: Path) -> dict:
    """Load and parse a YAML configuration file."""
    with open(path) as f:
        return yaml.safe_load(f)


def _banner(text: str, char: str = "─", width: int = 70) -> str:
    return f"\n{char * width}\n  {text}\n{char * width}"


def _stage_banner(stage: str, current: int, total: int) -> str:
    return f"\n{'─' * 70}\n  [{current}/{total}] {stage}\n{'─' * 70}"


def _infer_category(module_name: str) -> list:
    """Return the list of category names a module belongs to, based on prefix matching."""
    PREFIXES = {
        "TARGETS": ["ensembl_", "ncbi_", "hgnc_", "refseq_", "uniprot_",
                    "nodenorm_gene_", "nodenorm_protein_", "gene_",
                    "transcript_", "protein_", "antibodypedia", "target_"],
        "DISEASES": ["mondo_", "doid_", "medgen_", "orphanet_", "omim_",
                     "umls", "nodenorm_disease_", "disease_", "jensen_"],
        "DRUGS": ["gsrs_"],
        "GO": ["go_"],
        "PPI": ["string_"],
        "PHENOTYPES": ["hpo_"],
        "PATHWAYS": ["pathwaycommons_", "panther", "reactome_", "wikipathway_",
                     "nodenorm_pathway_", "pathways_", "pathway_"],
        "METABOLITES": ["hmdb_"],
    }
    return [cat for cat, prefixes in PREFIXES.items()
            if any(module_name.startswith(p) for p in prefixes)]


def list_pipeline(category: str) -> None:
    """Print a formatted listing of all pipeline steps for a category."""
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
        desc = PROCESSOR_MAP.get(name, (None, None, None, ""))[3]
        print(f"  {i:>2}. {name:<35} {desc}")


def _print_summary(results: dict, total: int) -> None:
    n_ok = len(results["succeeded"])
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


def run_selected_processors(selected, config, dry_run=False, skip_errors=False) -> dict:
    """Execute a list of pipeline modules in order, returning success/failure/skip tallies."""
    results = {"succeeded": [], "failed": [], "skipped": []}
    total = len(selected)

    if dry_run:
        print(_banner(f"DRY RUN — {total} step(s) would be executed"))
        for i, key in enumerate(selected, 1):
            desc = PROCESSOR_MAP.get(key, (None, None, None, ""))[3]
            print(f"  [{i}/{total}] {key}  —  {desc}")
        print()
        return results

    print(_banner(f"Running {total} step(s)"))

    for i, key in enumerate(selected, 1):
        if key not in PROCESSOR_MAP:
            print(f"\n⚠️  Unknown module: '{key}' — skipping.")
            results["skipped"].append(key)
            continue

        cls_or_fn, section, desc = _resolve_processor(key)
        print(_stage_banner(f"{key}  —  {desc}", i, total))

        t_start = time.perf_counter()
        try:
            cfg_to_pass = config if section is None else config.get(section, {})
            instance = cls_or_fn(cfg_to_pass) if section is not None else cls_or_fn(config)

            if hasattr(instance, "run"):
                # target_version needs the full config for manifest generation
                if key == "target_version":
                    result = instance.run(full_config=config)
                else:
                    result = instance.run()
            elif hasattr(instance, "process"):
                result = instance.process()
            elif callable(instance):
                result = instance()
            else:
                result = None

            if result is False:
                raise RuntimeError(f"{key} reported failure")

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

    _print_summary(results, total)
    return results


def main():
    """CLI entry point: parse arguments and run the selected pipeline modules."""
    parser = argparse.ArgumentParser(prog="python src/code/main.py")
    parser.add_argument(
        "category",
        choices=["TARGETS", "DISEASES", "DRUGS", "GO", "PPI", "PHENOTYPES", "PATHWAYS", "METABOLITES"],
        help="Data category to process",
    )
    parser.add_argument("--config", type=str, help="Path to YAML config file")
    parser.add_argument("--all", action="store_true", help="Run all modules in pipeline order")
    parser.add_argument("--modules", nargs="+", metavar="MODULE", help="Run specific modules by name")
    parser.add_argument("--from-step", metavar="MODULE", help="Start from this step")
    parser.add_argument("--to-step", metavar="MODULE", help="Stop after this step")
    parser.add_argument("--list", action="store_true", help="List modules for this category and exit")
    parser.add_argument("--dry-run", action="store_true", help="Show what would run without executing")
    parser.add_argument("--skip-errors", action="store_true", help="Continue even if a step fails")

    for name in PROCESSOR_MAP:
        parser.add_argument(f"--{name}", action="store_true", help=f"Run only `{name}`")

    args = parser.parse_args()
    category = args.category.upper()

    if args.list:
        list_pipeline(category)
        sys.exit(0)

    cfg_path = Path(args.config) if args.config else Path(DEFAULT_CONFIGS[category])
    if not cfg_path.exists():
        print(f"❌ Config file not found: {cfg_path}")
        sys.exit(1)
    config = load_config(cfg_path)

    if args.from_step or args.to_step:
        if not args.all and not args.modules:
            args.all = True

    if args.all:
        to_run = list(CATEGORY_PIPELINE.get(category, []))

        if args.from_step:
            if args.from_step not in to_run:
                print(f"❌ --from-step '{args.from_step}' not found in {category} pipeline.")
                sys.exit(1)
            start_idx = to_run.index(args.from_step)
            to_run = to_run[start_idx:]

        if args.to_step:
            if args.to_step not in to_run:
                print(f"❌ --to-step '{args.to_step}' not found in {category} pipeline.")
                sys.exit(1)
            end_idx = to_run.index(args.to_step)
            to_run = to_run[:end_idx + 1]

    elif args.modules:
        to_run = args.modules
    else:
        to_run = [name for name in PROCESSOR_MAP if getattr(args, name, False)]
        if not to_run:
            parser.error("Specify --all, --modules, --list, or --<module_name>")

    results = run_selected_processors(
        to_run,
        config,
        dry_run=args.dry_run,
        skip_errors=args.skip_errors,
    )

    os._exit(1 if results.get("failed") else 0)


if __name__ == "__main__":
    main()