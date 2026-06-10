import argparse
from pathlib import Path
from typing import List, Optional

from src.registry.download import download_url
from src.registry.manifest import (
    MANIFEST_FILENAME,
    build_source_snapshot_manifest,
    default_content_type,
    file_entry,
    manifest_checksum,
    read_manifest,
    storage_prefix,
    verify_manifest_files,
    write_manifest,
)
from src.registry.storage import DEFAULT_REGISTRY_BUCKET, MinioStorage, load_minio_credentials, s3_uri


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ifx-registry")
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch = subparsers.add_parser("fetch-url", help="Download URL(s), create a source snapshot manifest, and optionally upload to MinIO.")
    fetch.add_argument("--source", required=True)
    fetch.add_argument("--dataset", required=True)
    fetch.add_argument("--version", required=True)
    fetch.add_argument("--version-date")
    fetch.add_argument("--download-date")
    fetch.add_argument("--homepage")
    fetch.add_argument("--url", action="append", required=True, help="Source URL. Repeat for multi-file datasets.")
    fetch.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch.add_argument("--minio-credentials", type=Path)
    fetch.add_argument(
        "--bucket",
        default=DEFAULT_REGISTRY_BUCKET,
        help=f"MinIO bucket for registry objects. Defaults to {DEFAULT_REGISTRY_BUCKET}.",
    )
    fetch.add_argument("--upload", action="store_true")
    fetch.add_argument("--timeout", type=int, default=60)
    fetch.set_defaults(func=fetch_url)

    fetch_ctd = subparsers.add_parser("fetch-ctd", help="Download and register the CTD curated genes-diseases source snapshot.")
    fetch_ctd.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_ctd.add_argument("--minio-credentials", type=Path)
    fetch_ctd.add_argument("--upload", action="store_true")
    fetch_ctd.add_argument("--timeout", type=int, default=60)
    fetch_ctd.set_defaults(func=fetch_ctd_curated_genes_diseases)

    fetch_hcop = subparsers.add_parser("fetch-hcop", help="Download and register the HCOP human ortholog source snapshot.")
    fetch_hcop.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_hcop.add_argument("--minio-credentials", type=Path)
    fetch_hcop.add_argument("--upload", action="store_true")
    fetch_hcop.add_argument("--timeout", type=int, default=60)
    fetch_hcop.set_defaults(func=fetch_hcop_human_all)

    fetch_impc = subparsers.add_parser("fetch-impc", help="Download and register the IMPC genotype-phenotype assertions source snapshot.")
    fetch_impc.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_impc.add_argument("--minio-credentials", type=Path)
    fetch_impc.add_argument("--upload", action="store_true")
    fetch_impc.add_argument("--timeout", type=int, default=60)
    fetch_impc.set_defaults(func=fetch_impc_genotype_phenotype_assertions)

    fetch_mgi_hmd = subparsers.add_parser("fetch-mgi-hmd", help="Download and register the MGI HMD human phenotype source snapshot.")
    fetch_mgi_hmd.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_mgi_hmd.add_argument("--minio-credentials", type=Path)
    fetch_mgi_hmd.add_argument("--upload", action="store_true")
    fetch_mgi_hmd.add_argument("--timeout", type=int, default=60)
    fetch_mgi_hmd.set_defaults(func=fetch_mgi_hmd_human_phenotype)

    fetch_mp = subparsers.add_parser("fetch-mp", help="Download and register the MP ontology source snapshot.")
    fetch_mp.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_mp.add_argument("--minio-credentials", type=Path)
    fetch_mp.add_argument("--upload", action="store_true")
    fetch_mp.add_argument("--timeout", type=int, default=60)
    fetch_mp.set_defaults(func=fetch_mp_ontology)

    fetch_jensenlab_tissues = subparsers.add_parser("fetch-jensenlab-tissues", help="Download and register the JensenLab tissues source snapshot.")
    fetch_jensenlab_tissues.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_jensenlab_tissues.add_argument("--minio-credentials", type=Path)
    fetch_jensenlab_tissues.add_argument("--upload", action="store_true")
    fetch_jensenlab_tissues.add_argument("--timeout", type=int, default=60)
    fetch_jensenlab_tissues.set_defaults(func=fetch_jensenlab_tissues_snapshot)

    fetch_jensenlab_diseases = subparsers.add_parser("fetch-jensenlab-diseases", help="Download and register the JensenLab diseases source snapshot.")
    fetch_jensenlab_diseases.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_jensenlab_diseases.add_argument("--minio-credentials", type=Path)
    fetch_jensenlab_diseases.add_argument("--upload", action="store_true")
    fetch_jensenlab_diseases.add_argument("--timeout", type=int, default=60)
    fetch_jensenlab_diseases.set_defaults(func=fetch_jensenlab_diseases_snapshot)

    fetch_jensenlab_tinx = subparsers.add_parser("fetch-jensenlab-tinx", help="Download and register the JensenLab TIN-X source snapshot.")
    fetch_jensenlab_tinx.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_jensenlab_tinx.add_argument("--minio-credentials", type=Path)
    fetch_jensenlab_tinx.add_argument("--upload", action="store_true")
    fetch_jensenlab_tinx.add_argument("--timeout", type=int, default=60)
    fetch_jensenlab_tinx.set_defaults(func=fetch_jensenlab_tinx_snapshot)

    fetch_ncbi_publications = subparsers.add_parser("fetch-ncbi-publications", help="Download and register the NCBI publication source snapshot.")
    fetch_ncbi_publications.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_ncbi_publications.add_argument("--minio-credentials", type=Path)
    fetch_ncbi_publications.add_argument("--upload", action="store_true")
    fetch_ncbi_publications.add_argument("--timeout", type=int, default=60)
    fetch_ncbi_publications.set_defaults(func=fetch_ncbi_publications_snapshot)

    fetch_ncbi_gene_summary = subparsers.add_parser("fetch-ncbi-gene-summary", help="Download and register the NCBI gene summary source snapshot.")
    fetch_ncbi_gene_summary.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_ncbi_gene_summary.add_argument("--minio-credentials", type=Path)
    fetch_ncbi_gene_summary.add_argument("--upload", action="store_true")
    fetch_ncbi_gene_summary.add_argument("--timeout", type=int, default=60)
    fetch_ncbi_gene_summary.set_defaults(func=fetch_ncbi_gene_summary_snapshot)

    fetch_pubtator = subparsers.add_parser("fetch-pubtator", help="Download and register the PubTator gene2pubtator3 source snapshot.")
    fetch_pubtator.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_pubtator.add_argument("--minio-credentials", type=Path)
    fetch_pubtator.add_argument("--upload", action="store_true")
    fetch_pubtator.add_argument("--timeout", type=int, default=60)
    fetch_pubtator.set_defaults(func=fetch_pubtator_snapshot)

    fetch_tiga = subparsers.add_parser("fetch-tiga", help="Download and register the TIGA gene-trait source snapshot.")
    fetch_tiga.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_tiga.add_argument("--minio-credentials", type=Path)
    fetch_tiga.add_argument("--upload", action="store_true")
    fetch_tiga.add_argument("--timeout", type=int, default=60)
    fetch_tiga.set_defaults(func=fetch_tiga_snapshot)

    fetch_glygen = subparsers.add_parser("fetch-linkout-glygen", help="Download and register the GlyGen Pharos linkout source snapshot.")
    fetch_glygen.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_glygen.add_argument("--minio-credentials", type=Path)
    fetch_glygen.add_argument("--upload", action="store_true")
    fetch_glygen.add_argument("--timeout", type=int, default=120)
    fetch_glygen.set_defaults(func=fetch_linkout_glygen_snapshot)

    fetch_dark_kinome = subparsers.add_parser("fetch-linkout-dark-kinome", help="Download and register the Dark Kinome Pharos linkout source snapshot.")
    fetch_dark_kinome.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_dark_kinome.add_argument("--minio-credentials", type=Path)
    fetch_dark_kinome.add_argument("--upload", action="store_true")
    fetch_dark_kinome.add_argument("--timeout", type=int, default=120)
    fetch_dark_kinome.set_defaults(func=fetch_linkout_dark_kinome_snapshot)

    fetch_resolute = subparsers.add_parser("fetch-linkout-resolute", help="Download and register the RESOLUTE Pharos linkout source snapshot.")
    fetch_resolute.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_resolute.add_argument("--minio-credentials", type=Path)
    fetch_resolute.add_argument("--upload", action="store_true")
    fetch_resolute.add_argument("--timeout", type=int, default=120)
    fetch_resolute.set_defaults(func=fetch_linkout_resolute_snapshot)

    fetch_linkedomics = subparsers.add_parser("fetch-linkout-linkedomics", help="Download and register the LinkedOmicsKB Pharos linkout source snapshot.")
    fetch_linkedomics.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_linkedomics.add_argument("--minio-credentials", type=Path)
    fetch_linkedomics.add_argument("--upload", action="store_true")
    fetch_linkedomics.add_argument("--timeout", type=int, default=120)
    fetch_linkedomics.set_defaults(func=fetch_linkout_linkedomics_snapshot)

    fetch_uberon = subparsers.add_parser("fetch-uberon", help="Download and register the Uberon ontology source snapshot.")
    fetch_uberon.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_uberon.add_argument("--minio-credentials", type=Path)
    fetch_uberon.add_argument("--upload", action="store_true")
    fetch_uberon.add_argument("--timeout", type=int, default=60)
    fetch_uberon.set_defaults(func=fetch_uberon_snapshot)

    fetch_go = subparsers.add_parser("fetch-go", help="Download and register the GO basic ontology source snapshot.")
    fetch_go.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_go.add_argument("--minio-credentials", type=Path)
    fetch_go.add_argument("--upload", action="store_true")
    fetch_go.add_argument("--timeout", type=int, default=60)
    fetch_go.set_defaults(func=fetch_go_snapshot)

    fetch_goa_uniprot = subparsers.add_parser("fetch-goa-human-uniprot", help="Download and register the EBI GOA human UniProt annotation snapshot.")
    fetch_goa_uniprot.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_goa_uniprot.add_argument("--minio-credentials", type=Path)
    fetch_goa_uniprot.add_argument("--upload", action="store_true")
    fetch_goa_uniprot.add_argument("--timeout", type=int, default=60)
    fetch_goa_uniprot.set_defaults(func=fetch_goa_human_uniprot_snapshot)

    fetch_goa_go = subparsers.add_parser("fetch-goa-human-go", help="Download and register the GO-hosted human annotation snapshot.")
    fetch_goa_go.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_goa_go.add_argument("--minio-credentials", type=Path)
    fetch_goa_go.add_argument("--upload", action="store_true")
    fetch_goa_go.add_argument("--timeout", type=int, default=60)
    fetch_goa_go.set_defaults(func=fetch_goa_human_go_snapshot)

    fetch_mondo = subparsers.add_parser("fetch-mondo", help="Download and register the MONDO ontology source snapshot.")
    fetch_mondo.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_mondo.add_argument("--minio-credentials", type=Path)
    fetch_mondo.add_argument("--upload", action="store_true")
    fetch_mondo.add_argument("--timeout", type=int, default=60)
    fetch_mondo.set_defaults(func=fetch_mondo_snapshot)

    fetch_doid = subparsers.add_parser("fetch-disease-ontology", help="Download and register the Disease Ontology source snapshot.")
    fetch_doid.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_doid.add_argument("--minio-credentials", type=Path)
    fetch_doid.add_argument("--upload", action="store_true")
    fetch_doid.add_argument("--timeout", type=int, default=60)
    fetch_doid.set_defaults(func=fetch_disease_ontology_snapshot)

    fetch_protein_counts = subparsers.add_parser("fetch-jensenlab-protein-counts", help="Download and register the JensenLab protein_counts source snapshot.")
    fetch_protein_counts.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_protein_counts.add_argument("--minio-credentials", type=Path)
    fetch_protein_counts.add_argument("--upload", action="store_true")
    fetch_protein_counts.add_argument("--timeout", type=int, default=60)
    fetch_protein_counts.set_defaults(func=fetch_jensenlab_protein_counts_snapshot)

    fetch_reactome = subparsers.add_parser("fetch-reactome", help="Download and register the Reactome pathway source snapshot.")
    fetch_reactome.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_reactome.add_argument("--minio-credentials", type=Path)
    fetch_reactome.add_argument("--upload", action="store_true")
    fetch_reactome.add_argument("--timeout", type=int, default=120)
    fetch_reactome.set_defaults(func=fetch_reactome_snapshot)

    fetch_pathwaycommons = subparsers.add_parser("fetch-pathwaycommons", help="Download and register the PathwayCommons source snapshot.")
    fetch_pathwaycommons.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_pathwaycommons.add_argument("--minio-credentials", type=Path)
    fetch_pathwaycommons.add_argument("--upload", action="store_true")
    fetch_pathwaycommons.add_argument("--timeout", type=int, default=120)
    fetch_pathwaycommons.set_defaults(func=fetch_pathwaycommons_snapshot)

    fetch_panther = subparsers.add_parser("fetch-panther-classes", help="Download and register the PANTHER protein classes source snapshot.")
    fetch_panther.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_panther.add_argument("--minio-credentials", type=Path)
    fetch_panther.add_argument("--upload", action="store_true")
    fetch_panther.add_argument("--timeout", type=int, default=120)
    fetch_panther.set_defaults(func=fetch_panther_classes_snapshot)

    fetch_wikipathways = subparsers.add_parser("fetch-wikipathways", help="Download and register the WikiPathways human GMT source snapshot.")
    fetch_wikipathways.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_wikipathways.add_argument("--minio-credentials", type=Path)
    fetch_wikipathways.add_argument("--upload", action="store_true")
    fetch_wikipathways.add_argument("--timeout", type=int, default=120)
    fetch_wikipathways.set_defaults(func=fetch_wikipathways_snapshot)

    fetch_iuphar = subparsers.add_parser("fetch-iuphar", help="Download and register the IUPHAR ligand/interactions source snapshot.")
    fetch_iuphar.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_iuphar.add_argument("--minio-credentials", type=Path)
    fetch_iuphar.add_argument("--upload", action="store_true")
    fetch_iuphar.add_argument("--timeout", type=int, default=120)
    fetch_iuphar.set_defaults(func=fetch_iuphar_snapshot)

    fetch_uniprot = subparsers.add_parser("fetch-uniprot", help="Download and register the UniProt human source snapshot.")
    fetch_uniprot.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_uniprot.add_argument("--minio-credentials", type=Path)
    fetch_uniprot.add_argument("--upload", action="store_true")
    fetch_uniprot.add_argument("--timeout", type=int, default=240)
    fetch_uniprot.set_defaults(func=fetch_uniprot_snapshot)

    fetch_bioplex = subparsers.add_parser("fetch-bioplex", help="Download and register the BioPlex PPI source snapshot.")
    fetch_bioplex.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_bioplex.add_argument("--minio-credentials", type=Path)
    fetch_bioplex.add_argument("--upload", action="store_true")
    fetch_bioplex.add_argument("--timeout", type=int, default=120)
    fetch_bioplex.set_defaults(func=fetch_bioplex_snapshot)

    fetch_string = subparsers.add_parser("fetch-string", help="Download and register the STRING human protein links source snapshot.")
    fetch_string.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_string.add_argument("--minio-credentials", type=Path)
    fetch_string.add_argument("--upload", action="store_true")
    fetch_string.add_argument("--timeout", type=int, default=240)
    fetch_string.set_defaults(func=fetch_string_snapshot)

    fetch_gtex = subparsers.add_parser("fetch-gtex", help="Download and register the GTEx v11 expression source snapshot.")
    fetch_gtex.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_gtex.add_argument("--minio-credentials", type=Path)
    fetch_gtex.add_argument("--upload", action="store_true")
    fetch_gtex.add_argument("--timeout", type=int, default=300)
    fetch_gtex.set_defaults(func=fetch_gtex_snapshot)

    fetch_hpa = subparsers.add_parser("fetch-hpa", help="Download and register the Human Protein Atlas tissue expression source snapshot.")
    fetch_hpa.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_hpa.add_argument("--minio-credentials", type=Path)
    fetch_hpa.add_argument("--upload", action="store_true")
    fetch_hpa.add_argument("--timeout", type=int, default=240)
    fetch_hpa.set_defaults(func=fetch_hpa_snapshot)

    fetch_surechembl = subparsers.add_parser("fetch-surechembl-patent-discovery", help="Download and register the SureChEMBL patent discovery source snapshot.")
    fetch_surechembl.add_argument("--dest", required=True, type=Path, help="Local cache/output directory for the snapshot.")
    fetch_surechembl.add_argument("--minio-credentials", type=Path)
    fetch_surechembl.add_argument("--upload", action="store_true")
    fetch_surechembl.add_argument("--timeout", type=int, default=300)
    fetch_surechembl.set_defaults(func=fetch_surechembl_patent_discovery_snapshot)

    verify = subparsers.add_parser("verify-cache", help="Verify local files against a manifest.")
    verify.add_argument("manifest", type=Path)
    verify.set_defaults(func=verify_cache)

    inspect = subparsers.add_parser("inspect", help="Print a source snapshot manifest summary.")
    inspect.add_argument("manifest", type=Path)
    inspect.set_defaults(func=inspect_manifest)

    return parser


def _upload_files(
    *,
    storage: MinioStorage,
    source: str,
    dataset: str,
    version: str,
    files: List[Path],
    manifest_path: Optional[Path] = None,
) -> List[str]:
    prefix = storage_prefix(source, dataset, version)
    uris = []
    for path in files:
        key = f"{prefix}/{path.name}"
        uris.append(storage.upload_file(path, key, default_content_type(path)))
    if manifest_path:
        storage.upload_file(manifest_path, f"{prefix}/{MANIFEST_FILENAME}", "application/x-yaml")
    return uris


def fetch_url(args: argparse.Namespace) -> None:
    if args.upload and not args.minio_credentials:
        raise ValueError("--minio-credentials is required with --upload")

    snapshot_dir = args.dest / args.source / args.dataset / args.version
    downloaded_files = []
    source_entries = []
    inferred_version_dates = []

    for url in args.url:
        local_path, metadata = download_url(url, snapshot_dir, timeout=args.timeout)
        downloaded_files.append(local_path)
        inferred_version_dates.append(metadata.get("version_date"))
        source_entries.append((url, local_path, metadata))

    storage = None
    bucket = None
    if args.upload:
        storage = MinioStorage(load_minio_credentials(args.minio_credentials), bucket=args.bucket)
        bucket = storage.bucket

    file_entries = []
    for url, local_path, metadata in source_entries:
        storage_uri = None
        if bucket:
            storage_uri = s3_uri(bucket, f"{storage_prefix(args.source, args.dataset, args.version)}/{local_path.name}")
        file_entries.append(
            file_entry(
                local_path=local_path,
                source_url=metadata.get("final_url") or url,
                storage_uri=storage_uri,
                content_type=metadata.get("content_type"),
            )
        )

    version_date = args.version_date or next((value for value in inferred_version_dates if value), None)
    manifest = build_source_snapshot_manifest(
        source=args.source,
        dataset=args.dataset,
        version=args.version,
        version_date=version_date,
        download_date=args.download_date,
        homepage=args.homepage,
        upstream_urls=args.url,
        files=file_entries,
    )
    manifest_path = snapshot_dir / MANIFEST_FILENAME
    write_manifest(manifest, manifest_path)

    if storage:
        _upload_files(
            storage=storage,
            source=args.source,
            dataset=args.dataset,
            version=args.version,
            files=downloaded_files,
            manifest_path=manifest_path,
        )

    print(f"Wrote {manifest_path}")
    print(f"Manifest sha256: {manifest_checksum(manifest_path)}")
    if args.upload:
        print(f"Uploaded snapshot to s3://{bucket}/{storage_prefix(args.source, args.dataset, args.version)}/")


def verify_cache(args: argparse.Namespace) -> None:
    verify_manifest_files(args.manifest)
    print(f"Verified {args.manifest}")


def inspect_manifest(args: argparse.Namespace) -> None:
    manifest = read_manifest(args.manifest)
    print(f"snapshot_id: {manifest.get('snapshot_id')}")
    print(f"source: {manifest.get('source')}")
    print(f"dataset: {manifest.get('dataset')}")
    print(f"version: {manifest.get('version')}")
    print(f"version_date: {manifest.get('version_date')}")
    print(f"download_date: {manifest.get('download_date')}")
    print(f"files: {len(manifest.get('files', []))}")


def fetch_ctd_curated_genes_diseases(args: argparse.Namespace) -> None:
    from src.registry.sources.ctd import register_curated_genes_diseases

    register_curated_genes_diseases(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_hcop_human_all(args: argparse.Namespace) -> None:
    from src.registry.sources.hcop import register_human_all_sixteen_column

    register_human_all_sixteen_column(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_impc_genotype_phenotype_assertions(args: argparse.Namespace) -> None:
    from src.registry.sources.impc import register_genotype_phenotype_assertions

    register_genotype_phenotype_assertions(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_mgi_hmd_human_phenotype(args: argparse.Namespace) -> None:
    from src.registry.sources.mgi import register_hmd_human_phenotype

    register_hmd_human_phenotype(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_mp_ontology(args: argparse.Namespace) -> None:
    from src.registry.sources.mp import register_mp_obo

    register_mp_obo(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_jensenlab_tissues_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.jensenlab import register_tissues

    register_tissues(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_jensenlab_diseases_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.jensenlab import register_diseases

    register_diseases(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_jensenlab_tinx_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.jensenlab import register_tinx

    register_tinx(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_ncbi_publications_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.ncbi import register_publications

    register_publications(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_ncbi_gene_summary_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.ncbi import register_gene_summary

    register_gene_summary(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_pubtator_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.pubtator import register_gene2pubtator3

    register_gene2pubtator3(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_tiga_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.tiga import register_gene_trait

    register_gene_trait(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_linkout_glygen_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.pharos_linkouts import register_glygen

    register_glygen(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_linkout_dark_kinome_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.pharos_linkouts import register_dark_kinome

    register_dark_kinome(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_linkout_resolute_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.pharos_linkouts import register_resolute

    register_resolute(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_linkout_linkedomics_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.pharos_linkouts import register_linkedomics

    register_linkedomics(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_uberon_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.ontologies import register_uberon

    register_uberon(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_go_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.ontologies import register_go_basic

    register_go_basic(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_goa_human_uniprot_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.ontologies import register_goa_human_uniprot

    register_goa_human_uniprot(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_goa_human_go_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.ontologies import register_goa_human_go

    register_goa_human_go(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_mondo_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.ontologies import register_mondo

    register_mondo(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_disease_ontology_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.ontologies import register_disease_ontology

    register_disease_ontology(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_jensenlab_protein_counts_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.jensenlab import register_protein_counts

    register_protein_counts(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_reactome_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.pathway_sources import register_reactome

    register_reactome(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_pathwaycommons_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.pathway_sources import register_pathwaycommons

    register_pathwaycommons(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_panther_classes_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.pathway_sources import register_panther_classes

    register_panther_classes(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_wikipathways_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.pathway_sources import register_wikipathways

    register_wikipathways(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_iuphar_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.remaining_pharos import register_iuphar

    register_iuphar(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_uniprot_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.remaining_pharos import register_uniprot

    register_uniprot(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_bioplex_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.remaining_pharos import register_bioplex

    register_bioplex(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_string_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.remaining_pharos import register_string

    register_string(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_gtex_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.remaining_pharos import register_gtex

    register_gtex(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_hpa_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.remaining_pharos import register_hpa

    register_hpa(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def fetch_surechembl_patent_discovery_snapshot(args: argparse.Namespace) -> None:
    from src.registry.sources.remaining_pharos import register_surechembl_patent_discovery

    register_surechembl_patent_discovery(
        dest=args.dest,
        minio_credentials=args.minio_credentials,
        upload=args.upload,
        timeout=args.timeout,
    )


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
