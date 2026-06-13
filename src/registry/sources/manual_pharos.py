import csv
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from src.registry.fetchers import SnapshotFile, SourceFetcher, SourceSnapshot


def _copy_manual_files(files: List[Path], final_dir: Path) -> List[Path]:
    final_dir.mkdir(parents=True, exist_ok=True)
    copied = []
    for source_path in files:
        if not source_path.exists():
            raise FileNotFoundError(source_path)
        final_path = final_dir / source_path.name
        if source_path.resolve() != final_path.resolve():
            shutil.copy2(source_path, final_path)
        copied.append(final_path)
    return copied


def _build_manual_snapshot(
    *,
    source: str,
    dataset: str,
    version: str,
    version_date: Optional[str],
    source_files: List[Path],
    dest: Path,
    homepage: Optional[str],
    extra: Dict,
) -> SourceSnapshot:
    copied_files = _copy_manual_files(source_files, dest)

    upstream_urls = []
    snapshot_files = []
    for original_path, copied_path in zip(source_files, copied_files):
        manual_uri = f"manual://{source}/{dataset}/{original_path.name}"
        upstream_urls.append(manual_uri)
        snapshot_files.append(SnapshotFile(copied_path, manual_uri))

    return SourceSnapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        downloaded_by="ifx-registry-manual",
        homepage=homepage,
        upstream_urls=upstream_urls,
        files=snapshot_files,
        extra=extra,
    )


def fetch_tdl_updates(
    *,
    dest: Path,
    source_file: Path = Path("input_files/manual/target_graph/tdl_updates.csv"),
) -> SourceSnapshot:
    modified_date = datetime.fromtimestamp(source_file.stat().st_mtime).date().isoformat()
    return _build_manual_snapshot(
        source="target_graph",
        dataset="tdl_updates",
        version=modified_date,
        version_date=modified_date,
        source_files=[source_file],
        dest=dest,
        homepage=None,
        extra={
            "version_method": {
                "type": "local_file_mtime",
                "description": "Manual Pharos TDL override file; use local file modification date as the snapshot version.",
            },
            "provenance": {
                "received_from": "Tudor Oprea",
                "original_location": str(source_file),
            },
        },
    )


def fetch_target_graph_manual_file(
    *,
    dest: Path,
    dataset: str,
    source_file: Path,
) -> SourceSnapshot:
    version_date = _local_file_mtime_date(source_file)
    return _build_manual_snapshot(
        source="target_graph",
        dataset=dataset,
        version=version_date,
        version_date=version_date,
        source_files=[source_file],
        dest=dest,
        homepage=None,
        extra={
            "version_method": {
                "type": "local_file_mtime",
                "description": (
                    "Manual target graph resolver input file; use local file modification "
                    "date as the snapshot version."
                ),
            },
            "provenance": {
                "status": "generated externally and curated manually for ODIN use",
                "original_location": "input_files/manual/target_graph",
                "notes": (
                    "These files are derived by code outside this repository. Treat them "
                    "as manual registry artifacts until that upstream generation workflow "
                    "is reproducible here."
                ),
            },
        },
    )


def _read_hpm_version(version_file: Path) -> Dict[str, Optional[str]]:
    with version_file.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        row = next(reader)
    return {"version": row.get("version"), "version_date": row.get("version_date")}


def fetch_hpm_protein_expression(
    *,
    dest: Path,
    matrix_file: Path = Path("input_files/manual/hpm/HPM_protein_level_expression_matrix_Kim_et_al_052914.csv"),
    version_file: Path = Path("input_files/manual/hpm/version.csv"),
) -> SourceSnapshot:
    hpm_version = _read_hpm_version(version_file)
    version_date = hpm_version["version_date"] or datetime.fromtimestamp(matrix_file.stat().st_mtime).date().isoformat()
    return _build_manual_snapshot(
        source="hpm",
        dataset="protein_expression",
        version=version_date,
        version_date=version_date,
        source_files=[matrix_file, version_file],
        dest=dest,
        homepage="https://www.proteinatlas.org/humanproteome/tissue",
        extra={
            "version_method": {
                "type": "companion_version_file",
                "description": "Use version_date from the local HPM version.csv companion file.",
                "evidence": hpm_version,
            },
            "provenance": {
                "original_location": str(matrix_file.parent),
            },
        },
    )


def fetch_antibodypedia_scraped_results(
    *,
    dest: Path,
    source_file: Path = Path("input_files/manual/antibodypedia/antibodypedia_scraped_results_2025-06-27_12-32.csv"),
) -> SourceSnapshot:
    timestamp = source_file.stem.removeprefix("antibodypedia_scraped_results_")
    version_date = timestamp.split("_", 1)[0]
    return _build_manual_snapshot(
        source="antibodypedia",
        dataset="scraped_results",
        version=timestamp,
        version_date=version_date,
        source_files=[source_file],
        dest=dest,
        homepage="https://www.antibodypedia.com/",
        extra={
            "version_method": {
                "type": "filename_timestamp",
                "description": "Use timestamp embedded in the scraped results filename.",
            },
            "provenance": {
                "original_location": str(source_file),
            },
        },
    )


def latest_tdl_updates_version(*, timeout: int = 60) -> Optional[str]:
    source_file = Path("input_files/manual/target_graph/tdl_updates.csv")
    return _local_file_mtime_date(source_file)


def _local_file_mtime_date(source_file: Path) -> str:
    return datetime.fromtimestamp(source_file.stat().st_mtime).date().isoformat()


def latest_target_graph_gene_ids_version(*, timeout: int = 60) -> Optional[str]:
    return _local_file_mtime_date(Path("input_files/manual/target_graph/gene_ids.tsv"))


def latest_target_graph_transcript_ids_version(*, timeout: int = 60) -> Optional[str]:
    return _local_file_mtime_date(Path("input_files/manual/target_graph/transcript_ids.tsv"))


def latest_target_graph_protein_ids_version(*, timeout: int = 60) -> Optional[str]:
    return _local_file_mtime_date(Path("input_files/manual/target_graph/protein_ids.tsv"))


def latest_target_graph_disease_ids_version(*, timeout: int = 60) -> Optional[str]:
    return _local_file_mtime_date(Path("input_files/manual/target_graph/disease_ids.tsv"))


def latest_target_graph_uniprot_mapping_version(*, timeout: int = 60) -> Optional[str]:
    return _local_file_mtime_date(Path("input_files/manual/target_graph/uniprotkb_mapping_20260507.csv"))


def latest_hpm_protein_expression_version(*, timeout: int = 60) -> Optional[str]:
    version_info = _read_hpm_version(Path("input_files/manual/hpm/version.csv"))
    return version_info["version_date"] or datetime.fromtimestamp(
        Path("input_files/manual/hpm/HPM_protein_level_expression_matrix_Kim_et_al_052914.csv").stat().st_mtime
    ).date().isoformat()


def latest_antibodypedia_scraped_results_version(*, timeout: int = 60) -> Optional[str]:
    source_file = Path("input_files/manual/antibodypedia/antibodypedia_scraped_results_2025-06-27_12-32.csv")
    timestamp = source_file.stem.removeprefix("antibodypedia_scraped_results_")
    return timestamp


class ManualTdlUpdatesFetcher(SourceFetcher):
    source = "target_graph"
    dataset = "tdl_updates"

    def fetch(self, *, dest: Path, timeout: int = 60) -> SourceSnapshot:
        return fetch_tdl_updates(dest=dest)

    def get_latest_version(self, *, timeout: int = 60) -> Optional[str]:
        return latest_tdl_updates_version(timeout=timeout)


class ManualTargetGraphGeneIdsFetcher(SourceFetcher):
    source = "target_graph"
    dataset = "gene_ids"

    def fetch(self, *, dest: Path, timeout: int = 60) -> SourceSnapshot:
        return fetch_target_graph_manual_file(
            dest=dest,
            dataset=self.dataset,
            source_file=Path("input_files/manual/target_graph/gene_ids.tsv"),
        )

    def get_latest_version(self, *, timeout: int = 60) -> Optional[str]:
        return latest_target_graph_gene_ids_version(timeout=timeout)


class ManualTargetGraphTranscriptIdsFetcher(SourceFetcher):
    source = "target_graph"
    dataset = "transcript_ids"

    def fetch(self, *, dest: Path, timeout: int = 60) -> SourceSnapshot:
        return fetch_target_graph_manual_file(
            dest=dest,
            dataset=self.dataset,
            source_file=Path("input_files/manual/target_graph/transcript_ids.tsv"),
        )

    def get_latest_version(self, *, timeout: int = 60) -> Optional[str]:
        return latest_target_graph_transcript_ids_version(timeout=timeout)


class ManualTargetGraphProteinIdsFetcher(SourceFetcher):
    source = "target_graph"
    dataset = "protein_ids"

    def fetch(self, *, dest: Path, timeout: int = 60) -> SourceSnapshot:
        return fetch_target_graph_manual_file(
            dest=dest,
            dataset=self.dataset,
            source_file=Path("input_files/manual/target_graph/protein_ids.tsv"),
        )

    def get_latest_version(self, *, timeout: int = 60) -> Optional[str]:
        return latest_target_graph_protein_ids_version(timeout=timeout)


class ManualTargetGraphDiseaseIdsFetcher(SourceFetcher):
    source = "target_graph"
    dataset = "disease_ids"

    def fetch(self, *, dest: Path, timeout: int = 60) -> SourceSnapshot:
        return fetch_target_graph_manual_file(
            dest=dest,
            dataset=self.dataset,
            source_file=Path("input_files/manual/target_graph/disease_ids.tsv"),
        )

    def get_latest_version(self, *, timeout: int = 60) -> Optional[str]:
        return latest_target_graph_disease_ids_version(timeout=timeout)


class ManualTargetGraphUniprotMappingFetcher(SourceFetcher):
    source = "target_graph"
    dataset = "uniprot_mapping"

    def fetch(self, *, dest: Path, timeout: int = 60) -> SourceSnapshot:
        return fetch_target_graph_manual_file(
            dest=dest,
            dataset=self.dataset,
            source_file=Path("input_files/manual/target_graph/uniprotkb_mapping_20260507.csv"),
        )

    def get_latest_version(self, *, timeout: int = 60) -> Optional[str]:
        return latest_target_graph_uniprot_mapping_version(timeout=timeout)


class ManualHpmProteinExpressionFetcher(SourceFetcher):
    source = "hpm"
    dataset = "protein_expression"

    def fetch(self, *, dest: Path, timeout: int = 60) -> SourceSnapshot:
        return fetch_hpm_protein_expression(dest=dest)

    def get_latest_version(self, *, timeout: int = 60) -> Optional[str]:
        return latest_hpm_protein_expression_version(timeout=timeout)


class ManualAntibodypediaScrapedResultsFetcher(SourceFetcher):
    source = "antibodypedia"
    dataset = "scraped_results"

    def fetch(self, *, dest: Path, timeout: int = 60) -> SourceSnapshot:
        return fetch_antibodypedia_scraped_results(dest=dest)

    def get_latest_version(self, *, timeout: int = 60) -> Optional[str]:
        return latest_antibodypedia_scraped_results_version(timeout=timeout)
