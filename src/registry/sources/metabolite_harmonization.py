import shutil
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import requests

from src.registry.download import download_url, format_bytes, http_metadata
from src.registry.fetchers import SnapshotFile, SourceFetcher, SourceFunctionFetcher, SourceSnapshot
from src.registry.manifest import sha256_file, today_utc
from src.registry.sources.common import fetch_multi_file_last_modified_snapshot


WIKIPATHWAYS_RDF_LISTING_URL = "https://data.wikipathways.org/current/rdf/"
WIKIPATHWAYS_HOMEPAGE = "https://www.wikipathways.org/"
REFMET_URL = "https://www.metabolomicsworkbench.org/databases/refmet/refmet_download.php"
REFMET_HOMEPAGE = "https://www.metabolomicsworkbench.org/databases/refmet/browse.php"
LIPIDMAPS_URL = "https://www.lipidmaps.org/files/?file=LMSD&ext=sdf.zip"
LIPIDMAPS_HOMEPAGE = "https://www.lipidmaps.org/"
CHEBI_THREE_STAR_SDF_URL = "https://ftp.ebi.ac.uk/pub/databases/chebi/SDF/chebi_3_stars.sdf.gz"
CHEBI_HOMEPAGE = "https://www.ebi.ac.uk/chebi/"
HMDB_HOMEPAGE = "https://hmdb.ca/"
HMDB_VERSION = "5.0"
HMDB_VERSION_DATE = "2021-11-17"
MANUAL_HMDB_DIR = Path("input_files/manual/hmdb")


def parse_wikipathways_rdf_listing(text: str) -> Tuple[str, str]:
    import re

    match = re.search(r"wikipathways-(\d{4})(\d{2})(\d{2})-rdf-wp\.zip", text)
    if not match:
        raise ValueError("Could not find WikiPathways RDF wp zip in current RDF listing")
    year, month, day = match.groups()
    return f"{year}-{month}-{day}", match.group(0)


def latest_wikipathways_rdf_wp_version(timeout: int = 60) -> str:
    response = requests.get(WIKIPATHWAYS_RDF_LISTING_URL, timeout=timeout)
    response.raise_for_status()
    version, _file_name = parse_wikipathways_rdf_listing(response.text)
    return version


def fetch_wikipathways_rdf_wp(*, dest: Path, timeout: int = 60) -> SourceSnapshot:
    response = requests.get(WIKIPATHWAYS_RDF_LISTING_URL, timeout=timeout)
    response.raise_for_status()
    version, file_name = parse_wikipathways_rdf_listing(response.text)
    file_url = f"{WIKIPATHWAYS_RDF_LISTING_URL}{file_name}"
    work_dir = dest / "wikipathways" / "rdf_wp" / "pending"
    local_path, metadata = download_url(file_url, work_dir, timeout=timeout)
    return SourceSnapshot(
        source="wikipathways",
        dataset="rdf_wp",
        version=version,
        version_date=version,
        homepage=WIKIPATHWAYS_HOMEPAGE,
        upstream_urls=[WIKIPATHWAYS_RDF_LISTING_URL, file_url],
        files=[SnapshotFile(local_path, metadata.get("final_url") or file_url, metadata.get("content_type"))],
        extra={
            "version_method": {
                "type": "wikipathways_rdf_filename_date",
                "description": "Scrape the current RDF directory and use the YYYYMMDD date embedded in the rdf-wp zip filename.",
                "evidence": {
                    "listing_url": WIKIPATHWAYS_RDF_LISTING_URL,
                    "file_name": file_name,
                    "last_modified": metadata.get("last_modified"),
                },
            }
        },
    )


def _download_get_without_head(
    url: str,
    dest_dir: Path,
    *,
    file_name: str,
    timeout: int = 60,
) -> Tuple[Path, Dict[str, Optional[str]]]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / file_name
    with requests.get(url, stream=True, allow_redirects=True, timeout=timeout) as response:
        response.raise_for_status()
        content_length = response.headers.get("Content-Length")
        print(f"Downloading {file_name} ({format_bytes(content_length)}) from {response.url}", flush=True)
        with dest_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
        return dest_path, {
            "content_type": response.headers.get("Content-Type"),
            "content_length": response.headers.get("Content-Length"),
            "final_url": response.url,
            "last_modified": response.headers.get("Last-Modified"),
            "version_date": None,
        }


def _refmet_version_from_file(path: Path) -> str:
    return f"sha256-{sha256_file(path)[:12]}"


def latest_refmet_metabolites_csv_version(timeout: int = 60) -> str:
    with tempfile.TemporaryDirectory() as temp_dir:
        local_path, _metadata = _download_get_without_head(
            REFMET_URL,
            Path(temp_dir),
            file_name="refmet.csv",
            timeout=timeout,
        )
        return _refmet_version_from_file(local_path)


def fetch_refmet_metabolites_csv(*, dest: Path, timeout: int = 60) -> SourceSnapshot:
    work_dir = dest / "refmet" / "metabolites_csv" / "pending"
    local_path, metadata = _download_get_without_head(
        REFMET_URL,
        work_dir,
        file_name="refmet.csv",
        timeout=timeout,
    )
    version = _refmet_version_from_file(local_path)
    version_date = today_utc()
    return SourceSnapshot(
        source="refmet",
        dataset="metabolites_csv",
        version=version,
        version_date=version_date,
        homepage=REFMET_HOMEPAGE,
        upstream_urls=[REFMET_URL],
        files=[SnapshotFile(local_path, metadata.get("final_url") or REFMET_URL, metadata.get("content_type"))],
        extra={
            "version_method": {
                "type": "content_hash",
                "description": "RefMet direct download does not expose a stable version or Last-Modified header; use the downloaded CSV sha256 prefix as version and download date as version_date.",
                "evidence": {
                    "source_url": metadata.get("final_url") or REFMET_URL,
                    "content_length": metadata.get("content_length"),
                },
            }
        },
    )


def _lipidmaps_zip_inner_version(path: Path) -> str:
    with zipfile.ZipFile(path) as archive:
        info = archive.getinfo("structures.sdf")
    year, month, day, _hour, _minute, _second = info.date_time
    return datetime(year, month, day).date().isoformat()


def latest_lipidmaps_lmsd_sdf_version(timeout: int = 60) -> str:
    with tempfile.TemporaryDirectory() as temp_dir:
        local_path, _metadata = _download_get_without_head(
            LIPIDMAPS_URL,
            Path(temp_dir),
            file_name="LMSD.sdf.zip",
            timeout=timeout,
        )
        return _lipidmaps_zip_inner_version(local_path)


def fetch_lipidmaps_lmsd_sdf(*, dest: Path, timeout: int = 60) -> SourceSnapshot:
    work_dir = dest / "lipidmaps" / "lmsd_sdf" / "pending"
    local_path, metadata = _download_get_without_head(
        LIPIDMAPS_URL,
        work_dir,
        file_name="LMSD.sdf.zip",
        timeout=timeout,
    )
    version = _lipidmaps_zip_inner_version(local_path)
    return SourceSnapshot(
        source="lipidmaps",
        dataset="lmsd_sdf",
        version=version,
        version_date=version,
        homepage=LIPIDMAPS_HOMEPAGE,
        upstream_urls=[LIPIDMAPS_URL],
        files=[SnapshotFile(local_path, metadata.get("final_url") or LIPIDMAPS_URL, metadata.get("content_type"))],
        extra={
            "version_method": {
                "type": "zip_inner_file_timestamp",
                "description": "LIPID MAPS download does not provide reliable HEAD metadata; use the structures.sdf timestamp inside LMSD.sdf.zip.",
                "evidence": {
                    "source_url": metadata.get("final_url") or LIPIDMAPS_URL,
                    "inner_file": "structures.sdf",
                },
            }
        },
    )


def latest_chebi_three_star_sdf_version(timeout: int = 60) -> str:
    version_date = http_metadata(CHEBI_THREE_STAR_SDF_URL, timeout=timeout).get("version_date")
    if not version_date:
        raise ValueError("Could not determine ChEBI 3-star SDF version from Last-Modified header")
    return version_date


def fetch_chebi_three_star_sdf(*, dest: Path, timeout: int = 60) -> SourceSnapshot:
    return fetch_multi_file_last_modified_snapshot(
        source="chebi",
        dataset="three_star_sdf",
        urls=[CHEBI_THREE_STAR_SDF_URL],
        dest=dest / "chebi" / "three_star_sdf" / "pending",
        homepage=CHEBI_HOMEPAGE,
        timeout=timeout,
        version_description="Use the ChEBI 3-star SDF HTTP Last-Modified header as version and version_date.",
    )


def _manual_hmdb_snapshot(*, dataset: str, file_name: str, dest: Path) -> SourceSnapshot:
    source_file = MANUAL_HMDB_DIR / file_name
    if not source_file.exists():
        raise FileNotFoundError(
            f"{source_file} is required for manual HMDB registry snapshot creation"
        )
    dest.mkdir(parents=True, exist_ok=True)
    local_path = dest / source_file.name
    if source_file.resolve() != local_path.resolve():
        shutil.copy2(source_file, local_path)
    manual_uri = f"manual://hmdb/{dataset}/{source_file.name}"
    return SourceSnapshot(
        source="hmdb",
        dataset=dataset,
        version=HMDB_VERSION,
        version_date=HMDB_VERSION_DATE,
        downloaded_by="ifx-registry-manual",
        homepage=HMDB_HOMEPAGE,
        upstream_urls=[manual_uri],
        files=[SnapshotFile(local_path, manual_uri)],
        extra={
            "version_method": {
                "type": "manual_hmdb_release",
                "description": "HMDB download endpoints are Cloudflare-protected in automated environments; use the known HMDB 5.0 release metadata for manually supplied files.",
                "evidence": {
                    "hmdb_release": HMDB_VERSION,
                    "hmdb_release_date": HMDB_VERSION_DATE,
                    "expected_local_path": str(source_file),
                },
            }
        },
    )


class WikipathwaysRdfWpFetcher(SourceFunctionFetcher):
    source = "wikipathways"
    dataset = "rdf_wp"
    fetch_function = staticmethod(fetch_wikipathways_rdf_wp)
    latest_version_function = staticmethod(latest_wikipathways_rdf_wp_version)


class RefmetMetabolitesCsvFetcher(SourceFunctionFetcher):
    source = "refmet"
    dataset = "metabolites_csv"
    fetch_function = staticmethod(fetch_refmet_metabolites_csv)
    latest_version_function = staticmethod(latest_refmet_metabolites_csv_version)


class LipidmapsLmsdSdfFetcher(SourceFunctionFetcher):
    source = "lipidmaps"
    dataset = "lmsd_sdf"
    fetch_function = staticmethod(fetch_lipidmaps_lmsd_sdf)
    latest_version_function = staticmethod(latest_lipidmaps_lmsd_sdf_version)


class ChebiThreeStarSdfFetcher(SourceFunctionFetcher):
    source = "chebi"
    dataset = "three_star_sdf"
    fetch_function = staticmethod(fetch_chebi_three_star_sdf)
    latest_version_function = staticmethod(latest_chebi_three_star_sdf_version)


class ManualHmdbMetabolitesXmlFetcher(SourceFetcher):
    source = "hmdb"
    dataset = "metabolites_xml"

    def fetch(self, *, dest: Path, timeout: int = 60) -> SourceSnapshot:
        return _manual_hmdb_snapshot(
            dataset=self.dataset,
            file_name="hmdb_metabolites.zip",
            dest=dest / self.source / self.dataset / "pending",
        )

    def get_latest_version(self, *, timeout: int = 60) -> str:
        return HMDB_VERSION


class ManualHmdbStructuresSdfFetcher(SourceFetcher):
    source = "hmdb"
    dataset = "structures_sdf"

    def fetch(self, *, dest: Path, timeout: int = 60) -> SourceSnapshot:
        return _manual_hmdb_snapshot(
            dataset=self.dataset,
            file_name="structures.zip",
            dest=dest / self.source / self.dataset / "pending",
        )

    def get_latest_version(self, *, timeout: int = 60) -> str:
        return HMDB_VERSION
