import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests

from src.registry.download import download_url
from src.registry.fetchers import SnapshotFile, SourceFunctionFetcher, SourceSnapshot
from src.registry.manifest import parse_http_date_to_iso
from src.registry.sources.common import fetch_multi_file_last_modified_snapshot


PANTHER_CURRENT_RELEASE_URL = "https://data.pantherdb.org/ftp/sequence_classifications/current_release/PANTHER_Sequence_Classification_files/"
REACTOME_VERSION_URL = "https://reactome.org/ContentService/data/database/version"
PATHWAYCOMMONS_DATASOURCES_URL = "https://download.baderlab.org/PathwayCommons/PC2/v14/datasources.txt"
WIKIPATHWAYS_GMT_LISTING_URL = "https://data.wikipathways.org/current/gmt/"


def latest_panther_version(timeout: int = 60) -> str:
    response = requests.get(PANTHER_CURRENT_RELEASE_URL, timeout=timeout)
    response.raise_for_status()
    versions = [
        tuple(int(part) for part in match.split("."))
        for match in re.findall(r"PTHR(\d+(?:\.\d+)*)_human", response.text)
    ]
    if not versions:
        raise ValueError("Could not parse latest PANTHER version from current_release listing")
    return ".".join(str(part) for part in sorted(set(versions))[-1])


def latest_reactome_version(timeout: int = 60) -> str:
    response = requests.get(REACTOME_VERSION_URL, timeout=timeout)
    response.raise_for_status()
    return response.text.strip()


def latest_pathwaycommons_version(timeout: int = 60) -> str:
    response = requests.get(PATHWAYCOMMONS_DATASOURCES_URL, timeout=timeout)
    response.raise_for_status()
    version, _, _ = parse_pathwaycommons_datasources_version(response.text)
    return version


def latest_wikipathways_human_gmt_version(timeout: int = 60) -> str:
    response = requests.get(WIKIPATHWAYS_GMT_LISTING_URL, timeout=timeout)
    response.raise_for_status()
    version, _ = parse_wikipathways_human_gmt_listing(response.text)
    return version


def parse_pathwaycommons_datasources_version(text: str) -> tuple[str, str, str]:
    match = re.search(r"PC version (\d+) (\d+ \w+ \d+)", text)
    if not match:
        raise ValueError("Could not parse PathwayCommons version from datasources.txt")
    version = match.group(1)
    version_date = datetime.strptime(match.group(2), "%d %b %Y").date().isoformat()
    return version, version_date, match.group(0)


def parse_wikipathways_human_gmt_listing(text: str) -> tuple[str, str]:
    match = re.search(r"wikipathways-(\d{4})(\d{2})(\d{2})-gmt-Homo_sapiens\.gmt", text)
    if not match:
        raise ValueError("Could not find WikiPathways Homo sapiens GMT file")
    year, month, day = match.groups()
    return f"{year}-{month}-{day}", match.group(0)


def _build_snapshot(
    *,
    source: str,
    dataset: str,
    version: str,
    version_date: Optional[str],
    homepage: str,
    urls: List[str],
    files: List[tuple[Path, Dict[str, Optional[str]], str]],
    dest: Path,
    version_method: Dict,
) -> SourceSnapshot:
    return SourceSnapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        homepage=homepage,
        upstream_urls=urls,
        files=[
            SnapshotFile(local_path, metadata.get("final_url") or url, metadata.get("content_type"))
            for local_path, metadata, url in files
        ],
        extra={"version_method": version_method},
    )


def fetch_reactome(
    *,
    dest: Path,
    timeout: int = 60,
) -> SourceSnapshot:
    source = "reactome"
    dataset = "pathways"
    urls = [
        "https://reactome.org/download/current/ReactomePathways.gmt.zip",
        "https://reactome.org/download/current/ReactomePathwaysRelation.txt",
        "https://reactome.org/download/current/UniProt2Reactome_All_Levels.txt",
        "https://reactome.org/download/current/interactors/reactome.homo_sapiens.interactions.tab-delimited.txt",
    ]
    version_url = REACTOME_VERSION_URL
    version = latest_reactome_version(timeout=timeout)

    work_dir = dest / source / dataset / "pending"
    downloaded = [(*download_url(url, work_dir, timeout=timeout), url) for url in urls]
    interactor_metadata = downloaded[-1][1]
    version_date = interactor_metadata.get("version_date")
    if not version_date:
        raise ValueError("Could not determine Reactome version_date from interactor Last-Modified header")

    return _build_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        homepage="https://reactome.org/",
        urls=[*urls, version_url],
        files=downloaded,
        dest=dest,
        version_method={
            "type": "reactome_database_version",
            "description": "Use Reactome ContentService database version as version and interactor file Last-Modified as version_date.",
            "evidence": {
                "version_url": version_url,
                "interactor_last_modified": interactor_metadata.get("last_modified"),
            },
        },
    )


def fetch_pathwaycommons(
    *,
    dest: Path,
    timeout: int = 60,
) -> SourceSnapshot:
    source = "pathwaycommons"
    dataset = "pc_hgnc"
    file_url = "https://download.baderlab.org/PathwayCommons/PC2/v14/pc-hgnc.gmt.gz"
    datasources_url = PATHWAYCOMMONS_DATASOURCES_URL
    response = requests.get(datasources_url, timeout=timeout)
    response.raise_for_status()
    version, version_date, matched_text = parse_pathwaycommons_datasources_version(response.text)

    work_dir = dest / source / dataset / "pending"
    downloaded = [(*download_url(file_url, work_dir, timeout=timeout), file_url)]
    return _build_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        homepage="https://www.pathwaycommons.org/",
        urls=[file_url, datasources_url],
        files=downloaded,
        dest=dest,
        version_method={
            "type": "pathwaycommons_datasources_txt",
            "description": "Parse PC version and release date from datasources.txt.",
            "evidence": {"datasources_url": datasources_url, "matched_text": matched_text},
        },
    )


def fetch_panther_classes(
    *,
    dest: Path,
    timeout: int = 60,
) -> SourceSnapshot:
    version = latest_panther_version(timeout=timeout)
    return fetch_multi_file_last_modified_snapshot(
        source="panther",
        dataset="protein_classes",
        urls=[
            f"https://data.pantherdb.org/PANTHER{version}/ontology/Protein_Class_{version}",
            f"https://data.pantherdb.org/PANTHER{version}/ontology/Protein_class_relationship",
            f"{PANTHER_CURRENT_RELEASE_URL}PTHR{version}_human",
        ],
        dest=dest,
        homepage="https://pantherdb.org/",
        timeout=timeout,
        version=version,
        version_description="Parse PANTHER current_release sequence-classification listing as version and use max Last-Modified across class, relationship, and human sequence-classification files as version_date.",
    )


def fetch_wikipathways(
    *,
    dest: Path,
    timeout: int = 60,
) -> SourceSnapshot:
    source = "wikipathways"
    dataset = "human_gmt"
    listing_url = WIKIPATHWAYS_GMT_LISTING_URL
    response = requests.get(listing_url, timeout=timeout)
    response.raise_for_status()
    version, file_name = parse_wikipathways_human_gmt_listing(response.text)
    file_url = f"{listing_url}{file_name}"

    work_dir = dest / source / dataset / "pending"
    downloaded = [(*download_url(file_url, work_dir, timeout=timeout), file_url)]
    return _build_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version,
        homepage="https://www.wikipathways.org/",
        urls=[listing_url, file_url],
        files=downloaded,
        dest=dest,
        version_method={
            "type": "wikipathways_filename_date",
            "description": "Scrape current GMT directory and use the YYYYMMDD date embedded in the Homo sapiens GMT filename.",
            "evidence": {"listing_url": listing_url, "file_name": file_name},
        },
    )


class ReactomePathwaysFetcher(SourceFunctionFetcher):
    source = "reactome"
    dataset = "pathways"
    fetch_function = staticmethod(fetch_reactome)
    latest_version_function = staticmethod(latest_reactome_version)


class PathwaycommonsPathwaysFetcher(SourceFunctionFetcher):
    source = "pathwaycommons"
    dataset = "pc_hgnc"
    fetch_function = staticmethod(fetch_pathwaycommons)
    latest_version_function = staticmethod(latest_pathwaycommons_version)


class PantherClassesFetcher(SourceFunctionFetcher):
    source = "panther"
    dataset = "protein_classes"
    fetch_function = staticmethod(fetch_panther_classes)
    latest_version_function = staticmethod(latest_panther_version)


class WikipathwaysHumanGmtFetcher(SourceFunctionFetcher):
    source = "wikipathways"
    dataset = "human_gmt"
    fetch_function = staticmethod(fetch_wikipathways)
    latest_version_function = staticmethod(latest_wikipathways_human_gmt_version)
