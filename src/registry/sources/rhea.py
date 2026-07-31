from pathlib import Path

import requests

from src.registry.download import download_url
from src.registry.fetchers import SnapshotFile, SourceFunctionFetcher, SourceSnapshot


RHEA_RELEASE_PROPERTIES_URL = "https://ftp.expasy.org/databases/rhea/rhea-release.properties"
RHEA_REACTION_BUNDLE_URLS = [
    "https://ftp.expasy.org/databases/rhea/rdf/rhea.rdf.gz",
    "https://ftp.expasy.org/databases/rhea/tsv/rhea2uniprot_sprot.tsv",
    "https://ftp.expasy.org/databases/rhea/tsv/rhea2uniprot_trembl.tsv.gz",
    "https://ftp.expasy.org/databases/rhea/tsv/rhea2ec.tsv",
    "https://ftp.expasy.org/databases/rhea/tsv/rhea-directions.tsv",
]


def parse_rhea_release_properties(text: str) -> tuple[str, str]:
    values = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    version = values.get("rhea.release.number")
    version_date = values.get("rhea.release.date")
    if not version:
        raise ValueError("Could not parse rhea.release.number from Rhea release properties")
    if not version_date:
        raise ValueError("Could not parse rhea.release.date from Rhea release properties")
    return version, version_date


def fetch_rhea_release_info(timeout: int = 60) -> tuple[str, str, str]:
    response = requests.get(RHEA_RELEASE_PROPERTIES_URL, timeout=timeout)
    response.raise_for_status()
    version, version_date = parse_rhea_release_properties(response.text)
    return version, version_date, response.text


def latest_rhea_reaction_bundle_version(timeout: int = 60) -> str:
    version, _version_date, _text = fetch_rhea_release_info(timeout=timeout)
    return version


def fetch_rhea_reaction_bundle(
    *,
    dest: Path,
    timeout: int = 60,
) -> SourceSnapshot:
    source = "rhea"
    dataset = "reaction_bundle"
    version, version_date, _release_text = fetch_rhea_release_info(timeout=timeout)
    urls = [RHEA_RELEASE_PROPERTIES_URL, *RHEA_REACTION_BUNDLE_URLS]

    work_dir = dest / source / dataset / "pending"
    downloaded = [(*download_url(url, work_dir, timeout=timeout), url) for url in urls]
    evidence_files = [
        {
            "path": local_path.name,
            "url": metadata.get("final_url") or url,
            "last_modified": metadata.get("last_modified"),
        }
        for local_path, metadata, url in downloaded
    ]
    return SourceSnapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        homepage="https://www.rhea-db.org/",
        upstream_urls=urls,
        files=[
            SnapshotFile(local_path, metadata.get("final_url") or url, metadata.get("content_type"))
            for local_path, metadata, url in downloaded
        ],
        extra={
            "version_method": {
                "type": "rhea_release_properties",
                "description": "Use rhea.release.number and rhea.release.date from the Rhea release properties file.",
                "evidence": {
                    "release_properties_url": RHEA_RELEASE_PROPERTIES_URL,
                    "rhea_release_number": version,
                    "rhea_release_date": version_date,
                    "files": evidence_files,
                },
            }
        },
    )


class RheaReactionBundleFetcher(SourceFunctionFetcher):
    source = "rhea"
    dataset = "reaction_bundle"
    fetch_function = staticmethod(fetch_rhea_reaction_bundle)
    latest_version_function = staticmethod(latest_rhea_reaction_bundle_version)
