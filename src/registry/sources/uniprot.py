from pathlib import Path
from datetime import datetime

import requests

from src.registry.fetchers import SourceFunctionFetcher
from src.registry.sources.snapshot_helpers import build_downloaded_snapshot, download_stream_without_head


UNIPROT_RELEASE_PROBE_URL = "https://rest.uniprot.org/uniprotkb/stream?compressed=false&format=json&size=1&query=accession:P04637"


def normalize_uniprot_release_date(value: str | None) -> str | None:
    if not value:
        return None
    text = value.strip()
    for date_format in ("%d-%B-%Y", "%d-%B-%y", "%d-%b-%Y", "%d-%b-%y"):
        try:
            return datetime.strptime(text, date_format).date().isoformat()
        except ValueError:
            pass
    return datetime.fromisoformat(text[:10]).date().isoformat()


def latest_uniprot_human_version(timeout: int = 60) -> str:
    response = requests.head(UNIPROT_RELEASE_PROBE_URL, timeout=timeout)
    response.raise_for_status()
    version = response.headers.get("x-uniprot-release")
    if not version:
        raise ValueError("UniProt response did not include x-uniprot-release")
    return version


def fetch_uniprot(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    source = "uniprot"
    dataset = "human"
    urls = [
        "https://rest.uniprot.org/uniprotkb/stream?compressed=true&format=json&query=(*)+AND+(model_organism:9606)",
        "https://rest.uniprot.org/uniprotkb/stream?compressed=true&format=json&query=(reviewed:true)+AND+(model_organism:9606)",
    ]
    release_probe_url = UNIPROT_RELEASE_PROBE_URL
    probe = requests.head(release_probe_url, timeout=timeout)
    probe.raise_for_status()
    version = probe.headers.get("x-uniprot-release")
    raw_version_date = probe.headers.get("x-uniprot-release-date")
    version_date = normalize_uniprot_release_date(raw_version_date)
    if not version:
        raise ValueError("UniProt response did not include x-uniprot-release")

    work_dir = dest / source / dataset / "pending"
    downloaded = [
        (*download_stream_without_head(urls[0], work_dir, "uniprot-human.json.gz", timeout), urls[0]),
        (*download_stream_without_head(urls[1], work_dir, "uniprot-human-reviewed.json.gz", timeout), urls[1]),
    ]
    return build_downloaded_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        homepage="https://www.uniprot.org/",
        urls=[*urls, release_probe_url],
        downloaded=downloaded,
        dest=dest,
        version_method={
            "type": "uniprot_release_headers",
            "description": "Use x-uniprot-release and x-uniprot-release-date headers from a small UniProt accession probe.",
            "evidence": {
                "probe_url": release_probe_url,
                "x_uniprot_release": version,
                "x_uniprot_release_date": raw_version_date,
                "normalized_version_date": version_date,
            },
        },
    )


class UniprotHumanFetcher(SourceFunctionFetcher):
    source = "uniprot"
    dataset = "human"
    fetch_function = staticmethod(fetch_uniprot)
    latest_version_function = staticmethod(latest_uniprot_human_version)
