from pathlib import Path
from datetime import datetime
import gzip

import ijson
import requests

from src.registry.fetchers import SourceFunctionFetcher, SourceSnapshot
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


def _scan_uniprot_accessions(path: Path) -> dict:
    primary_accessions = set()
    secondary_accessions = set()
    reviewed_primary_accessions = set()
    records = 0

    with gzip.open(path, "rb") as handle:
        for record in ijson.items(handle, "results.item"):
            records += 1
            primary_accession = record.get("primaryAccession")
            if primary_accession:
                primary_accessions.add(primary_accession)
            secondary_accessions.update(record.get("secondaryAccessions") or [])
            entry_type = (record.get("entryType") or "").lower()
            if (
                primary_accession
                and "reviewed" in entry_type
                and "unreviewed" not in entry_type
            ):
                reviewed_primary_accessions.add(primary_accession)

    return {
        "records": records,
        "primary_accessions": primary_accessions,
        "secondary_accessions": secondary_accessions,
        "reviewed_primary_accessions": reviewed_primary_accessions,
    }


def _validate_full_human_includes_reviewed(full_path: Path, reviewed_path: Path) -> dict:
    full = _scan_uniprot_accessions(full_path)
    reviewed = _scan_uniprot_accessions(reviewed_path)

    full_any_accessions = full["primary_accessions"] | full["secondary_accessions"]
    missing_reviewed_primary = sorted(
        reviewed["primary_accessions"] - full["primary_accessions"]
    )
    missing_reviewed_secondary = sorted(
        reviewed["secondary_accessions"] - full_any_accessions
    )
    stats = {
        "full_records": full["records"],
        "full_primary_accessions": len(full["primary_accessions"]),
        "full_secondary_accessions": len(full["secondary_accessions"]),
        "full_reviewed_primary_accessions": len(full["reviewed_primary_accessions"]),
        "reviewed_records": reviewed["records"],
        "reviewed_primary_accessions": len(reviewed["primary_accessions"]),
        "reviewed_secondary_accessions": len(reviewed["secondary_accessions"]),
        "missing_reviewed_primary_accessions": len(missing_reviewed_primary),
        "missing_reviewed_secondary_accessions": len(missing_reviewed_secondary),
    }

    if missing_reviewed_primary or missing_reviewed_secondary:
        sample_primary = ", ".join(missing_reviewed_primary[:20]) or "none"
        sample_secondary = ", ".join(missing_reviewed_secondary[:20]) or "none"
        raise ValueError(
            "UniProt full human download does not include every reviewed human accession: "
            f"{stats['missing_reviewed_primary_accessions']} reviewed primary accessions "
            f"missing from full primary accessions; "
            f"{stats['missing_reviewed_secondary_accessions']} reviewed secondary accessions "
            f"missing from full primary+secondary accessions. "
            f"Sample missing primary: {sample_primary}. "
            f"Sample missing secondary: {sample_secondary}."
        )

    return stats


def fetch_uniprot(
    *,
    dest: Path,
    timeout: int = 60,
) -> SourceSnapshot:
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
    inclusion_stats = _validate_full_human_includes_reviewed(
        downloaded[0][0],
        downloaded[1][0],
    )
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
        extra={"validation": {"reviewed_in_full": inclusion_stats}},
    )


class UniprotHumanFetcher(SourceFunctionFetcher):
    source = "uniprot"
    dataset = "human"
    fetch_function = staticmethod(fetch_uniprot)
    latest_version_function = staticmethod(latest_uniprot_human_version)
