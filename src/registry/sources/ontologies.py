from pathlib import Path
import gzip
import json
import re
import tempfile

import requests

from src.registry.download import download_url
from src.registry.fetchers import SnapshotFile, SourceFunctionFetcher, SourceSnapshot
from src.registry.sources.common import latest_version_from_last_modified_urls, fetch_multi_file_last_modified_snapshot


UBERON_URLS = ["http://purl.obolibrary.org/obo/uberon.obo"]
GO_BASIC_URLS = ["https://current.geneontology.org/ontology/go-basic.json"]
EBI_GOA_HUMAN_UNIPROT_URLS = ["https://ftp.ebi.ac.uk/pub/databases/GO/goa/HUMAN/goa_human.gaf.gz"]
GOA_HUMAN_GO_URLS = ["https://current.geneontology.org/annotations/goa_human.gaf.gz"]
MONDO_URLS = ["https://purl.obolibrary.org/obo/mondo.json"]
DISEASE_ONTOLOGY_URL = "https://purl.obolibrary.org/obo/doid.json"
CHEBI_FULL_OBO_URL = "https://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.obo.gz"
CHEBI_ONTOLOGY_README_URL = "https://ftp.ebi.ac.uk/pub/databases/chebi/ontology/README"
CHEBI_HOMEPAGE = "https://www.ebi.ac.uk/chebi/"


def parse_chebi_readme_metadata(text: str) -> tuple[str, str]:
    release_match = re.search(r"ChEBI Release:\s*(\S+)", text)
    date_match = re.search(r"Date of last update:\s*(\d{4}-\d{2}-\d{2})", text)
    if not release_match:
        raise ValueError("Could not parse ChEBI release from ontology README")
    if not date_match:
        raise ValueError("Could not parse ChEBI update date from ontology README")
    return release_match.group(1), date_match.group(1)


def _open_text_maybe_gzip(path: Path):
    if path.name.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def extract_chebi_obo_metadata(path: Path) -> tuple[str, str]:
    data_version = None
    date_value = None
    with _open_text_maybe_gzip(path) as handle:
        for index, line in enumerate(handle):
            if index > 300:
                break
            if line.startswith("data-version:"):
                data_version = line.split(":", 1)[1].strip()
            elif line.startswith("date:"):
                raw_date = line.split(":", 1)[1].strip()
                date_match = re.match(r"(\d{2}):(\d{2}):(\d{4})", raw_date)
                if date_match:
                    day, month, year = date_match.groups()
                    date_value = f"{year}-{month}-{day}"
                else:
                    date_value = raw_date
            if data_version and date_value:
                return data_version, date_value
    if not data_version:
        raise ValueError(f"Could not parse ChEBI data-version from {path}")
    if not date_value:
        raise ValueError(f"Could not parse ChEBI date from {path}")
    return data_version, date_value


def extract_disease_ontology_version(payload: dict) -> str:
    graph_meta = ((payload.get("graphs") or [{}])[0].get("meta") or {})
    basic_values = graph_meta.get("basicPropertyValues") or []
    for entry in basic_values:
        if entry.get("pred") == "http://www.w3.org/2002/07/owl#versionInfo" and entry.get("val"):
            return entry["val"]
    version_uri = graph_meta.get("version")
    if version_uri:
        return version_uri.rstrip("/").rsplit("/", 2)[-2]
    raise ValueError("Could not determine Disease Ontology version from doid.json metadata")


def latest_disease_ontology_version(timeout: int = 60) -> str:
    with tempfile.TemporaryDirectory() as temp_dir:
        local_path, _ = download_url(DISEASE_ONTOLOGY_URL, Path(temp_dir), timeout=timeout, verbose=False)
        with local_path.open("r", encoding="utf-8") as handle:
            return extract_disease_ontology_version(json.load(handle))


def latest_chebi_full_ontology_version(timeout: int = 60) -> str:
    response = requests.get(CHEBI_ONTOLOGY_README_URL, timeout=timeout)
    response.raise_for_status()
    version, _ = parse_chebi_readme_metadata(response.text)
    return version


def fetch_uberon(
    *,
    dest: Path,
    timeout: int = 60,
) -> SourceSnapshot:
    return fetch_multi_file_last_modified_snapshot(
        source="uberon",
        dataset="ontology",
        urls=UBERON_URLS,
        dest=dest,
        homepage="https://obofoundry.org/ontology/uberon.html",
        timeout=timeout,
        version_description="Use the Uberon OBO HTTP Last-Modified header as version and version_date.",
    )


def fetch_go_basic(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    return fetch_multi_file_last_modified_snapshot(
        source="go",
        dataset="ontology",
        urls=GO_BASIC_URLS,
        dest=dest,
        homepage="https://geneontology.org/",
        timeout=timeout,
        version_description="Use the GO go-basic.json HTTP Last-Modified header as version and version_date.",
    )


def fetch_goa_human_uniprot(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    return fetch_multi_file_last_modified_snapshot(
        source="go",
        dataset="goa_human_uniprot",
        urls=EBI_GOA_HUMAN_UNIPROT_URLS,
        dest=dest,
        homepage="https://geneontology.org/",
        timeout=timeout,
        version_description="Use the EBI GOA human GAF HTTP Last-Modified header as version and version_date.",
    )


def fetch_goa_human_go(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    return fetch_multi_file_last_modified_snapshot(
        source="go",
        dataset="goa_human_go",
        urls=GOA_HUMAN_GO_URLS,
        dest=dest,
        homepage="https://geneontology.org/",
        timeout=timeout,
        version_description="Use the GO-hosted human GAF HTTP Last-Modified header as version and version_date.",
    )


def fetch_mondo(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    return fetch_multi_file_last_modified_snapshot(
        source="mondo",
        dataset="ontology",
        urls=MONDO_URLS,
        dest=dest,
        homepage="https://mondo.monarchinitiative.org/",
        timeout=timeout,
        version_description="Use the MONDO JSON HTTP Last-Modified header as version and version_date.",
    )


def fetch_disease_ontology(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    source = "disease_ontology"
    dataset = "ontology"
    url = DISEASE_ONTOLOGY_URL
    work_dir = dest / source / dataset / "pending"
    local_path, metadata = download_url(url, work_dir, timeout=timeout)

    with local_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    graph_meta = ((payload.get("graphs") or [{}])[0].get("meta") or {})
    version = extract_disease_ontology_version(payload)

    return SourceSnapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version,
        homepage="https://disease-ontology.org/",
        upstream_urls=[url],
        files=[SnapshotFile(local_path, metadata.get("final_url") or url, metadata.get("content_type"))],
        extra={
            "version_method": {
                "type": "owl_version_info",
                "description": "Use owl#versionInfo embedded in doid.json as version and version_date.",
                "evidence": {
                    "url": metadata.get("final_url") or url,
                    "graph_version": graph_meta.get("version"),
                    "last_modified": metadata.get("last_modified"),
                },
            }
        },
    )


def fetch_chebi_full_ontology(
    *,
    dest: Path,
    timeout: int = 60,
) -> SourceSnapshot:
    source = "chebi"
    dataset = "ontology_full"
    readme_response = requests.get(CHEBI_ONTOLOGY_README_URL, timeout=timeout)
    readme_response.raise_for_status()
    readme_version, readme_version_date = parse_chebi_readme_metadata(readme_response.text)

    work_dir = dest / source / dataset / "pending"
    local_path, metadata = download_url(CHEBI_FULL_OBO_URL, work_dir, timeout=timeout)
    obo_version, obo_date = extract_chebi_obo_metadata(local_path)
    if obo_version != readme_version:
        raise ValueError(
            f"ChEBI README release {readme_version} does not match OBO data-version {obo_version}"
        )

    return SourceSnapshot(
        source=source,
        dataset=dataset,
        version=readme_version,
        version_date=readme_version_date,
        homepage=CHEBI_HOMEPAGE,
        upstream_urls=[CHEBI_FULL_OBO_URL, CHEBI_ONTOLOGY_README_URL],
        files=[SnapshotFile(local_path, metadata.get("final_url") or CHEBI_FULL_OBO_URL, metadata.get("content_type"))],
        extra={
            "version_method": {
                "type": "chebi_release_and_obo_data_version",
                "description": "Use the ChEBI ontology README release as version and README update date as version_date; validate the downloaded FULL OBO data-version matches the release.",
                "evidence": {
                    "readme_url": CHEBI_ONTOLOGY_README_URL,
                    "readme_release": readme_version,
                    "readme_update_date": readme_version_date,
                    "obo_url": metadata.get("final_url") or CHEBI_FULL_OBO_URL,
                    "obo_data_version": obo_version,
                    "obo_date": obo_date,
                    "last_modified": metadata.get("last_modified"),
                },
            }
        },
    )


def latest_uberon_version(timeout: int = 60) -> str:
    return latest_version_from_last_modified_urls(UBERON_URLS, timeout=timeout)


def latest_go_basic_version(timeout: int = 60) -> str:
    return latest_version_from_last_modified_urls(GO_BASIC_URLS, timeout=timeout)


def latest_goa_human_uniprot_version(timeout: int = 60) -> str:
    return latest_version_from_last_modified_urls(EBI_GOA_HUMAN_UNIPROT_URLS, timeout=timeout)


def latest_goa_human_go_version(timeout: int = 60) -> str:
    return latest_version_from_last_modified_urls(GOA_HUMAN_GO_URLS, timeout=timeout)


def latest_mondo_version(timeout: int = 60) -> str:
    return latest_version_from_last_modified_urls(MONDO_URLS, timeout=timeout)


class UberonOntologyFetcher(SourceFunctionFetcher):
    source = "uberon"
    dataset = "ontology"
    fetch_function = staticmethod(fetch_uberon)
    latest_version_function = staticmethod(latest_uberon_version)


class GoBasicOntologyFetcher(SourceFunctionFetcher):
    source = "go"
    dataset = "ontology"
    fetch_function = staticmethod(fetch_go_basic)
    latest_version_function = staticmethod(latest_go_basic_version)


class GoaHumanGoFetcher(SourceFunctionFetcher):
    source = "go"
    dataset = "goa_human_go"
    fetch_function = staticmethod(fetch_goa_human_go)
    latest_version_function = staticmethod(latest_goa_human_go_version)


class EbiGoaHumanUniprotFetcher(SourceFunctionFetcher):
    source = "go"
    dataset = "goa_human_uniprot"
    fetch_function = staticmethod(fetch_goa_human_uniprot)
    latest_version_function = staticmethod(latest_goa_human_uniprot_version)


class MondoOntologyFetcher(SourceFunctionFetcher):
    source = "mondo"
    dataset = "ontology"
    fetch_function = staticmethod(fetch_mondo)
    latest_version_function = staticmethod(latest_mondo_version)


class DiseaseOntologyFetcher(SourceFunctionFetcher):
    source = "disease_ontology"
    dataset = "ontology"
    fetch_function = staticmethod(fetch_disease_ontology)
    latest_version_function = staticmethod(latest_disease_ontology_version)


class ChebiFullOntologyFetcher(SourceFunctionFetcher):
    source = "chebi"
    dataset = "ontology_full"
    fetch_function = staticmethod(fetch_chebi_full_ontology)
    latest_version_function = staticmethod(latest_chebi_full_ontology_version)
