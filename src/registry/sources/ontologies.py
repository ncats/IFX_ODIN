from pathlib import Path
import json
import tempfile

from src.registry.download import download_url
from src.registry.fetchers import SnapshotFile, SourceFunctionFetcher, SourceSnapshot
from src.registry.sources.common import latest_version_from_last_modified_urls, fetch_multi_file_last_modified_snapshot


UBERON_URLS = ["http://purl.obolibrary.org/obo/uberon.obo"]
GO_BASIC_URLS = ["https://current.geneontology.org/ontology/go-basic.json"]
EBI_GOA_HUMAN_UNIPROT_URLS = ["https://ftp.ebi.ac.uk/pub/databases/GO/goa/HUMAN/goa_human.gaf.gz"]
GOA_HUMAN_GO_URLS = ["https://current.geneontology.org/annotations/goa_human.gaf.gz"]
MONDO_URLS = ["https://purl.obolibrary.org/obo/mondo.json"]
DISEASE_ONTOLOGY_URL = "https://purl.obolibrary.org/obo/doid.json"


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
