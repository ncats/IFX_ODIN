import csv
import json
import re
from datetime import timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote, unquote

import requests

from src.registry.manifest import (
    MANIFEST_FILENAME,
    build_source_snapshot_manifest,
    file_entry,
    iso_timestamp,
    manifest_checksum,
    storage_prefix,
    today_utc,
    write_manifest,
)
from src.registry.storage import MinioStorage, load_minio_credentials, s3_uri


USER_AGENT = "IFX_ODIN pharos linkout discovery"


def _headers(accept: Optional[str] = None) -> Dict[str, str]:
    headers = {"User-Agent": USER_AGENT}
    if accept:
        headers["Accept"] = accept
    return headers


def _post_json(url: str, payload: Dict[str, Any], timeout: int = 60, accept: Optional[str] = None) -> requests.Response:
    response = requests.post(url, json=payload, headers=_headers(accept), timeout=timeout)
    response.raise_for_status()
    return response


def _get(url: str, timeout: int = 60, accept: Optional[str] = None) -> requests.Response:
    response = requests.get(url, headers=_headers(accept), timeout=timeout)
    response.raise_for_status()
    return response


def _write_tsv(path: Path, fieldnames: List[str], rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def _write_bytes(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        handle.write(body)


def _upload_snapshot(
    *,
    source: str,
    dataset: str,
    version: str,
    version_date: Optional[str],
    homepage: str,
    upstream_urls: List[str],
    output_path: Path,
    source_url: str,
    content_type: str,
    minio_credentials: Optional[Path],
    upload: bool,
    extra: Dict[str, Any],
) -> Path:
    storage = None
    bucket = None
    if upload:
        if not minio_credentials:
            raise ValueError("minio_credentials is required when upload=True")
        storage = MinioStorage(load_minio_credentials(minio_credentials))
        bucket = storage.bucket

    object_prefix = storage_prefix(source, dataset, version)
    storage_uri = s3_uri(bucket, f"{object_prefix}/{output_path.name}") if bucket else None
    manifest = build_source_snapshot_manifest(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        download_date=None,
        homepage=homepage,
        upstream_urls=upstream_urls,
        files=[
            file_entry(
                local_path=output_path,
                source_url=source_url,
                storage_uri=storage_uri,
                content_type=content_type,
            )
        ],
        extra=extra,
    )
    manifest_path = output_path.parent / MANIFEST_FILENAME
    write_manifest(manifest, manifest_path)

    if storage:
        storage.upload_file(output_path, f"{object_prefix}/{output_path.name}", content_type)
        storage.upload_file(manifest_path, f"{object_prefix}/{MANIFEST_FILENAME}", "application/x-yaml")

    print(f"Wrote {manifest_path}")
    print(f"Manifest sha256: {manifest_checksum(manifest_path)}")
    if bucket:
        print(f"Uploaded snapshot to s3://{bucket}/{object_prefix}/")
    return manifest_path


def register_glygen(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    source = "glygen"
    dataset = "proteins"
    search_url = "https://api.glygen.org/protein/search_simple/"
    list_url = "https://api.glygen.org/protein/list/"
    download_url = "https://api.glygen.org/data/list_download/"

    search_payload = {"term_category": "organism", "term": "human"}
    search_response = _post_json(search_url, search_payload, timeout=timeout).json()
    list_id = search_response.get("list_id")
    if not list_id:
        raise ValueError("GlyGen search response did not include list_id")

    list_response = _post_json(list_url, {"id": list_id}, timeout=timeout).json()
    cache_info = list_response.get("cache_info") or {}
    listcache_id = cache_info.get("listcache_id") or list_response.get("listcache_id") or list_id

    download_payload = {
        "id": listcache_id,
        "download_type": "protein_list",
        "format": "csv",
        "compressed": False,
    }
    response = _post_json(download_url, download_payload, timeout=timeout, accept="text/csv")
    version = str(listcache_id)
    output_path = dest / source / dataset / version / "glygen_proteins.csv"
    _write_bytes(output_path, response.content)
    record_count = search_response.get("resultcount")
    if record_count is None:
        record_count = max(0, len(response.content.decode("utf-8", "replace").splitlines()) - 1)

    return _upload_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=None,
        homepage="https://glygen.org/",
        upstream_urls=[search_url, list_url, download_url],
        output_path=output_path,
        source_url=download_url,
        content_type="text/csv",
        minio_credentials=minio_credentials,
        upload=upload,
        extra={
            "record_count": record_count,
            "version_method": {
                "type": "glygen_listcache_id",
                "description": "Use GlyGen protein listcache_id returned by the API as the source snapshot version.",
                "evidence": {
                    "search_payload": search_payload,
                    "list_id": list_id,
                    "listcache_id": listcache_id,
                    "record_count": record_count,
                },
            },
        },
    )


class DarkKinomeParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows: List[Dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        if tag != "a":
            return
        href = dict(attrs).get("href", "") or ""
        match = re.search(r"/kinase/([^/?#]+)", href)
        if not match:
            return
        symbol = unquote(match.group(1)).strip()
        if symbol:
            self.rows.append({"symbol": symbol, "url": f"https://darkkinome.org/kinase/{quote(symbol)}"})


def register_dark_kinome(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    source = "dark_kinome"
    dataset = "kinases"
    url = "https://darkkinome.org/data"
    html = _get(url, timeout=timeout, accept="text/html").text

    parser = DarkKinomeParser()
    parser.feed(html)
    regex_rows = [
        {
            "symbol": unquote(symbol).strip(),
            "url": f"https://darkkinome.org/kinase/{quote(unquote(symbol).strip())}",
        }
        for symbol in re.findall(r"""href=["'](?:https://darkkinome\.org)?/kinase/([^"'/\s?#]+)["']""", html)
        if unquote(symbol).strip()
    ]

    seen = set()
    rows = []
    for row in [*parser.rows, *regex_rows]:
        if row["symbol"] in seen:
            continue
        seen.add(row["symbol"])
        rows.append(row)
    if not rows:
        raise ValueError("No Dark Kinome kinase links found")

    version = today_utc()
    output_path = dest / source / dataset / version / "dark_kinome_kinases.tsv"
    _write_tsv(output_path, ["symbol", "url"], rows)
    return _upload_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=None,
        homepage="https://darkkinome.org/",
        upstream_urls=[url],
        output_path=output_path,
        source_url=url,
        content_type="text/tab-separated-values",
        minio_credentials=minio_credentials,
        upload=upload,
        extra={
            "record_count": len(rows),
            "version_method": {
                "type": "download_date",
                "description": "Dark Kinome does not expose a stable release identifier; use registry download_date as snapshot version.",
                "evidence": {"record_count": len(rows)},
            },
        },
    )


def register_resolute(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    source = "resolute"
    dataset = "genes"
    url = "https://re-solute.eu/api/graphql"
    query = """
    query LinkoutGenes {
      genesList(first: 1000, condition: {isSlc: true}) {
        symbol
        proteinsList {
          nextprotac
          identifiersList {
            identifier
          }
        }
      }
    }
    """
    response = _post_json(url, {"query": query}, timeout=timeout).json()
    if response.get("errors"):
        raise ValueError(json.dumps(response["errors"], indent=2))

    rows = []
    for gene in response.get("data", {}).get("genesList") or []:
        nextprot_ids = []
        ensembl_protein_ids = []
        for protein in gene.get("proteinsList") or []:
            if protein.get("nextprotac"):
                nextprot_ids.append(protein["nextprotac"])
            for identifier in protein.get("identifiersList") or []:
                value = identifier.get("identifier")
                if value:
                    ensembl_protein_ids.append(value)
        symbol = gene.get("symbol")
        if symbol:
            rows.append(
                {
                    "symbol": symbol,
                    "nextprot_ids": "|".join(sorted(set(nextprot_ids))),
                    "ensembl_protein_ids": "|".join(sorted(set(ensembl_protein_ids))),
                    "url": f"https://re-solute.eu/knowledgebase/gene/{quote(symbol)}",
                }
            )
    if not rows:
        raise ValueError("No RESOLUTE genes returned")

    version = today_utc()
    output_path = dest / source / dataset / version / "resolute_genes.tsv"
    _write_tsv(output_path, ["symbol", "nextprot_ids", "ensembl_protein_ids", "url"], rows)
    return _upload_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=None,
        homepage="https://re-solute.eu/",
        upstream_urls=[url],
        output_path=output_path,
        source_url=url,
        content_type="text/tab-separated-values",
        minio_credentials=minio_credentials,
        upload=upload,
        extra={
            "record_count": len(rows),
            "version_method": {
                "type": "download_date",
                "description": "RESOLUTE GraphQL endpoint does not expose a stable release identifier; use registry download_date as snapshot version.",
                "evidence": {"record_count": len(rows), "query": query},
            },
        },
    )


def register_linkedomics(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    source = "linkedomics"
    dataset = "genes"
    url = "https://kb.linkedomics.org/data/list/gene"
    genes = _get(url, timeout=timeout, accept="application/json").json()
    if not isinstance(genes, list):
        raise ValueError("LinkedOmicsKB gene list response was not a list")
    rows = [
        {"symbol": str(symbol), "url": f"https://kb.linkedomics.org/gene/{quote(str(symbol))}"}
        for symbol in genes
        if str(symbol).strip()
    ]
    if not rows:
        raise ValueError("No LinkedOmicsKB genes returned")

    version = today_utc()
    output_path = dest / source / dataset / version / "linkedomics_genes.tsv"
    _write_tsv(output_path, ["symbol", "url"], rows)
    return _upload_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=None,
        homepage="https://kb.linkedomics.org/",
        upstream_urls=[url],
        output_path=output_path,
        source_url=url,
        content_type="text/tab-separated-values",
        minio_credentials=minio_credentials,
        upload=upload,
        extra={
            "record_count": len(rows),
            "version_method": {
                "type": "download_date",
                "description": "LinkedOmicsKB gene list endpoint does not expose a stable release identifier; use registry download_date as snapshot version.",
                "evidence": {"record_count": len(rows), "requested_at": iso_timestamp()},
            },
        },
    )
