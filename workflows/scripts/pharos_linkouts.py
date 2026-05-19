#!/usr/bin/env python3
import argparse
import csv
import json
import os
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser


USER_AGENT = "IFX_ODIN pharos linkout discovery"


def _request(url, method="GET", payload=None, accept=None):
    data = None
    headers = {"User-Agent": USER_AGENT}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if accept:
        headers["Accept"] = accept

    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=120) as response:
        body = response.read()
        return body, response.headers


def _json_request(url, method="GET", payload=None):
    body, headers = _request(url, method=method, payload=payload, accept="application/json")
    return json.loads(body.decode("utf-8")), headers


def _write_version(path, source, source_url, record_count, version="", version_date=""):
    download_date = datetime.now(timezone.utc).date().isoformat()
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "source",
                "version",
                "version_date",
                "download_date",
                "source_url",
                "record_count",
            ],
            delimiter="\t",
        )
        writer.writeheader()
        writer.writerow(
            {
                "source": source,
                "version": version,
                "version_date": version_date,
                "download_date": download_date,
                "source_url": source_url,
                "record_count": record_count,
            }
        )


def _ensure_parent(path):
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _write_tsv(path, fieldnames, rows):
    _ensure_parent(path)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def download_glygen(args):
    search_url = "https://api.glygen.org/protein/search_simple/"
    list_url = "https://api.glygen.org/protein/list/"
    download_url = "https://api.glygen.org/data/list_download/"

    search_payload = {"term_category": "organism", "term": "human"}
    search_response, _ = _json_request(search_url, method="POST", payload=search_payload)
    list_id = search_response.get("list_id")
    if not list_id:
        raise SystemExit("GlyGen search response did not include list_id")

    list_response, _ = _json_request(list_url, method="POST", payload={"id": list_id})
    cache_info = list_response.get("cache_info") or {}
    listcache_id = cache_info.get("listcache_id") or list_response.get("listcache_id") or list_id

    download_payload = {
        "id": listcache_id,
        "download_type": "protein_list",
        "format": "csv",
        "compressed": False,
    }
    csv_body, _ = _request(download_url, method="POST", payload=download_payload, accept="text/csv")

    _ensure_parent(args.output)
    with open(args.output, "wb") as handle:
        handle.write(csv_body)

    record_count = search_response.get("resultcount")
    if record_count is None:
        record_count = max(0, sum(1 for _ in csv_body.decode("utf-8", "replace").splitlines()) - 1)

    _write_version(
        args.version,
        source="GlyGen",
        source_url=download_url,
        record_count=record_count,
        version=str(listcache_id),
    )


class DarkKinomeParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        attrs_dict = dict(attrs)
        href = attrs_dict.get("href", "")
        match = re.search(r"/kinase/([^/?#]+)", href)
        if not match:
            return
        symbol = urllib.parse.unquote(match.group(1)).strip()
        if symbol:
            self.rows.append(
                {
                    "symbol": symbol,
                    "url": f"https://darkkinome.org/kinase/{urllib.parse.quote(symbol)}",
                }
            )


def download_dark_kinome(args):
    url = "https://darkkinome.org/data"
    body, _ = _request(url, accept="text/html")
    html = body.decode("utf-8", "replace")

    parser = DarkKinomeParser()
    parser.feed(html)

    regex_rows = [
        {
            "symbol": urllib.parse.unquote(symbol).strip(),
            "url": f"https://darkkinome.org/kinase/{urllib.parse.quote(urllib.parse.unquote(symbol).strip())}",
        }
        for symbol in re.findall(r"""href=["'](?:https://darkkinome\.org)?/kinase/([^"'/\s?#]+)["']""", html)
        if urllib.parse.unquote(symbol).strip()
    ]

    seen = set()
    rows = []
    for row in [*parser.rows, *regex_rows]:
        if row["symbol"] in seen:
            continue
        seen.add(row["symbol"])
        rows.append(row)

    if not rows:
        raise SystemExit("No Dark Kinome kinase links found")

    _write_tsv(args.output, ["symbol", "url"], rows)
    _write_version(args.version, "Dark Kinome", url, len(rows))


def download_resolute(args):
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

    rows = []
    response, _ = _json_request(url, method="POST", payload={"query": query})
    if response.get("errors"):
        raise SystemExit(json.dumps(response["errors"], indent=2))
    genes = response.get("data", {}).get("genesList") or []
    for gene in genes:
        proteins = gene.get("proteinsList") or []
        nextprot_ids = []
        ensembl_protein_ids = []
        for protein in proteins:
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
                    "url": f"https://re-solute.eu/knowledgebase/gene/{urllib.parse.quote(symbol)}",
                }
            )

    if not rows:
        raise SystemExit("No RESOLUTE genes returned")

    _write_tsv(args.output, ["symbol", "nextprot_ids", "ensembl_protein_ids", "url"], rows)
    _write_version(args.version, "RESOLUTE", url, len(rows))


def download_linkedomics(args):
    url = "https://kb.linkedomics.org/data/list/gene"
    genes, _ = _json_request(url)
    if not isinstance(genes, list):
        raise SystemExit("LinkedOmicsKB gene list response was not a list")

    rows = [
        {
            "symbol": str(symbol),
            "url": f"https://kb.linkedomics.org/gene/{urllib.parse.quote(str(symbol))}",
        }
        for symbol in genes
        if str(symbol).strip()
    ]
    if not rows:
        raise SystemExit("No LinkedOmicsKB genes returned")

    _write_tsv(args.output, ["symbol", "url"], rows)
    _write_version(args.version, "LinkedOmicsKB", url, len(rows))


def main(argv=None):
    parser = argparse.ArgumentParser(description="Download Pharos linkout source lists")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name, func in [
        ("glygen", download_glygen),
        ("dark-kinome", download_dark_kinome),
        ("resolute", download_resolute),
        ("linkedomics", download_linkedomics),
    ]:
        sub = subparsers.add_parser(name)
        sub.add_argument("--output", required=True)
        sub.add_argument("--version", required=True)
        sub.set_defaults(func=func)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
