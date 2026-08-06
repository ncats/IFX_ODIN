#!/usr/bin/env python3
"""Build the QA Browser target harmonizer app graph bundle from TargetGraph IDs."""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable


TARGET_NODE_COLUMNS = [
    "target_id",
    "target_type",
    "primary_id",
    "symbol",
    "name",
    "description",
    "category",
    "ids",
    "id_namespaces",
    "source_namespaces",
    "mapping_ratio",
    "quality_note",
    "canonical_status",
    "canonical_ifx_id",
    "source_annotations",
    "createdAt",
    "updatedAt",
]


def _clean(value: object) -> str:
    text = "" if value is None else str(value).strip()
    if text.lower() in {"", "nan", "none", "null"}:
        return ""
    return text


def _parts(value: object) -> list[str]:
    text = _clean(value)
    if not text:
        return []
    return [part.strip() for part in text.split("|") if _clean(part)]


def _clean_id(value: object) -> str:
    text = _clean(value)
    if text.endswith(".0") and text[:-2].replace(".", "", 1).isdigit():
        return text[:-2]
    return text


def _curie(prefix: str, value: object) -> str:
    text = _clean_id(value)
    if not text:
        return ""
    if ":" in text:
        return text
    return f"{prefix}:{text}"


def _add_ids(ids: list[str], values: Iterable[str]) -> None:
    seen = set(ids)
    for value in values:
        if value and value not in seen:
            ids.append(value)
            seen.add(value)


def _namespaces(ids: Iterable[str]) -> str:
    return "|".join(sorted({value.split(":", 1)[0] for value in ids if ":" in value}))


def _sources(row: dict[str, str], fields: Iterable[str]) -> str:
    found: list[str] = []
    seen: set[str] = set()
    for field in fields:
        for value in _clean(row.get(field)).replace(",", "|").split("|"):
            source = value.strip().lower()
            if source and source not in seen:
                found.append(source)
                seen.add(source)
    if (_clean(row.get("nodenorm_UMLS")) or _clean(row.get("NodeNorm_Protein"))) and "nodenorm" not in seen:
        found.append("nodenorm")
    return "|".join(found)


def _annotations(row: dict[str, str], skip: set[str]) -> str:
    values = {
        key: _clean(value)
        for key, value in row.items()
        if key not in skip and _clean(value)
    }
    return json.dumps(values, sort_keys=True, separators=(",", ":")) if values else ""


def _gene_row(row: dict[str, str]) -> dict[str, str]:
    ids: list[str] = []
    _add_ids(ids, [_curie("HGNC", row.get("consolidated_hgnc_id"))])
    _add_ids(ids, [_curie("NCBIGene", value) for value in _parts(row.get("consolidated_NCBI_id"))])
    _add_ids(ids, [_curie("ENSEMBL", row.get("consolidated_gene_id"))])
    _add_ids(ids, [_curie("MIM", row.get("consolidated_mim_id"))])
    _add_ids(ids, [_curie("UMLS", row.get("nodenorm_UMLS"))])
    primary_id = ids[0] if ids else _clean(row.get("ncats_gene_id"))
    skip = {
        "ncats_gene_id", "consolidated_gene_id", "consolidated_NCBI_id",
        "consolidated_hgnc_id", "consolidated_symbol", "consolidated_mim_id",
        "nodenorm_UMLS", "consolidated_description", "consolidated_gene_type",
        "Total_Mapping_Ratio", "createdAt", "updatedAt",
    }
    return {
        "target_id": _clean(row.get("ncats_gene_id")),
        "target_type": "gene",
        "primary_id": primary_id,
        "symbol": _clean(row.get("consolidated_symbol")),
        "name": _clean(row.get("consolidated_symbol")),
        "description": _clean(row.get("consolidated_description")),
        "category": _clean(row.get("consolidated_gene_type")),
        "ids": "|".join(ids),
        "id_namespaces": _namespaces(ids),
        "source_namespaces": _sources(row, [
            "Ensembl_ID_Provenance", "NCBI_ID_Provenance", "HGNC_ID_Provenance",
            "OMIM_ID_Provenance", "Symbol_Provenance", "Description_Provenance",
            "Location_Provenance",
        ]),
        "mapping_ratio": _clean(row.get("Total_Mapping_Ratio")),
        "quality_note": "",
        "canonical_status": "",
        "canonical_ifx_id": "",
        "source_annotations": _annotations(row, skip),
        "createdAt": _clean(row.get("createdAt")),
        "updatedAt": _clean(row.get("updatedAt")),
    }


def _protein_row(row: dict[str, str]) -> dict[str, str]:
    ids: list[str] = []
    _add_ids(ids, [_curie("UniProtKB", row.get("uniprot_id"))])
    _add_ids(ids, [_curie("UniProtKB", value) for value in _parts(row.get("uniprot_secondaryAccessions"))])
    _add_ids(ids, [_curie("ENSEMBL", value) for value in _parts(row.get("consolidated_ensembl_protein_id"))])
    _add_ids(ids, [_curie("ENSEMBL", value) for value in _parts(row.get("consolidated_ensembl_transcript_id"))])
    _add_ids(ids, [_curie("RefSeq", value) for value in _parts(row.get("consolidated_refseq_protein"))])
    _add_ids(ids, [_curie("NCBIGene", value) for value in _parts(row.get("uniprot_NCBI_id"))])
    _add_ids(ids, [_curie("UniRef100", row.get("uniref100_cluster_id"))])
    _add_ids(ids, [_curie("UniRef100", row.get("parent_uniref100_cluster_id"))])
    _add_ids(ids, [_curie("UMLS", row.get("nodenorm_UMLS"))])
    _add_ids(ids, [_curie("NodeNorm", row.get("NodeNorm_Protein"))])
    category = _clean(row.get("canonical_isoform_status"))
    if _clean(row.get("is_canonical")).lower() == "true":
        category = "canonical"
    quality_note = _clean(row.get("protein_grouping_reason"))
    if category == "canonical" and "reviewed" in _clean(row.get("uniprot_entryType")).lower():
        quality_note = "reviewed_representative"
    canonical_status = "|".join(value for value in [_clean(row.get("is_canonical")), _clean(row.get("canonical_isoform_status"))] if value)
    skip = {
        "ncats_protein_id", "uniprot_id", "consolidated_ensembl_protein_id",
        "consolidated_ensembl_transcript_id", "consolidated_refseq_protein",
        "consolidated_symbol", "combined_protein_name", "uniprot_entryType",
        "canonical_isoform_status", "protein_grouping_reason", "canonical_ifx_id",
        "uniref100_cluster_id", "parent_uniref100_cluster_id", "Total_Mapping_Ratio",
        "createdAt", "updatedAt",
    }
    return {
        "target_id": _clean(row.get("ncats_protein_id")),
        "target_type": "protein",
        "primary_id": ids[0] if ids else _clean(row.get("ncats_protein_id")),
        "symbol": _clean(row.get("consolidated_symbol")),
        "name": _clean(row.get("combined_protein_name")),
        "description": _clean(row.get("uniprot_entryType")),
        "category": category,
        "ids": "|".join(ids),
        "id_namespaces": _namespaces(ids),
        "source_namespaces": _sources(row, ["Ensembl_ID_Provenance", "RefSeq_ID_Provenance", "UniProt_ID_Provenance"]),
        "mapping_ratio": _clean(row.get("Total_Mapping_Ratio")),
        "quality_note": quality_note,
        "canonical_status": canonical_status,
        "canonical_ifx_id": _clean(row.get("canonical_ifx_id")),
        "source_annotations": _annotations(row, skip),
        "createdAt": _clean(row.get("createdAt")),
        "updatedAt": _clean(row.get("updatedAt")),
    }


def _transcript_row(row: dict[str, str]) -> dict[str, str]:
    ids: list[str] = []
    _add_ids(ids, [_curie("ENSEMBL", row.get("ensembl_transcript_id_version"))])
    _add_ids(ids, [_curie("ENSEMBL", row.get("ensembl_transcript_id"))])
    _add_ids(ids, [_curie("ENSEMBL", row.get("ensembl_gene_id"))])
    _add_ids(ids, [_curie("RefSeq", row.get("refseq_rna_id"))])
    _add_ids(ids, [_curie("NCBIGene", row.get("refseq_ncbi_id"))])
    canonical_status = "|".join(value for value in [_clean(row.get("ensembl_canonical")), _clean(row.get("ensembl_refseq_MANEselect"))] if value)
    transcript_id = _clean(row.get("ensembl_transcript_id"))
    skip = {
        "ncats_transcript_id", "ensembl_transcript_id_version", "ensembl_transcript_id",
        "ensembl_gene_id", "symbol", "ensembl_transcript_type", "refseq_rna_id",
        "refseq_ncbi_id", "refseq_status", "ensembl_canonical",
        "ensembl_refseq_MANEselect", "createdAt", "updatedAt",
    }
    return {
        "target_id": _clean(row.get("ncats_transcript_id")),
        "target_type": "transcript",
        "primary_id": ids[0] if ids else _clean(row.get("ncats_transcript_id")),
        "symbol": _clean(row.get("symbol")),
        "name": f"{_clean(row.get('symbol'))} {transcript_id}".strip(),
        "description": _clean(row.get("refseq_status")),
        "category": _clean(row.get("ensembl_transcript_type")),
        "ids": "|".join(ids),
        "id_namespaces": _namespaces(ids),
        "source_namespaces": _sources(row, ["Ensembl_Transcript_ID_Provenance", "RefSeq_Provenance"]),
        "mapping_ratio": "",
        "quality_note": "",
        "canonical_status": canonical_status,
        "canonical_ifx_id": "",
        "source_annotations": _annotations(row, skip),
        "createdAt": _clean(row.get("createdAt")),
        "updatedAt": _clean(row.get("updatedAt")),
    }


def _iter_rows(path: Path, mapper) -> Iterable[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            mapped = mapper(row)
            if mapped.get("target_id"):
                yield mapped


def _portable_path(path: Path) -> str:
    text = str(path)
    marker = "src/data/publicdata/target_data/"
    if marker in text:
        return text[text.index(marker):]
    return text


def build(target_data_dir: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    sources = {
        "gene_ids": target_data_dir / "gene_ids.tsv",
        "protein_ids": target_data_dir / "protein_ids.tsv",
        "transcript_ids": target_data_dir / "transcript_ids.tsv",
    }
    for path in sources.values():
        if not path.exists():
            raise FileNotFoundError(path)

    type_counts: Counter[str] = Counter()
    namespace_counts: Counter[str] = Counter()
    row_count = 0
    nodes_path = out_dir / "target_nodes.tsv"
    with nodes_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=TARGET_NODE_COLUMNS, delimiter="\t", extrasaction="ignore")
        writer.writeheader()
        for source_path, mapper in [
            (sources["gene_ids"], _gene_row),
            (sources["protein_ids"], _protein_row),
            (sources["transcript_ids"], _transcript_row),
        ]:
            for row in _iter_rows(source_path, mapper):
                writer.writerow(row)
                row_count += 1
                type_counts[row["target_type"]] += 1
                for namespace in _parts(row["id_namespaces"]):
                    namespace_counts[namespace] += 1

    manifest = {
        "contract_version": "1.0.0",
        "pipeline": "TargetGraph target harmonizer",
        "generated_at": datetime.now(UTC).isoformat(),
        "source_files": {key: _portable_path(path) for key, path in sources.items()},
        "files": {
            "target_nodes.tsv": {
                "rows": row_count,
                "key": "target_id",
                "columns": TARGET_NODE_COLUMNS,
            }
        },
        "node_counts": dict(type_counts),
        "identifier_namespace_concept_counts": dict(namespace_counts.most_common()),
    }
    with (out_dir / "manifest.json").open("w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
        fh.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-data-dir", required=True, type=Path, help="Directory containing gene_ids.tsv, protein_ids.tsv, transcript_ids.tsv")
    parser.add_argument("--out-dir", required=True, type=Path, help="Output target app graph directory")
    args = parser.parse_args()
    build(args.target_data_dir, args.out_dir)


if __name__ == "__main__":
    main()
