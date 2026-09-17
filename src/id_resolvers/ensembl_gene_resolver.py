"""Resolve human gene identifiers from a pinned Ensembl Registry snapshot."""

from __future__ import annotations

import csv
from collections.abc import Generator
from typing import Any

from src.constants import Prefix
from src.id_resolvers.sqlite_cache_resolver import MatchingPair, SqliteCacheResolver
from src.models.node import EquivalentId

ENSEMBL_GENE_FILE = "gene_transcript_identifiers.csv"
RESOLVER_CONTRACT_VERSION = "1"


class EnsemblGeneResolver(SqliteCacheResolver):
    """Human ENSG resolver backed only by an exact Registry data source."""

    name = "Ensembl Gene Resolver"

    def __init__(self, data_source, **kwargs):
        self.data_source = data_source
        self.data_file = data_source.file(ENSEMBL_GENE_FILE)
        super().__init__(**kwargs)

    def get_version_info(self) -> str:
        return (
            f"{self.data_source.snapshot_id}:"
            f"ensembl-gene-resolver-{RESOLVER_CONTRACT_VERSION}"
        )

    def matching_ids(self) -> Generator[MatchingPair, Any, None]:
        emitted: set[MatchingPair] = set()
        with self.data_file.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                ensembl_id = _equivalent(row.get("Gene stable ID"), Prefix.ENSEMBL)
                if ensembl_id is None:
                    continue
                pairs = [MatchingPair(id=ensembl_id, match=ensembl_id, type="exact")]

                hgnc_id = _equivalent(row.get("HGNC ID"), Prefix.HGNC, strip_prefix="HGNC:")
                if hgnc_id is not None:
                    pairs.append(
                        MatchingPair(
                            id=ensembl_id,
                            match=hgnc_id,
                            type=Prefix.HGNC.value,
                        )
                    )

                ncbi_id = _ncbi_gene(row.get("NCBI gene (formerly Entrezgene) ID"))
                if ncbi_id is not None:
                    pairs.append(
                        MatchingPair(
                            id=ensembl_id,
                            match=ncbi_id,
                            type=Prefix.NCBIGene.value,
                        )
                    )

                # The existing Harmonizers export calls BioMart's
                # external_gene_name field "Gene name".
                symbol = _equivalent(row.get("Gene name"), Prefix.Symbol)
                if symbol is not None:
                    pairs.append(
                        MatchingPair(
                            id=ensembl_id,
                            match=symbol,
                            type=Prefix.Symbol.value,
                        )
                    )

                for pair in pairs:
                    if pair not in emitted:
                        emitted.add(pair)
                        yield pair


def _equivalent(
    raw_value: str | None,
    prefix: Prefix,
    *,
    strip_prefix: str | None = None,
) -> str | None:
    value = (raw_value or "").strip()
    if strip_prefix and value.startswith(strip_prefix):
        value = value[len(strip_prefix) :]
    if not value:
        return None
    return EquivalentId(id=value, type=prefix).id_str()


def _ncbi_gene(raw_value: str | None) -> str | None:
    value = (raw_value or "").strip()
    if not value:
        return None
    try:
        normalized = str(int(value))
    except ValueError:
        return None
    return EquivalentId(id=normalized, type=Prefix.NCBIGene).id_str()
