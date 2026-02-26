from typing import Generator, Any

import pandas as pd
import requests
import requests_cache
from pybiomart import Server

from src.constants import Prefix
from src.id_resolvers.sqlite_cache_resolver import SqliteCacheResolver, MatchingPair
from src.models.node import EquivalentId


class EnsemblGeneResolver(SqliteCacheResolver):
    name = "Ensembl Gene Resolver"
    species: str

    def __init__(self, species: str = 'hsapiens', **kwargs):
        self.species = species
        SqliteCacheResolver.__init__(self, **kwargs)
        # pybiomart calls requests_cache.install_cache() at module import time,
        # which globally patches requests.Session. This causes ArangoDB's
        # python-arango client to receive stale cached responses for API calls
        # like has_graph/has_collection. Uninstall the cache here so downstream
        # HTTP clients are unaffected.
        requests_cache.uninstall_cache()

    def get_version_info(self) -> str:
        try:
            response = requests.get(
                'https://rest.ensembl.org/info/software',
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            release = response.json().get('release', 'unknown')
        except Exception:
            release = 'unknown'
        return f"ensembl:{self.species}:release-{release}"

    def matching_ids(self) -> Generator[MatchingPair, Any, None]:
        server = Server(host='http://www.ensembl.org', use_cache=False)
        dataset = server.marts['ENSEMBL_MART_ENSEMBL'].datasets[f'{self.species}_gene_ensembl']

        results = dataset.query(
            attributes=['ensembl_gene_id', 'hgnc_id', 'entrezgene_id', 'hgnc_symbol'],
            use_attr_names=True
        )

        for _, row in results.iterrows():
            ensg_raw = row.get('ensembl_gene_id')
            if pd.isna(ensg_raw) or not ensg_raw:
                continue

            ensg_id = EquivalentId(id=str(ensg_raw).strip(), type=Prefix.ENSEMBL).id_str()
            yield MatchingPair(id=ensg_id, match=ensg_id, type='exact')

            hgnc_raw = row.get('hgnc_id')
            if pd.notna(hgnc_raw) and hgnc_raw:
                hgnc_str = str(hgnc_raw).strip()
                # BioMart may return "HGNC:12345" or just "12345"
                hgnc_num = hgnc_str.removeprefix('HGNC:')
                if hgnc_num:
                    hgnc_id = EquivalentId(id=hgnc_num, type=Prefix.HGNC).id_str()
                    yield MatchingPair(id=ensg_id, match=hgnc_id, type=Prefix.HGNC.value)

            entrez_raw = row.get('entrezgene_id')
            if pd.notna(entrez_raw):
                try:
                    ncbi_id = EquivalentId(id=str(int(entrez_raw)), type=Prefix.NCBIGene).id_str()
                    yield MatchingPair(id=ensg_id, match=ncbi_id, type=Prefix.NCBIGene.value)
                except (ValueError, TypeError):
                    pass

            symbol_raw = row.get('hgnc_symbol')
            if pd.notna(symbol_raw) and symbol_raw:
                prefixed_symbol = EquivalentId(id=str(symbol_raw).strip(), type=Prefix.Symbol).id_str()
                yield MatchingPair(id=ensg_id, match=prefixed_symbol, type=Prefix.Symbol.value)