from typing import Generator, List, Optional, Set

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.protein import Protein
from src.shared.arango_adapter import ArangoAdapter


class SetPreferredSymbolAdapter(InputAdapter, ArangoAdapter):
    batch_size: int = 1000

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PostProcessing

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo()

    def get_all(self) -> Generator[List[Protein], None, None]:
        duplicate_symbols = set(self.runQuery(duplicate_symbol_query))
        last_key = ""

        while True:
            rows = self.runQuery(
                protein_symbol_query,
                bind_vars={
                    "last_key": last_key,
                    "limit": self.batch_size,
                },
            )
            if not rows:
                break

            batch = []
            for row in rows:
                preferred_symbol = get_preferred_symbol(row, duplicate_symbols)
                if preferred_symbol:
                    batch.append(Protein(id=row["id"], preferred_symbol=preferred_symbol))

            if batch:
                yield batch

            last_key = rows[-1]["_key"]


def get_preferred_symbol(row: dict, duplicate_symbols: Set[str]) -> Optional[str]:
    symbol = row.get("symbol")
    if symbol and symbol not in duplicate_symbols:
        return symbol
    return row.get("uniprot_id")


duplicate_symbol_query = """
FOR p IN Protein
    FILTER p.symbol != null AND p.symbol != ""
    COLLECT symbol = p.symbol WITH COUNT INTO count
    FILTER count > 1
    RETURN symbol
"""


protein_symbol_query = """
FOR p IN Protein
    FILTER p._key > @last_key
    SORT p._key
    LIMIT @limit
    RETURN {
        _key: p._key,
        id: p.id,
        symbol: p.symbol,
        uniprot_id: p.uniprot_id
    }
"""
