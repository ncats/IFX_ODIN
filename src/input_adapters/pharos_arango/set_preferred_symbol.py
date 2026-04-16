from typing import Generator, List

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.protein import Protein
from src.shared.arango_adapter import ArangoAdapter


class SetPreferredSymbolAdapter(InputAdapter, ArangoAdapter):

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PostProcessing

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo()

    def get_all(self) -> Generator[List[Protein], None, None]:
        rows = self.runQuery(preferred_symbol_query)
        yield [
            Protein(id=row["id"], preferred_symbol=row["preferred_symbol"])
            for row in rows
            if row.get("preferred_symbol")
        ]


preferred_symbol_query = """
LET symbol_counts = (
    FOR p IN Protein
        FILTER p.symbol != null AND p.symbol != ""
        COLLECT symbol = p.symbol WITH COUNT INTO count
        RETURN {symbol, count}
)
FOR p IN Protein
    LET symbol_count = FIRST(
        FOR sc IN symbol_counts
            FILTER sc.symbol == p.symbol
            RETURN sc.count
    )
    LET preferred_symbol = (
        p.symbol != null AND p.symbol != "" AND symbol_count == 1
            ? p.symbol
            : p.uniprot_id
    )
    RETURN {
        id: p.id,
        preferred_symbol: preferred_symbol
    }
"""
