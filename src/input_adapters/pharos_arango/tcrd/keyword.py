from typing import Generator, List

from src.input_adapters.pharos_arango.tcrd.protein import PharosArangoAdapter
from src.models.datasource_version_info import DataSourceDetails
from src.models.keyword import Keyword, ProteinKeywordEdge
from src.models.protein import Protein


def protein_keyword_query(last_key: str = None, limit: int = 10000) -> str:
    filter_clause = f'FILTER rel._key > "{last_key}"' if last_key else ""
    return f"""
    FOR rel IN `ProteinKeywordEdge`
        {filter_clause}
        SORT rel._key
        LIMIT {limit}
        RETURN rel
    """


def keyword_version_query() -> str:
    return """FOR k IN `Keyword` LIMIT 1 RETURN k.creation"""


class ProteinKeywordAdapter(PharosArangoAdapter):
    batch_size = 10_000

    def get_all(self) -> Generator[List[ProteinKeywordEdge], None, None]:
        last_key = None
        while True:
            rows = list(self.runQuery(protein_keyword_query(last_key=last_key, limit=self.batch_size)))
            if not rows:
                break

            yield [
                ProteinKeywordEdge(
                    start_node=Protein(id=row['start_id']),
                    end_node=Keyword.from_dict(row['end_node']),
                )
                for row in rows
            ]
            last_key = rows[-1]['_key']

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(keyword_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)