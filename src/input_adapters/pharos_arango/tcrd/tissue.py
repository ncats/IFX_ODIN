from typing import Generator, List, Union

from src.input_adapters.pharos_arango.tcrd.protein import PharosArangoAdapter
from src.models.datasource_version_info import DataSourceDetails
from src.models.expression import ExpressionDetail, ProteinTissueExpressionEdge
from src.models.protein import Protein
from src.models.tissue import Tissue, TissueParentEdge


def tissue_query() -> str:
    return """FOR t IN `Tissue`
    RETURN t
    """


def tissue_parent_query() -> str:
    return """FOR rel IN `TissueParentEdge`
    RETURN { "start": rel.start_id, "end": rel.end_id }
    """


def expression_query(last_key: str = None, limit: int = 5000) -> str:
    filter_clause = f'FILTER rel._key > "{last_key}"' if last_key else ""
    return f"""
    FOR rel IN `ProteinTissueExpressionEdge`
        {filter_clause}
        SORT rel._key
        LIMIT {limit}
        RETURN rel
    """


def tissue_version_query() -> str:
    return """FOR t IN `Tissue` LIMIT 1 RETURN t.creation"""


class TissueAdapter(PharosArangoAdapter):

    def get_all(self) -> Generator[List[Union[Tissue, TissueParentEdge]], None, None]:
        tissues = self.runQuery(tissue_query())
        tissue_map = {}
        rows = []
        for t in tissues:
            tissue = Tissue.from_dict(t)
            tissue_map[tissue.id] = tissue
            rows.append(tissue)
        yield rows

        parents = self.runQuery(tissue_parent_query())
        yield [
            TissueParentEdge(
                start_node=tissue_map[rel['start']],
                end_node=tissue_map[rel['end']]
            )
            for rel in parents
            if rel['start'] in tissue_map and rel['end'] in tissue_map
        ]

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(tissue_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)


class ExpressionAdapter(PharosArangoAdapter):
    batch_size = 5_000

    def __init__(self, credentials, database_name: str, max_rows: int = None):
        self.max_rows = max_rows
        super().__init__(credentials=credentials, database_name=database_name)

    def get_all(self) -> Generator[List[ProteinTissueExpressionEdge], None, None]:
        last_key = None
        emitted = 0
        while True:
            batch_limit = self.batch_size
            if self.max_rows is not None:
                remaining = self.max_rows - emitted
                if remaining <= 0:
                    break
                batch_limit = min(batch_limit, remaining)

            rows = list(self.runQuery(expression_query(last_key=last_key, limit=batch_limit)))
            if not rows:
                break

            batch = [
                ProteinTissueExpressionEdge(
                    start_node=Protein(id=row['start_id']),
                    end_node=Tissue(id=row['end_id']),
                    details=[ExpressionDetail.from_dict(d) for d in (row.get('details') or [])]
                )
                for row in rows
            ]
            yield batch
            emitted += len(batch)
            last_key = rows[-1]['_key']

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(tissue_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)
