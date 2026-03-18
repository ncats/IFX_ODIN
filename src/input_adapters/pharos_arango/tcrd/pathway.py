from typing import Generator, List

from src.input_adapters.pharos_arango.tcrd.protein import PharosArangoAdapter
from src.models.datasource_version_info import DataSourceDetails
from src.models.pathway import Pathway, ProteinPathwayEdge
from src.models.protein import Protein


def protein_pathway_query(last_key: str = None, limit: int = 10000) -> str:
    filter_clause = f'FILTER rel._key > "{last_key}"' if last_key else ""
    return f"""
    FOR rel IN `ProteinPathwayEdge`
        {filter_clause}
        SORT rel._key
        LIMIT {limit}
        RETURN rel
    """


def pathway_version_query() -> str:
    return """FOR p IN `Pathway` LIMIT 1 RETURN p.creation"""


class ProteinPathwayAdapter(PharosArangoAdapter):
    batch_size = 10_000

    def get_all(self) -> Generator[List[ProteinPathwayEdge], None, None]:
        last_key = None
        while True:
            rows = list(self.runQuery(protein_pathway_query(last_key=last_key, limit=self.batch_size)))
            if not rows:
                break

            yield [
                ProteinPathwayEdge(
                    start_node=Protein(id=row['start_id']),
                    end_node=Pathway.from_dict(row['end_node']),
                    source=row.get('source')
                )
                for row in rows
            ]
            last_key = rows[-1]['_key']

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(pathway_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)