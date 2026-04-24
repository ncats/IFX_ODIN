from typing import Generator, List

from src.input_adapters.pharos_arango.tcrd.protein import PharosArangoAdapter
from src.models.datasource_version_info import DataSourceDetails
from src.models.ppi import PPIEdge
from src.models.protein import Protein


def protein_ppi_query(last_key: str = None, limit: int = 10000) -> str:
    filter_clause = f'FILTER rel._key > "{last_key}"' if last_key else ""
    return f"""
    FOR rel IN `PPIEdge`
        {filter_clause}
        SORT rel._key
        LIMIT {limit}
        RETURN rel
    """


def ppi_version_query() -> str:
    return """FOR rel IN `PPIEdge` LIMIT 1 RETURN rel.creation"""


class ProteinPPIAdapter(PharosArangoAdapter):
    batch_size = 10_000

    def get_all(self) -> Generator[List[PPIEdge], None, None]:
        last_key = None
        while True:
            rows = list(self.runQuery(protein_ppi_query(last_key=last_key, limit=self.batch_size)))
            if not rows:
                break

            yield [
                PPIEdge(
                    start_node=Protein(id=row["start_id"]),
                    end_node=Protein(id=row["end_id"]),
                    provenance=row.get("provenance"),
                    sources=row.get("sources") or [],
                    p_int=row.get("p_int") or [],
                    p_ni=row.get("p_ni") or [],
                    p_wrong=row.get("p_wrong") or [],
                    pmids=row.get("pmids") or [],
                    contexts=row.get("contexts") or [],
                    interaction_type=row.get("interaction_type") or [],
                    score=row.get("score") or [],
                )
                for row in rows
            ]
            last_key = rows[-1]["_key"]

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(ppi_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)
