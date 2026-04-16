from typing import Generator, List

from src.input_adapters.pharos_arango.tcrd.protein import PharosArangoAdapter
from src.models.datasource_version_info import DataSourceDetails
from src.models.dto_class import DTOClass, DTOClassParentEdge, ProteinDTOClassEdge
from src.models.protein import Protein


def dto_class_query() -> str:
    return """FOR d IN `DTOClass` RETURN d"""


def dto_class_parent_query() -> str:
    return """FOR rel IN `DTOClassParentEdge` RETURN rel"""


def protein_dto_class_query(last_key: str = None, limit: int = 10000) -> str:
    filter_clause = f'FILTER rel._key > "{last_key}"' if last_key else ""
    return f"""
    FOR rel IN `ProteinDTOClassEdge`
        {filter_clause}
        SORT rel._key
        LIMIT {limit}
        RETURN rel
    """


def dto_version_query() -> str:
    return """FOR d IN `DTOClass` LIMIT 1 RETURN d.creation"""


class DTOClassAdapter(PharosArangoAdapter):
    def get_all(self) -> Generator[List[DTOClass], None, None]:
        rows = [
            DTOClass(
                id=row["id"],
                source_id=row.get("source_id"),
                name=row.get("name"),
                description=row.get("description"),
                provenance=row.get("provenance"),
                sources=row.get("sources") or [],
            )
            for row in self.runQuery(dto_class_query())
        ]
        yield rows

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(dto_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)


class DTOClassParentAdapter(PharosArangoAdapter):
    def get_all(self) -> Generator[List[DTOClassParentEdge], None, None]:
        yield [
            DTOClassParentEdge(
                start_node=DTOClass(id=row["start_id"]),
                end_node=DTOClass(id=row["end_id"]),
                provenance=row.get("provenance"),
                sources=row.get("sources") or [],
            )
            for row in self.runQuery(dto_class_parent_query())
        ]

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(dto_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)


class ProteinDTOClassAdapter(PharosArangoAdapter):
    batch_size = 10_000

    def get_all(self) -> Generator[List[ProteinDTOClassEdge], None, None]:
        last_key = None
        while True:
            rows = list(self.runQuery(protein_dto_class_query(last_key=last_key, limit=self.batch_size)))
            if not rows:
                break

            yield [
                ProteinDTOClassEdge(
                    start_node=Protein(id=row["start_id"]),
                    end_node=DTOClass(id=row["end_id"]),
                    provenance=row.get("provenance"),
                    sources=row.get("sources") or [],
                )
                for row in rows
            ]
            last_key = rows[-1]["_key"]

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(dto_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)
