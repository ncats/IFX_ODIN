from typing import Generator, List, Union

from src.input_adapters.pharos_arango.tcrd.protein import PharosArangoAdapter
from src.models.datasource_version_info import DataSourceDetails
from src.models.protein import Protein
from src.models.tiga import GwasTrait, ProteinGwasTraitEdge


def protein_gwas_trait_query(last_key: str = None, limit: int = 10000) -> str:
    filter_clause = f'FILTER rel._key > "{last_key}"' if last_key else ""
    return f"""
    FOR rel IN `ProteinGwasTraitEdge`
        {filter_clause}
        SORT rel._key
        LIMIT {limit}
        LET trait = DOCUMENT(rel._to)
        LET disease_ids = UNIQUE(
            FOR disease_rel IN `GwasTraitDiseaseEdge`
                FILTER disease_rel.start_id == rel.end_id
                RETURN disease_rel.end_id
        )
        RETURN {{
            "_key": rel._key,
            "start_id": rel.start_id,
            "end_id": rel.end_id,
            "end_node": trait,
            "details": rel.details,
            "disease_ids": disease_ids
        }}
    """


def tiga_version_query() -> str:
    return """FOR trait IN `GwasTrait`
    LIMIT 1
    RETURN trait.creation
    """


class ProteinGwasTraitAdapter(PharosArangoAdapter):
    batch_size = 10_000

    def get_all(self) -> Generator[List[Union[ProteinGwasTraitEdge]], None, None]:
        last_key = None
        while True:
            rows = list(self.runQuery(protein_gwas_trait_query(last_key=last_key, limit=self.batch_size)))
            if not rows:
                break

            yield [
                ProteinGwasTraitEdge(
                    start_node=Protein(id=row["start_id"]),
                    end_node=GwasTrait.from_dict(row["end_node"]),
                    details=row.get("details") or [],
                    disease_ids=row.get("disease_ids") or [],
                )
                for row in rows
            ]
            last_key = rows[-1]["_key"]

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(tiga_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)
