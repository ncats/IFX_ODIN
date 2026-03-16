from typing import Generator, List, Union

from src.input_adapters.pharos_arango.tcrd.protein import PharosArangoAdapter
from src.models.datasource_version_info import DataSourceDetails
from src.models.disease import Disease, DiseaseParentEdge, ProteinDiseaseEdge
from src.models.protein import Protein


def disease_query() -> str:
    return """FOR d IN `Disease`
    RETURN d
    """


def disease_parent_query() -> str:
    return """FOR rel IN `DiseaseParentEdge`
    RETURN { "start": rel.start_id, "end": rel.end_id }
    """


def protein_disease_query(last_key: str = None, limit: int = 10000) -> str:
    filter_clause = f'FILTER rel._key > "{last_key}"' if last_key else ""
    return f"""
    FOR rel IN `ProteinDiseaseEdge`
        {filter_clause}
        SORT rel._key
        LIMIT {limit}
        RETURN rel
    """


def disease_version_query() -> str:
    return """FOR d IN `Disease` LIMIT 1 RETURN d.creation"""


class DiseaseAdapter(PharosArangoAdapter):

    def get_all(self) -> Generator[List[Union[Disease, DiseaseParentEdge]], None, None]:
        diseases = self.runQuery(disease_query())
        disease_map = {}
        rows = []
        for d in diseases:
            disease = Disease.from_dict(d)
            disease_map[disease.id] = disease
            rows.append(disease)
        yield rows

        parents = self.runQuery(disease_parent_query())
        yield [
            DiseaseParentEdge(
                start_node=disease_map[rel['start']],
                end_node=disease_map[rel['end']]
            )
            for rel in parents
            if rel['start'] in disease_map and rel['end'] in disease_map
        ]

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(disease_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)


class ProteinDiseaseAdapter(PharosArangoAdapter):
    batch_size = 10_000

    def get_all(self) -> Generator[List[ProteinDiseaseEdge], None, None]:
        last_key = None
        while True:
            rows = list(self.runQuery(protein_disease_query(last_key=last_key, limit=self.batch_size)))
            if not rows:
                break

            yield [
                ProteinDiseaseEdge(
                    start_node=Protein(id=row['start_id']),
                    end_node=Disease(id=row['end_id'], name=row.get('end_node', {}).get('name', '')),
                    source=row.get('source')
                )
                for row in rows
            ]
            last_key = rows[-1]['_key']

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(disease_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)