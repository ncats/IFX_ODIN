from typing import List, Generator

from src.constants import Prefix, DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import EquivalentId
from src.models.protein import Protein
from src.models.year_score import YearScore


class TotalPMScoreAdapter(InputAdapter):
    batch_size: int = 1000

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.JensenLabPM

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def __init__(self, data_source):
        self.file_path = str(data_source.file("protein_counts.tsv"))
        self.version_info = data_source.version_info()

    def get_all(self) -> Generator[List[Protein], None, None]:
        total_pm_dict = {}
        yearly_scores = {}
        with open(self.file_path, 'r') as file:
            for line in file:
                ensp_id, year, score = line.strip().split('\t')
                score_value = float(score)

                if ensp_id not in total_pm_dict:
                    total_pm_dict[ensp_id] = 0
                    yearly_scores[ensp_id] = []
                total_pm_dict[ensp_id] += score_value
                yearly_scores[ensp_id].append(
                    YearScore(year=int(year) if year else None, score=score_value)
                )

        proteins: List[Protein] = []
        for ensp_id, score in total_pm_dict.items():
            prefixed_id = EquivalentId(id = ensp_id, type = Prefix.ENSEMBL)
            yearly_history = sorted(
                yearly_scores[ensp_id],
                key=lambda entry: (entry.year is None, entry.year if entry.year is not None else 0)
            )
            protein = Protein(
                id=prefixed_id.id_str(),
                pm_score=[score],
                pm_score_by_year=yearly_history,
            )
            proteins.append(protein)

        yield proteins
