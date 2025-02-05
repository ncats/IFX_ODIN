import os
from datetime import datetime, date
from typing import List

from src.constants import Prefix, DataSourceName
from src.interfaces.input_adapter import NodeInputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import EquivalentId
from src.models.protein import Protein


class TotalPMScoreAdapter(NodeInputAdapter):
    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.JensenLab

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            download_date=self.download_date
        )

    file_path: str
    download_date: date

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.download_date = datetime.fromtimestamp(os.path.getmtime(file_path)).date()

    def get_all(self):
        total_pm_dict = {}
        with open(self.file_path, 'r') as file:
            for line in file:
                ensp_id, year, score = line.strip().split('\t')

                if ensp_id not in total_pm_dict:
                    total_pm_dict[ensp_id] = 0
                total_pm_dict[ensp_id] += float(score)

        proteins: List[Protein] = []
        for ensp_id, score in total_pm_dict.items():
            prefixed_id = EquivalentId(id = ensp_id, type = Prefix.ENSEMBL)
            protein = Protein(id = prefixed_id.id_str(), pm_score = score)
            proteins.append(protein)

        return proteins
