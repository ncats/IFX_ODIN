import csv
import os
from datetime import date, datetime
from typing import List, Generator

from src.constants import Prefix, DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.node import EquivalentId


class AntibodyCountAdapter(InputAdapter):
    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.Antibodypedia

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            download_date=self.download_date
        )

    file_path: str
    download_date: date

    def __init__(self, file_path: str):
        super().__init__()
        self.file_path = file_path
        self.download_date = datetime.fromtimestamp(os.path.getmtime(file_path)).date()

    def get_all(self) -> Generator[List[Gene], None, None]:
        genes = []
        with open(self.file_path, mode='r') as file:
            csv_reader = csv.DictReader(file, delimiter='\t')
            for row in csv_reader:
                ensembl_id = row['ensembl']
                equiv_id = EquivalentId(id=ensembl_id, type=Prefix.ENSEMBL)
                antibody_count = int(row['num_antibodies'])
                gene_obj = Gene(id=equiv_id.id_str(), antibody_count=antibody_count)
                genes.append(gene_obj)

        yield genes
