import csv
import os
import re
from datetime import date, datetime
from typing import List, Generator

from src.constants import Prefix, DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.node import EquivalentId
from src.models.protein import Protein


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
        proteins = []
        with open(self.file_path, mode='r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                uniprot_id = row['uniprot_id']
                equiv_id = EquivalentId(id=uniprot_id, type=Prefix.UniProtKB)
                antibody_str = row['antibodies']
                if antibody_str is None or antibody_str == '':
                    continue
                match = re.search(r'\d+', antibody_str)
                if match:
                    antibody_count = int(match.group())
                else:
                    antibody_count = 0  # or handle the error
                if antibody_count <= 0:
                    continue
                protein_obj = Protein(id=equiv_id.id_str(), antibody_count=antibody_count)
                proteins.append(protein_obj)

        yield proteins
