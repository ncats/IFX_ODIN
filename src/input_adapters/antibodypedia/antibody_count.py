import csv
import os
from datetime import date, datetime
from typing import List

from src.constants import Prefix, DataSourceName
from src.interfaces.input_adapter import NodeInputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import EquivalentId
from src.models.protein import Protein


class AntibodyCountAdapter(NodeInputAdapter):
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

    def get_all(self) -> List[Protein]:
        proteins = []
        with open(self.file_path, mode='r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                uniprot_id = row['uniprot_id']
                if uniprot_id in ['0', '#N/A']:
                    continue
                equiv_id = EquivalentId(id=uniprot_id, type=Prefix.UniProtKB)
                antibody_count = int(row['Antibodies'].split()[0])
                protein = Protein(id=equiv_id.id_str(), antibody_count=antibody_count)
                proteins.append(protein)

        return proteins
