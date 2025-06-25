import csv
from typing import Generator, List
from src.constants import DataSourceName, Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import EquivalentId
from src.models.protein import Protein
from src.shared.csv_parser import CSVParser


class TDLOverrideAdapter(InputAdapter, CSVParser):
    def __init__(self, file_path: str):
        InputAdapter.__init__(self)
        CSVParser.__init__(self, file_path=file_path)

    def get_all(self) -> Generator[List[Protein], None, None]:

        proteins = []
        with open(self.file_path, mode='r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                uniprot_id = row['UniProt']
                tdl = row['Target Development Level']
                equiv_id = EquivalentId(id=uniprot_id, type=Prefix.UniProtKB)
                protein_obj = Protein(id=equiv_id.id_str(), tdl=tdl)
                proteins.append(protein_obj)

        yield proteins

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.ManualUpdate

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            download_date=self.download_date
        )