import csv
from typing import Generator, List
from src.constants import DataSourceName, Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import EquivalentId
from src.models.protein import Protein
from src.shared.csv_parser import CSVParser
from src.shared.record_merger import FieldConflictBehavior


class TDLOverrideAdapter(InputAdapter, CSVParser):
    batch_size: int = 1000
    field_conflict_behavior: FieldConflictBehavior = FieldConflictBehavior.KeepLast

    def __init__(self, data_source):
        InputAdapter.__init__(self)
        file_path = str(data_source.file("tdl_updates.csv"))
        CSVParser.__init__(self, file_path=file_path)
        self.version_info = data_source.version_info()

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
        return self.version_info
