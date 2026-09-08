import csv
from typing import Generator, List

from src.constants import DataSourceName, Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import EquivalentId
from src.models.protein import IDGFamily, Protein
from src.shared.csv_parser import CSVParser


class ManualIDGFamilyAdapter(InputAdapter, CSVParser):
    batch_size: int = 1000

    def __init__(self, data_source):
        InputAdapter.__init__(self)
        file_path = str(data_source.file("tdl_updates.csv"))
        CSVParser.__init__(self, file_path=file_path)
        self.version_info = data_source.version_info()

    def get_all(self) -> Generator[List[Protein], None, None]:
        proteins = []
        with open(self.file_path, newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                uniprot_id = (row.get("UniProt") or row.get("uniprot_id") or "").strip()
                idg_family = (row.get("idg_family") or "").strip()
                if not uniprot_id or not idg_family:
                    continue
                equiv_id = EquivalentId(id=uniprot_id, type=Prefix.UniProtKB)
                proteins.append(
                    Protein(
                        id=equiv_id.id_str(),
                        idg_family=IDGFamily.parse(idg_family),
                    )
                )
                if len(proteins) >= self.batch_size:
                    yield proteins
                    proteins = []
        if proteins:
            yield proteins

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.ManualUpdate

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info
