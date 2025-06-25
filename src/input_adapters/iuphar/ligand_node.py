import csv
import os
from abc import ABC
from datetime import datetime, date
from typing import Generator, List

from src.api_adapters.strawberry_models.pharos_query_models import EquivalentId
from src.constants import DataSourceName, Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.ligand import Ligand

class IUPHARAdapter(InputAdapter, ABC):
    file_path: str
    version: str
    version_date: date
    download_date: date

    def __init__(self, file_path: str):
        InputAdapter.__init__(self)
        self.file_path = file_path
        self.download_date = datetime.fromtimestamp(os.path.getmtime(file_path)).date()

        with open(file_path, 'r') as f:
            first_line = f.readline().strip()

        version_part = first_line.split("Version:")[1].split("-")[0].strip()
        date_part = first_line.split("published:")[1].strip().rstrip('"')

        self.version = version_part
        self.version_date = datetime.strptime(date_part, "%Y-%m-%d").date()

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.IUPHAR

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version=self.version,
            version_date=self.version_date,
            download_date=self.download_date
        )

class LigandNodeAdapter(IUPHARAdapter):
    def get_all(self) -> Generator[List[Ligand], None, None]:
        ligands: List[Ligand] = []
        with open(self.file_path, mode='r') as file:
            next(file)
            csv_reader = csv.DictReader(file)

            for row in csv_reader:
                if row['Type'] in ['Peptide', 'Antibody']:
                    continue
                name = row['Name']
                cid = row['PubChem CID']
                smiles = row['SMILES']
                if not cid or cid == '':
                    continue
                ligand_obj = Ligand(
                    id=EquivalentId(id=cid, type=Prefix.PUBCHEM_COMPOUND).id_str(),
                    name=name,
                    smiles = smiles
                )
                ligands.append(ligand_obj)
        yield ligands

