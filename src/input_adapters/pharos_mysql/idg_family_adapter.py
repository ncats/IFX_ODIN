from datetime import datetime
from typing import List, Generator

from src.constants import Prefix, DataSourceName
from src.input_adapters.sql_adapter import MySqlAdapter
from src.input_adapters.pharos_mysql.old_tables import Protein as mysql_Protein, Target as mysql_Target, T2TC as mysql_t2tc
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import EquivalentId
from src.models.protein import Protein, IDGFamily


class IDGFamilyAdapter(InputAdapter, MySqlAdapter):
    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.OldPharos

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version="3.19",
            version_date=datetime.fromisoformat("2024-02-15"),
        )

    def get_all(self) -> Generator[List[Protein], None, None]:
        results = (self.get_session().query(
            mysql_Protein.uniprot,
            mysql_Target.fam
        ).join(mysql_t2tc, mysql_Protein.id == mysql_t2tc.protein_id)
                   .join(mysql_Target, mysql_t2tc.target_id == mysql_Target.id))

        nodes: List[Protein] = [
            Protein(
                id=EquivalentId(id=row[0], type=Prefix.UniProtKB).id_str(),
                idg_family=IDGFamily.parse(row[1])
            ) for row in results
        ]

        yield nodes
