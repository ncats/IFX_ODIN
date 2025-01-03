from dataclasses import dataclass
from datetime import datetime
from typing import List
from src.constants import Prefix
from src.input_adapters.drug_central.tables import Structures, DBVersion
from src.input_adapters.sql_adapter import PostgreSqlAdapter
from src.interfaces.input_adapter import NodeInputAdapter
from src.models.ligand import Ligand
from src.models.node import Node, EquivalentId
from src.shared.db_credentials import DBCredentials


@dataclass
class DatabaseVersionInfo:
    version: int
    timestamp: datetime


class DrugCentralAdapter(PostgreSqlAdapter):
    version_info: DatabaseVersionInfo

    def __init__(self, credentials: DBCredentials):
        PostgreSqlAdapter.__init__(self, credentials)
        self.initialize_version()

    def initialize_version(self):
        results = self.get_session().query(
            DBVersion.version,
            DBVersion.dtime
        ).first()
        self.version_info = DatabaseVersionInfo(version=results.version, timestamp=results.dtime)


class DrugNodeAdapter(NodeInputAdapter, DrugCentralAdapter):
    name = "DrugCentral Drug Adapter"

    def get_all(self) -> List[Node]:
        query_results = self.get_session().query(
            Structures.id,
            Structures.name,
            Structures.smiles
        ).all()

        drug_list = [
            Ligand(
                id=EquivalentId(id=row.id, type=Prefix.DrugCentral).id_str(),
                name=row.name,
                smiles=row.smiles,
                isDrug=True
            )
            for row in query_results
        ]
        return drug_list

    def get_audit_trail_entries(self, obj: Node) -> List[str]:
        version_info = [
            f"Node created by {self.name} based on DrugCentral version: {self.version_info.version} ({self.version_info.timestamp})"]
        return version_info
