from typing import List

from src.input_adapters.sql_adapter import MySqlAdapter
from src.interfaces.input_adapter import NodeInputAdapter
from src.models.ligand import Ligand
from src.models.node import Node
from src.input_adapters.mysql_pharos.tables import Ligand as mysql_ligand

class LigandAdapter(NodeInputAdapter, MySqlAdapter):
    name = "Pharos Ligand Adapter"
    def get_audit_trail_entries(self, obj) -> List[str]:
        version_info = [f"Node created based on Pharos version: {self.db_credentials.schema}"]
        return version_info

    def get_all(self) -> List[Node]:

        results = self.get_session().query(
            mysql_ligand.identifier,
            mysql_ligand.name,
            mysql_ligand.isDrug,
            mysql_ligand.smiles,
            mysql_ligand.description
        )

        return [Ligand(
            id=row[0],
            name=row[1],
            isDrug=row[2],
            smiles=row[3],
            description=row[4]
        ) for row in results]
