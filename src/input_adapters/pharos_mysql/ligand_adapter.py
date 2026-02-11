from typing import List

from src.input_adapters.sql_adapter import MySqlAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.ligand import Ligand
from src.models.node import Node
from src.input_adapters.pharos_mysql.old_tables import Ligand as mysql_Ligand

class LigandAdapter(InputAdapter, MySqlAdapter):

    def get_all(self) -> List[Node]:

        results = self.get_session().query(
            mysql_Ligand.identifier,
            mysql_Ligand.name,
            mysql_Ligand.isDrug,
            mysql_Ligand.smiles,
            mysql_Ligand.description
        )

        return [Ligand(
            id=row[0],
            name=row[1],
            isDrug=row[2],
            smiles=row[3],
            description=row[4]
        ) for row in results]
