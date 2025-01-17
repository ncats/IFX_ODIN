from typing import List

from src.constants import Prefix
from src.input_adapters.sql_adapter import MySqlAdapter
from src.input_adapters.mysql_pharos.tables import Protein as mysql_Protein, Target as mysql_Target, T2TC as mysql_t2tc
from src.interfaces.input_adapter import NodeInputAdapter
from src.models.node import EquivalentId
from src.models.protein import Protein, IDGFamily


class IDGFamilyAdapter(NodeInputAdapter, MySqlAdapter):
    name = "IDG Family Adapter"

    def get_audit_trail_entries(self, obj) -> List[str]:
        version_info = [f"IDG Family updated based on Pharos version: {self.credentials.schema}"]
        return version_info

    def get_all(self):
        results = (self.get_session().query(
            mysql_Protein.uniprot,
            mysql_Target.fam
        ).join(mysql_t2tc, mysql_Protein.id == mysql_t2tc.protein_id)
                   .join(mysql_Target, mysql_t2tc.target_id == mysql_Target.id))

        nodes: [Protein] = [
            Protein(
                id=EquivalentId(id=row[0], type=Prefix.UniProtKB).id_str(),
                idg_family=IDGFamily.parse(row[1])
            ) for row in results
        ]

        return nodes
