from typing import List

from src.constants import Prefix
from src.input_adapters.sql_adapter import MySqlAdapter
from src.input_adapters.mysql_pharos.tables import Protein as mysql_Protein, Target as mysql_Target, T2TC as mysql_t2tc
from src.interfaces.input_adapter import InputAdapter
from src.models.protein import Protein, IDGFamily


class ProteinAdapter(NodeInputAdapter, MySqlAdapter):
    name = "Pharos Protein Adapter"

    def get_audit_trail_entries(self, obj) -> List[str]:
        version_info = [f"Node created based on Pharos version: {self.credentials.schema}"]
        return version_info

    def get_all(self):
        results = (self.get_session().query(
            mysql_Protein.name,
            mysql_Protein.seq,
            mysql_Protein.sym,
            mysql_Protein.uniprot,
            mysql_Protein.description,
            mysql_Protein.family,
            mysql_Target.tdl,
            mysql_Target.fam
        ).join(mysql_t2tc, mysql_Protein.id == mysql_t2tc.protein_id)
                   .join(mysql_Target, mysql_t2tc.target_id == mysql_Target.id))

        nodes: [Protein] = [
            Protein(
                gene_name=row[0],
                sequence=row[1],
                symbol=row[2],
                id=f"{Prefix.UniProtKB}:{row[3]}",
                name=row[4],
                protein_type=row[5],
                tdl=row[6],
                idg_family=IDGFamily.parse(row[7])
            ) for row in results
        ]

        return nodes
