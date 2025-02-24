from abc import ABC, abstractmethod
from typing import List

from src.constants import Prefix
from src.input_adapters.sql_adapter import MySqlAdapter
from src.input_adapters.mysql_pharos.tables import Protein as mysql_Protein, TDL_info as mysql_tdl_info
from src.interfaces.input_adapter import InputAdapter
from src.models.protein import Protein


class TdlTableAdapter(NodeInputAdapter, MySqlAdapter, ABC):
    name = "Pharos TDL Table Adapter"
    column = None
    field = None
    itype = None

    @abstractmethod
    def get_column(self):
        raise Exception("base classes must implement this method")

    def get_audit_trail_entries(self, obj) -> List[str]:
        return [f'{self.field} updated by {self.name} using {self.credentials.schema}']

    def get_all(self):
        results = (self.get_session().query(
            mysql_Protein.uniprot,
            self.get_column()
        ).join(mysql_Protein, mysql_Protein.id == mysql_tdl_info.protein_id)
                   .where(mysql_tdl_info.itype == self.itype))

        nodes = []
        for row in results:
            protein: Protein = Protein(
                id=f"{Prefix.UniProtKB}:{row[0]}"
            )
            setattr(protein, self.field, row[1])
            nodes.append(protein)

        return nodes


class AntibodyCountAdapter(TdlTableAdapter):
    name = "Pharos Antibody Count Adapter"
    field = 'antibody_count'
    itype = "Ab Count"

    def get_column(self):
        return mysql_tdl_info.integer_value


class PubMedScoreAdapter(TdlTableAdapter):
    name = "Pharos PubMed Score Adapter"
    field = 'pm_score'
    itype = "JensenLab PubMed Score"

    def get_column(self):
        return mysql_tdl_info.number_value
