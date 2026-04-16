from typing import List, Generator

from src.constants import Prefix
from src.input_adapters.pharos_mysql.base import Pharos319Adapter
from src.shared.sqlalchemy_tables.pharos_tables_old import Protein as mysql_Protein, Target as mysql_Target, T2TC as mysql_t2tc
from src.models.node import EquivalentId
from src.models.protein import Protein, IDGFamily


class IDGFamilyAdapter(Pharos319Adapter):
    batch_size: int = 1000

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
