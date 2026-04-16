from typing import Generator, List

from src.constants import Prefix
from src.input_adapters.pharos_mysql.base import Pharos319Adapter
from src.models.dto_class import DTOClass, DTOClassParentEdge, ProteinDTOClassEdge
from src.models.node import EquivalentId
from src.models.protein import Protein
from src.shared.sqlalchemy_tables.pharos_tables_old import (
    DTO as mysql_DTO,
    P2DTO as mysql_P2DTO,
    Protein as mysql_Protein,
)


def _dto_node_id(dtoid: str) -> str:
    return dtoid.replace("_", ":")


class DTOClassAdapter(Pharos319Adapter):
    def get_all(self) -> Generator[List[DTOClass], None, None]:
        rows = self.get_session().query(
            mysql_DTO.dtoid,
            mysql_DTO.name,
            mysql_DTO.def_,
        )

        yield [
            DTOClass(
                id=_dto_node_id(row[0]),
                source_id=row[0],
                name=row[1],
                description=row[2],
            )
            for row in rows
        ]


class DTOClassParentEdgeAdapter(Pharos319Adapter):
    def get_all(self) -> Generator[List[DTOClassParentEdge], None, None]:
        rows = (
            self.get_session().query(
                mysql_DTO.dtoid,
                mysql_DTO.parent_id,
            )
            .filter(mysql_DTO.parent_id.is_not(None))
        )

        yield [
            DTOClassParentEdge(
                start_node=DTOClass(id=_dto_node_id(row[0])),
                end_node=DTOClass(id=_dto_node_id(row[1])),
            )
            for row in rows
        ]


class ProteinDTOClassAdapter(Pharos319Adapter):
    def get_all(self) -> Generator[List[Protein | ProteinDTOClassEdge], None, None]:
        rows = (
            self.get_session().query(
                mysql_Protein.uniprot,
                mysql_P2DTO.dtoid,
                mysql_P2DTO.generation,
                mysql_DTO.name,
            )
            .join(mysql_Protein, mysql_Protein.id == mysql_P2DTO.protein_id)
            .join(mysql_DTO, mysql_DTO.dtoid == mysql_P2DTO.dtoid)
            .filter(mysql_P2DTO.generation == 0)
        )

        objects: List[Protein | ProteinDTOClassEdge] = []
        for row in rows:
            protein_id = EquivalentId(id=row[0], type=Prefix.UniProtKB).id_str()
            dtoid = _dto_node_id(row[1])
            objects.append(
                Protein(
                    id=protein_id,
                    dtoid=dtoid,
                    dtoclass=row[3],
                )
            )
            objects.append(
                ProteinDTOClassEdge(
                    start_node=Protein(id=protein_id),
                    end_node=DTOClass(id=dtoid),
                )
            )

        yield objects
