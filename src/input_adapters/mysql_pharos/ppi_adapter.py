from typing import List

from sqlalchemy import and_, or_
from sqlalchemy.orm import aliased

from src.constants import Prefix
from src.input_adapters.sql_adapter import MySqlAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.node import Relationship

from src.input_adapters.mysql_pharos.tables import (Protein as mysql_protein, PPI as mysql_ppi)
from src.models.ppi import PPIRelationship
from src.models.protein import Protein


class ProteinProteinInteractionAdapter(RelationshipInputAdapter, MySqlAdapter):
    name = "Pharos Protein-Protein Interaction Relationship Adapter"

    def get_audit_trail_entries(self, obj) -> List[str]:
        return [f"Protein-Protein Interaction from {self.credentials.schema})"]

    def get_all(self) -> List[Relationship]:
        protein_alias1 = aliased(mysql_protein)
        protein_alias2 = aliased(mysql_protein)

        results = (self.get_session().query(
            protein_alias1.uniprot,
            protein_alias2.uniprot,
            mysql_ppi.ppitypes,
            mysql_ppi.p_int,
            mysql_ppi.p_ni,
            mysql_ppi.p_wrong,
            mysql_ppi.score
        ).join(protein_alias1, mysql_ppi.protein_id == protein_alias1.id)
            .join(protein_alias2, mysql_ppi.other_id == protein_alias2.id)
            .filter(mysql_ppi.protein_id < mysql_ppi.other_id)
            .filter(
                or_(
                    mysql_ppi.ppitypes != 'StringDB', # i.e. not just StringDB by itself
                    and_(                             # or it is StringDB, with a decent score
                        mysql_ppi.ppitypes == 'StringDB',
                        mysql_ppi.score >= 400
                    )
                )
            )
        )

        return [
            PPIRelationship(
                start_node=Protein(id=f"{Prefix.UniProtKB}:{row[0]}"),
                end_node=Protein(id=f"{Prefix.UniProtKB}:{row[1]}"),
                sources=row[2].split(','),
                p_int=row[3],
                p_ni=row[4],
                p_wrong=row[5],
                score=row[6]
            )
            for row in results]
