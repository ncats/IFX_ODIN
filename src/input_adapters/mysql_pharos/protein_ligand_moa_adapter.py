from typing import List

from src.constants import Prefix
from src.input_adapters.sql_adapter import MySqlAdapter
from src.interfaces.input_adapter import RelationshipInputAdapter
from src.models.ligand import ProteinLigandRelationship, Ligand
from src.models.node import Relationship
from src.input_adapters.mysql_pharos.tables import (DrugActivity as mysql_drug_activity,
                                                    T2TC as mysql_t2tc, Protein as mysql_protein)
from src.models.protein import Protein


class ProteinLigandMOAAdapter(RelationshipInputAdapter, MySqlAdapter):
    name = "Pharos Protein Ligand Has MOA Adapter"

    def get_audit_trail_entries(self, obj) -> List[str]:
        return [f'has_moa updated by {self.name} using {self.db_credentials.schema}']

    def get_all(self) -> List[Relationship]:
        results = (self.get_session().query(
            mysql_protein.uniprot,
            mysql_drug_activity.lychi_h4,
            mysql_drug_activity.drug,
            mysql_drug_activity.has_moa
        ).join(mysql_t2tc, mysql_drug_activity.target_id == mysql_t2tc.target_id)
                   .join(mysql_protein, mysql_t2tc.protein_id == mysql_protein.id))

        pl_rel_dict = dict()

        for row in results:
            uniprot, lychi_h4, name, has_moa = row
            ligid = lychi_h4 or name
            key = f"{uniprot}-{ligid}"
            if key not in pl_rel_dict:
                plr = ProteinLigandRelationship(
                    start_node=Protein(id=f"{Prefix.UniProtKB}:{uniprot}"),
                    end_node=Ligand(id=ligid),
                )
                pl_rel_dict[key] = plr
            else:
                plr = pl_rel_dict[key]
            if not plr.has_moa:
                plr.has_moa = True if has_moa == 1 else False

        return list(pl_rel_dict.values())
