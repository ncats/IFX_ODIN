from typing import List

from src.constants import Prefix
from src.input_adapters.sql_adapter import MySqlAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.ligand import ProteinLigandRelationship, Ligand
from src.models.node import Relationship
from src.input_adapters.pharos_mysql.tables import (Ligand as mysql_ligand, LigandActivity as mysql_ligand_activity,
                                                    T2TC as mysql_t2tc, Protein as mysql_protein)
from src.models.protein import Protein


class ProteinLigandRelationshipAdapter(RelationshipInputAdapter, MySqlAdapter):
    name = "Pharos Protein Ligand Relationship Adapter"

    def get_audit_trail_entries(self, obj) -> List[str]:
        return [f"Protein Ligand Relationship from {self.credentials.schema})"]

    def get_all(self) -> List[Relationship]:
        results = (self.get_session().query(
            mysql_protein.uniprot,
            mysql_ligand.identifier,
            mysql_ligand_activity.act_value,
            mysql_ligand_activity.act_type,
            mysql_ligand_activity.action_type,
            mysql_ligand_activity.reference,
            mysql_ligand_activity.reference_source,
            mysql_ligand_activity.pubmed_ids
        ).join(mysql_t2tc, mysql_ligand_activity.target_id == mysql_t2tc.target_id)
                   .join(mysql_protein, mysql_t2tc.protein_id == mysql_protein.id)
                   .join(mysql_ligand, mysql_ligand_activity.ncats_ligand_id == mysql_ligand.id))

        pl_rel_dict = dict()

        for row in results:
            uniprot, ligid, act_value, act_type, action_type, reference, reference_source, pubmed_ids = row
            key = f"{uniprot}-{ligid}"
            if key not in pl_rel_dict:
                plr = ProteinLigandRelationship(
                    start_node=Protein(id=f"{Prefix.UniProtKB}:{uniprot}"),
                    end_node=Ligand(id=ligid),
                )
                pl_rel_dict[key] = plr
            else:
                plr = pl_rel_dict[key]
            plr.act_values.append(act_value)
            plr.act_types.append(act_type)
            plr.action_types.append(action_type)
            plr.references.append(reference)
            plr.sources.append(reference_source)
            if pubmed_ids is not None and len(pubmed_ids) > 0:
                plr.pmids.extend(pubmed_ids.split('|'))

        return list(pl_rel_dict.values())
