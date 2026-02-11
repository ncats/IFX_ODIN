from typing import List, Union

from src.constants import Prefix
from src.input_adapters.sql_adapter import MySqlAdapter
from src.input_adapters.pharos_mysql.old_tables import Protein as mysql_Protein, GoA as mysql_Goa, TDL_info as mysql_tdlInfo
from src.interfaces.input_adapter import InputAdapter
from src.models.go_term import GoTerm, ProteinGoTermRelationship, GoEvidence
from src.models.protein import Protein

class GoTermAdapter(InputAdapter, MySqlAdapter):

    def get_all(self):
        results = self.get_session().query(
            mysql_Goa.go_type,
            mysql_Goa.go_id,
            mysql_Goa.go_term_text,
            mysql_Goa.evidence,
            mysql_Goa.goeco,
            mysql_Goa.assigned_by,
            mysql_Protein.uniprot,
        ).join(mysql_Protein, mysql_Protein.id == mysql_Goa.protein_id)

        go_dict = dict()
        nodes = []
        relationships = []
        for row in results:
            go_type, go_id, go_text, evidence, eco, assigned_by, uniprot = row
            if go_id not in go_dict:
                go_term_obj = GoTerm(id=go_id, type=go_type, term=go_text)
                go_dict[go_id] = go_term_obj
                nodes.append(go_term_obj)
            else:
                go_term_obj = go_dict[go_id]

            relationships.append(ProteinGoTermRelationship(
                start_node=Protein(id=f"{Prefix.UniProtKB}:{uniprot}"),
                end_node=go_term_obj,
                evidence=[GoEvidence(eco, assigned_by=assigned_by)]
            ))

        return [*nodes, *relationships]

class GoLeafTermAdapter(InputAdapter, MySqlAdapter):

    def get_all(self):

        results = self.get_session().query(
            mysql_tdlInfo.string_value
        ).where(mysql_tdlInfo.itype == "Experimental MF/BP Leaf Term GOA")

        leaf_go_terms = set()

        for row in results:
            string_val = row[0]
            go_string_list = string_val.split(';')
            for go_string_val in go_string_list:
                pieces = go_string_val.split('|')
                leaf_go_terms.add(pieces[0].strip())

        return [GoTerm(id=val, is_leaf=True) for val in leaf_go_terms]
