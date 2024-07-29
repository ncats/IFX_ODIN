from typing import List

from src.input_adapters.neo4j_adapter import Neo4jAdapter
from src.interfaces.input_adapter import NodeInputAdapter
from src.models.node import Node
from src.models.protein import TDL, Protein


class TDLInputAdapter(NodeInputAdapter, Neo4jAdapter):
    name = "Neo4j Pharos TDL Adapter"

    def get_audit_trail_entries(self, obj) -> List[str]:
        return [f'tdl updated by {self.name} using {self.credentials.url}']

    def get_all(self) -> List[Node]:
        all_proteins_and_old_tdls = self.runQuery(proteins_and_their_old_tdls)

        good_ligand_list = self.runQuery(proteins_with_good_ligand_activities)
        moa_drug_list = self.runQuery(proteins_with_moa_drugs)
        good_go_terms_list = self.runQuery(proteins_with_experimental_f_or_p_go_terms)
        few_generifs_list = self.runQuery(proteins_with_three_or_fewer_generifs)
        low_pm_score_list = self.runQuery(proteins_with_low_pm_score)
        low_ab_count_list = self.runQuery(proteins_with_low_ab_count)

        good_ligand_set = make_set(good_ligand_list)
        moa_drug_set = make_set(moa_drug_list)
        good_go_terms_set = make_set(good_go_terms_list)
        few_generifs_set = make_set(few_generifs_list)
        low_pm_score_set = make_set(low_pm_score_list)
        low_ab_count_set = make_set(low_ab_count_list)

        nodes: List[Protein] = []
        for row in all_proteins_and_old_tdls:
            uniprot, old_tdl = row
            new_tdl = calculate_tdl(uniprot in good_ligand_set, uniprot in moa_drug_set, uniprot in good_go_terms_set,
                                    uniprot in few_generifs_set, uniprot in low_pm_score_set, uniprot in low_ab_count_set)
            if new_tdl.value != old_tdl:
                print(f"updating: {uniprot} from {old_tdl} to {new_tdl}")
                nodes.append(Protein(id=uniprot, tdl=new_tdl))
        print(f"found {len(nodes)} with changed TDLs")
        return nodes

def calculate_tdl(has_ligand: bool, has_moa_drug: bool, has_good_go_term: bool, has_few_generifs: bool, has_low_pm_score: bool, has_low_ab_score: bool):
    if has_moa_drug:
        return TDL.Tclin
    if has_ligand:
        return TDL.Tchem
    if has_good_go_term:
        return TDL.Tbio
    darkPoints = 0
    if has_low_pm_score:
        darkPoints += 1
    if has_few_generifs:
        darkPoints += 1
    if has_low_ab_score:
        darkPoints += 1
    if darkPoints >= 2:
        return TDL.Tdark
    return TDL.Tbio

def make_set(list_query_result: list):
    ret_set = set()
    for row in list_query_result:
        ret_set.add(row[0])
    return ret_set

proteins_and_their_old_tdls = """
    MATCH (n:Protein)
    RETURN n.id, n.tdl
"""

proteins_with_good_ligand_activities = """
    MATCH (n:Protein)-[r:ProteinLigandRelationship]->(l:Ligand)
    WHERE (n.idg_family = 'GPCR' AND ANY(value IN r.act_values WHERE value >= 7))
        OR (n.idg_family = 'Kinase' AND ANY(value IN r.act_values WHERE value >= 7.52288))
        OR (n.idg_family = 'Ion Channel' AND ANY(value IN r.act_values WHERE value >= 5))
        OR (NOT n.idg_family IN ['Ion Channel', 'Kinase', 'GPCR'] AND ANY(value IN r.act_values WHERE value >= 6))
    RETURN DISTINCT n.id
"""

proteins_with_moa_drugs = """
    MATCH (n:Protein)-[r:ProteinLigandRelationship]->(l:Ligand)
    WHERE r.has_moa and l.isDrug = 1
    RETURN DISTINCT n.id
"""

proteins_with_experimental_f_or_p_go_terms = """
    MATCH (p:Protein)-[r:ProteinGoTermRelationship]->(g:GoTerm) 
    WHERE (g.is_leaf AND  g.type <> 'Component')
        AND ANY(code IN r.abbreviation WHERE code IN ['EXP', 'IDA', 'IPI', 'IMP', 'IGI', 'IEP'])
    RETURN DISTINCT p.id
"""

proteins_with_low_pm_score = """
    MATCH (n:Protein)
    WHERE n.pm_score < 5
    RETURN DISTINCT n.id
"""

proteins_with_low_ab_count = """
    MATCH (n:Protein)
    WHERE n.antibody_count <= 50
    RETURN DISTINCT n.id
"""

proteins_with_three_or_fewer_generifs = """
    MATCH (n:Protein)
    OPTIONAL MATCH (n)-[r:ProteinGeneRifRelationship]-()
    WITH n, COUNT(r) as relCount
    WHERE relCount <= 3
    RETURN DISTINCT n.id
"""





