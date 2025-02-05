from typing import List

from src.constants import DataSourceName
from src.input_adapters.neo4j_adapter import Neo4jAdapter
from src.interfaces.input_adapter import NodeInputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import Node
from src.models.protein import TDL, Protein


class TDLInputAdapter(NodeInputAdapter, Neo4jAdapter):

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PostProcessing

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo()

    def get_all(self) -> List[Node]:
        all_protein_list = self.runQuery(all_proteins)
        good_ligand_list = self.runQuery(proteins_with_good_ligand_activities)
        moa_drug_list = self.runQuery(proteins_with_moa_drugs)
        good_go_terms_list = self.runQuery(proteins_with_experimental_f_or_p_go_terms)
        few_generifs_list = self.runQuery(proteins_with_three_or_fewer_generifs)
        low_pm_score_list = self.runQuery(proteins_with_low_pm_score)
        low_ab_count_list = self.runQuery(proteins_with_low_ab_count)

        all_protein_set = make_set(all_protein_list)
        good_ligand_set = make_set(good_ligand_list)
        moa_drug_set = make_set(moa_drug_list)
        good_go_terms_set = make_set(good_go_terms_list)
        few_generifs_set = make_set(few_generifs_list)
        low_pm_score_set = make_set(low_pm_score_list)
        low_ab_count_set = make_set(low_ab_count_list)

        nodes: List[Protein] = []
        for protein_id in all_protein_set:
            new_tdl = calculate_tdl(
                protein_id in good_ligand_set,
                protein_id in moa_drug_set,
                protein_id in good_go_terms_set,
                protein_id in few_generifs_set,
                protein_id in low_pm_score_set,
                protein_id in low_ab_count_set
            )
            nodes.append(Protein(id=protein_id, tdl=new_tdl))

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

all_proteins = """MATCH (n:`biolink:Protein`) RETURN n.id"""

proteins_with_good_ligand_activities = """
    MATCH (n:`biolink:Protein`)-[r:`biolink:interacts_with`]->(l:`biolink:ChemicalEntity`)
        WHERE r.meets_idg_cutoff = true 
    RETURN distinct n.id
"""

proteins_with_moa_drugs = """
    MATCH (n:`biolink:Protein`)-[r:`biolink:interacts_with`]->(l:`biolink:ChemicalEntity`)
        WHERE r.has_moa and l.isDrug
    RETURN distinct n.id
"""

proteins_with_experimental_f_or_p_go_terms = """
    MATCH (p:`biolink:Protein`)-[r:ProteinGoTermRelationship]->(g:GoTerm) 
    WHERE 
        g.is_leaf 
        AND g.type <> 'C'
        AND 'Experimental evidence code' in r.category
    RETURN distinct p.id
"""

proteins_with_low_pm_score = """
    MATCH (n:`biolink:Protein`)
        WHERE n.pm_score < 5 OR n.pm_score IS NULL
    RETURN DISTINCT n.id
"""

proteins_with_low_ab_count = """
    MATCH (n:`biolink:Protein`)
        WHERE n.antibody_count <= 50 OR n.antibody_count IS NULL
    RETURN DISTINCT n.id
"""

proteins_with_three_or_fewer_generifs = """
    MATCH (n:`biolink:Protein`)
        OPTIONAL MATCH (n)<-[:`biolink:translates_to`]-(g1:`biolink:Gene`) 
        OPTIONAL MATCH (n)<-[:`biolink:translates_to`]-(:`biolink:Transcript`)<-[:`biolink:transcribed_to`]-(g2:`biolink:Gene`)
    WITH n, apoc.coll.toSet(collect(g1) + collect(g2)) AS genes
    UNWIND genes AS g
        OPTIONAL MATCH (g)-[r:GeneGeneRifRelationship]-()
    WITH n, collect(DISTINCT r) AS allRelationships
        WHERE size(allRelationships) <= 3
    RETURN DISTINCT n.id
"""
