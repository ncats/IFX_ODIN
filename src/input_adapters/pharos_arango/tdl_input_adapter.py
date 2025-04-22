from typing import List, Generator

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.protein import TDL, Protein
from src.shared.arango_adapter import ArangoAdapter


class TDLInputAdapter(InputAdapter, ArangoAdapter):

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PostProcessing

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo()

    def get_all(self) -> Generator[List[Protein], None, None]:
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

        yield nodes

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
        ret_set.add(row)
    return ret_set

all_proteins = """
FOR n IN `biolink:Protein`
  RETURN n.id
"""

proteins_with_good_ligand_activities = """
FOR n IN `biolink:Protein`
  FOR l, r IN OUTBOUND n `biolink:interacts_with`
    FILTER r.meets_idg_cutoff == true
    RETURN DISTINCT n.id
"""

proteins_with_moa_drugs = """
FOR n IN `biolink:Protein`
  FOR l, r IN OUTBOUND n `biolink:interacts_with`
    FILTER l.isDrug == true
      AND LENGTH(r.has_moa[* FILTER CURRENT == TRUE]) > 0
    RETURN DISTINCT n.id
"""

proteins_with_experimental_f_or_p_go_terms = """
FOR p IN `biolink:Protein`
  FOR g, r IN OUTBOUND p `ProteinGoTermRelationship`
    FILTER g.is_leaf == true
      AND g.type != 'C'
      AND 'Experimental evidence code' IN r.category
    RETURN DISTINCT p.id
"""

proteins_with_low_pm_score = """
FOR n IN `biolink:Protein`
  FILTER n.pm_score < 5 OR n.pm_score == null
  RETURN DISTINCT n.id
"""

proteins_with_low_ab_count = """
FOR n IN `biolink:Protein`
  FILTER n.antibody_count <= 50 OR n.antibody_count == null
  RETURN DISTINCT n.id
"""

proteins_with_three_or_fewer_generifs = """
FOR p IN `biolink:Protein`
  LET genes = (
    FOR g IN INBOUND p `biolink:translates_to`
      FILTER g._id LIKE "biolink:Gene/%"
      RETURN g
  )
  LET more_genes = (
    FOR t IN INBOUND p `biolink:translates_to`
      FILTER t._id LIKE "biolink:Transcript/%"
      FOR g IN INBOUND t `biolink:transcribed_to`
        FILTER g._id LIKE "biolink:Gene/%"
        RETURN g
  )
  LET all_genes = UNION(genes, more_genes)
  LET all_gene_rifs = (
    FOR gene in all_genes
      FOR rif in OUTBOUND gene `GeneGeneRifRelationship`
      return rif
  )
  FILTER COUNT_DISTINCT(all_gene_rifs) <= 3
RETURN DISTINCT p.id
"""
