from typing import List, Generator

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.protein import TDL, Protein, TDLMetadata
from src.shared.arango_adapter import ArangoAdapter
from src.shared.db_credentials import DBCredentials


class TDLInputAdapter(InputAdapter, ArangoAdapter):
    use_pharos_queries: bool
    batch_size: int = 1000

    def __init__(self, credentials: DBCredentials, database_name: str, use_pharos_queries: bool = False):
        ArangoAdapter.__init__(self, credentials=credentials, database_name=database_name)
        self.use_pharos_queries = use_pharos_queries

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PostProcessing

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo()

    def get_all(self) -> Generator[List[Protein], None, None]:
        all_protein_list = self.runQuery(all_proteins)
        all_protein_set = make_set(all_protein_list)

        ligand_counts = self.runQuery(ligand_activity_count)
        drug_counts = self.runQuery(moa_drug_count)
        go_term_counts = self.runQuery(experimental_f_or_p_go_term_count)
        pm_score_values = self.runQuery(pm_scores)
        ab_count_values = self.runQuery(ab_counts)
        generif_counts = self.runQuery(pharos_gene_rif_count if self.use_pharos_queries else gene_rif_count)

        ligand_counts_dict = make_dict(ligand_counts)
        drug_counts_dict = make_dict(drug_counts)
        go_term_counts_dict = make_dict(go_term_counts)
        pm_score_values_dict = make_dict(pm_score_values)
        ab_count_values_dict = make_dict(ab_count_values)
        generif_counts_dict = make_dict(generif_counts)

        nodes: List[Protein] = []
        for protein_id in all_protein_set:
            new_tdl = calculate_tdl_from_counts(
                ligand_counts_dict[protein_id],
                drug_counts_dict[protein_id],
                go_term_counts_dict[protein_id],
                generif_counts_dict[protein_id],
                pm_score_values_dict[protein_id],
                ab_count_values_dict[protein_id]
            )

            tdl_metadata = TDLMetadata(
                tdl_ligand_count=ligand_counts_dict.get(protein_id) or 0,
                tdl_drug_count=drug_counts_dict.get(protein_id) or 0,
                tdl_go_term_count=go_term_counts_dict.get(protein_id) or 0,
                tdl_generif_count=generif_counts_dict.get(protein_id) or 0,
                tdl_pm_score=pm_score_values_dict.get(protein_id) or 0,
                tdl_antibody_count=ab_count_values_dict.get(protein_id) or 0
            )

            nodes.append(Protein(id=protein_id, tdl=new_tdl, tdl_meta=tdl_metadata))

        yield nodes

def calculate_tdl_from_counts(ligand_count: int, drug_count: int, go_term_count: int, generif_count: int, pm_score: float, ab_count: int):
    if drug_count is not None and drug_count > 0:
        return TDL.Tclin
    if ligand_count is not None and ligand_count > 0:
        return TDL.Tchem
    if go_term_count is not None and go_term_count > 0:
        return TDL.Tbio
    darkPoints = 0
    if pm_score is None or pm_score < 5:
        darkPoints += 1
    if generif_count is None or generif_count <= 3:
        darkPoints += 1
    if ab_count is None or ab_count <= 50:
        darkPoints += 1
    if darkPoints >= 2:
        return TDL.Tdark
    return TDL.Tbio


def make_set(list_query_result: list):
    ret_set = set()
    for row in list_query_result:
        ret_set.add(row)
    return ret_set

def make_dict(list_query_result: list):
    return {row['protein_id']: row['value'] for row in list_query_result}

all_proteins = """
FOR n IN `Protein`
  RETURN n.id
"""

ligand_activity_count = """
FOR n IN `Protein`
  LET distinct_ligands = UNIQUE(
    FOR l, r IN OUTBOUND n `ProteinLigandEdge`
      FILTER r.meets_idg_cutoff == true
      RETURN l._id
  )
  RETURN {
    protein_id: n.id,
    value: LENGTH(distinct_ligands)
  }
"""

moa_drug_count = """
FOR pro IN `Protein`
  LET moa_drugs = UNIQUE(
    FOR lig, act IN OUTBOUND pro `ProteinLigandEdge`
      FILTER lig.isDrug == TRUE
      FILTER LENGTH(
        act.details[* FILTER CURRENT.has_moa == TRUE]
      ) > 0
      RETURN lig._id
  )
  RETURN {
    protein_id: pro.id,
    value: LENGTH(moa_drugs)
  }
"""

experimental_f_or_p_go_term_count = """
FOR p IN `Protein`
  LET go_terms = UNIQUE(
    FOR g, r IN OUTBOUND p `ProteinGoTermEdge`
      FILTER g.is_leaf == true
        AND g.type != 'C'
      LET evidence_categories = r.evidence[* RETURN CURRENT.category]
      FILTER 'Experimental evidence code' IN evidence_categories
      RETURN g._id
  )
  RETURN {
    protein_id: p.id,
    value: LENGTH(go_terms)
  }
"""

pm_scores = """
FOR p IN `Protein`
  RETURN {
    protein_id: p.id,
    value: MAX(p.pm_score)
  }
"""

ab_counts = """
FOR n IN `Protein`
  RETURN {
    protein_id: n.id,
    value: MAX(n.antibody_count)
  }
"""

pharos_gene_rif_count = """
FOR p IN `Protein`
  LET direct_keys = UNIQUE(
    FOR pub IN (p.publications || [])
      FOR gene_rif IN (pub.gene_rifs || [])
        FILTER gene_rif.text != null AND TRIM(gene_rif.text) != ""
        RETURN CONCAT_SEPARATOR("||", TO_STRING(pub.gene_id), LOWER(TRIM(gene_rif.text)))
  )
  RETURN {
    protein_id: p.id,
    value: LENGTH(direct_keys)
  }
"""

gene_rif_count = """
FOR p IN `Protein`
  LET genes = (
    FOR g IN INBOUND p `GeneProteinEdge`
      FILTER IS_SAME_COLLECTION("Gene", g)
      RETURN g
  )

  LET more_genes = (
    FOR t IN INBOUND p `TranscriptProteinEdge`
      FILTER IS_SAME_COLLECTION("Transcript", t)
      FOR g IN INBOUND t `GeneTranscriptEdge`
        FILTER IS_SAME_COLLECTION("Gene", g)
        RETURN g
  )

  LET all_genes = UNION_DISTINCT(genes, more_genes)

  LET direct_keys = (
    FOR pub IN (p.publications || [])
      FOR gene_rif IN (pub.gene_rifs || [])
        FILTER gene_rif.text != null AND TRIM(gene_rif.text) != ""
        RETURN CONCAT_SEPARATOR("||", TO_STRING(pub.gene_id), LOWER(TRIM(gene_rif.text)))
  )

  LET gene_keys = (
    FOR g IN all_genes
      FOR pub IN (g.publications || [])
        FOR gene_rif IN (pub.gene_rifs || [])
          FILTER gene_rif.text != null AND TRIM(gene_rif.text) != ""
          RETURN CONCAT_SEPARATOR("||", TO_STRING(pub.gene_id), LOWER(TRIM(gene_rif.text)))
  )

  RETURN {
    protein_id: p.id,
    value: LENGTH(UNIQUE(APPEND(direct_keys, gene_keys)))
  }
"""
