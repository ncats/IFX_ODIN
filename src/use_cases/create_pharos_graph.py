from src.core.etl import ETL
from src.id_normalizers.uniprot_normalizer import UniProtNormalizer
from src.input_adapters.mysql_pharos.ab_count_adapter import AntibodyCountAdapter, PubMedScoreAdapter
from src.input_adapters.mysql_pharos.gene_rif_adapter import GeneRifAdapter
from src.input_adapters.mysql_pharos.go_term_adapter import GoTermAdapter, GoLeafTermAdapter
from src.input_adapters.mysql_pharos.ligand_adapter import LigandAdapter
from src.input_adapters.mysql_pharos.protein_adapter import ProteinAdapter
from src.input_adapters.mysql_pharos.protein_ligand_moa_adapter import ProteinLigandMOAAdapter
from src.input_adapters.mysql_pharos.protein_ligand_relationship_adapter import ProteinLigandRelationshipAdapter
from src.input_adapters.neo4j_pharos.tdl_input_adapter import TDLInputAdapter
from src.interfaces.labeler import PharosLabeler
from src.output_adapters.neo4j_output_adapter import Neo4jOutputAdapter
from src.use_cases.secrets.local_neo4j import stuff_neo4j_credentials as neo4j_credentials
from src.use_cases.secrets.pharos_credentials import pharos_read_credentials

uniprot_file="/Users/kelleherkj/IdeaProjects/NCATS_ODIN/input_files/target_graph/uniprot_human_reviewed.json.gz"

# upn = UniProtNormalizer(uniprot_json_path=uniprot_file)
# upn.add_labels_for_normalization_events = True

protein_adapter = ProteinAdapter(pharos_read_credentials) # .set_id_normalizer(upn)
go_term_adapter = GoTermAdapter(pharos_read_credentials)
go_leaf_term_adapter = GoLeafTermAdapter(pharos_read_credentials)
ab_count_adapter = AntibodyCountAdapter(pharos_read_credentials)
pm_score_adapter = PubMedScoreAdapter(pharos_read_credentials)
gene_rif_adapter = GeneRifAdapter(pharos_read_credentials)
ligand_adapter = LigandAdapter(pharos_read_credentials)
protein_ligand_rel_adapter = ProteinLigandRelationshipAdapter(pharos_read_credentials)
protein_ligand_moa_adapter = ProteinLigandMOAAdapter(pharos_read_credentials)

calculate_tdls_adapter = TDLInputAdapter(neo4j_credentials)

etl_output_list = [
    Neo4jOutputAdapter(credentials=neo4j_credentials)
]

etl_input_list = [
    protein_adapter,
    go_term_adapter,
    go_leaf_term_adapter,
    ab_count_adapter,
    pm_score_adapter,
    gene_rif_adapter,
    ligand_adapter,
    protein_ligand_rel_adapter,
    protein_ligand_moa_adapter
]
etl = ETL(input_adapters=etl_input_list, output_adapters=etl_output_list).set_labeler(PharosLabeler())
etl.create_or_truncate_datastores()
etl.do_etl()

post_etl_calculations_list = [
    calculate_tdls_adapter
]
etl.input_adapters = post_etl_calculations_list
etl.do_etl()