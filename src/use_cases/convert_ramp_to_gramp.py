import time

from src.input_adapters.analyte_equivalent_id_adapter import AnalyteEquivalentIDAdapter
from src.input_adapters.analyte_ontology_relationship_adapter import AnalyteOntologyRelationshipAdapter
from src.input_adapters.analyte_pathway_relationship_adapter import AnalytePathwayRelationshipAdapter
from src.input_adapters.analyte_synonym_adapter import AnalyteSynonymAdapter
from src.input_adapters.gene_adapter import GeneAdapter
from src.input_adapters.metabolite_adapter import MetaboliteAdapter
from src.input_adapters.metabolite_class_adapter import MetaboliteClassAdapter
from src.input_adapters.metabolite_class_relationship_adapter import MetaboliteClassRelationshipAdapter
from src.input_adapters.metabolite_gene_relationship_adapter import MetaboliteGeneRelationshipAdapter
from src.input_adapters.metabolite_reaction_relationship_adapter import MetaboliteReactionRelationshipAdapter
from src.input_adapters.ontology_adapter import OntologyAdapter
from src.input_adapters.pathway_adapter import PathwayAdapter
from src.input_adapters.protein_reaction_relationship_adapter import ProteinReactionRelationshipAdapter
from src.input_adapters.reaction_adapter import ReactionAdapter
from src.input_adapters.reaction_class_relationship_adapter import ReactionClassRelationshipAdapter
from src.input_adapters.reaction_reaction_class_relationship_adapter import ReactionReactionClassRelationshipAdapter
from src.input_adapters.version_metadata_adapter import VersionMetaAdapter
from src.output_adapters.neo4j_csv_output_adapter import Neo4jCsvOutputAdapter
from src.input_adapters.reaction_class_adapter import ReactionClassAdapter

sqlite_file="/Users/kelleherkj/IdeaProjects/RaMP-DB-clean/db/RaMP_SQLite_v2.5.4.sqlite"

# Input Adapters
met_adapter = MetaboliteAdapter(sqlite_file=sqlite_file)
met_class_adapter = MetaboliteClassAdapter(sqlite_file=sqlite_file)
met_class_relationship_adapter = MetaboliteClassRelationshipAdapter(sqlite_file=sqlite_file)
gene_adapter = GeneAdapter(sqlite_file=sqlite_file)
pathway_adapter = PathwayAdapter(sqlite_file=sqlite_file)
analyte_pathway_relationship_adapter = AnalytePathwayRelationshipAdapter(sqlite_file=sqlite_file)
metabolite_gene_relationship_adapter = MetaboliteGeneRelationshipAdapter(sqlite_file=sqlite_file)
ontology_adapter = OntologyAdapter(sqlite_file=sqlite_file)
analyte_ontology_relationship_adapter = AnalyteOntologyRelationshipAdapter(sqlite_file=sqlite_file)
reaction_adapter = ReactionAdapter(sqlite_file=sqlite_file)
reaction_class_adapter = ReactionClassAdapter(sqlite_file=sqlite_file)
reaction_class_relationship_adapter = ReactionClassRelationshipAdapter(sqlite_file=sqlite_file)
metabolite_reaction_relationship_adapter = MetaboliteReactionRelationshipAdapter(sqlite_file=sqlite_file)
reaction_reaction_class_relationship_adapter = ReactionReactionClassRelationshipAdapter(sqlite_file=sqlite_file)
gene_reaction_relationship_adapter = ProteinReactionRelationshipAdapter(sqlite_file=sqlite_file)
analyte_synonym_list_adapter = AnalyteSynonymAdapter(sqlite_file=sqlite_file)
analyte_equivalent_id_adapter = AnalyteEquivalentIDAdapter(sqlite_file=sqlite_file)
version_meta_adapter = VersionMetaAdapter(sqlite_file=sqlite_file)

# Output Adapters
csv_output_adapter = Neo4jCsvOutputAdapter(destination_directory="./output_files/ramp_neo4j_output")
csv_output_adapter.create_or_truncate_datastore()

etl_input_list = [
    met_adapter,
    met_class_adapter,
    met_class_relationship_adapter,
    gene_adapter,
    analyte_synonym_list_adapter,
    analyte_equivalent_id_adapter,
    metabolite_gene_relationship_adapter,
    pathway_adapter,
    analyte_pathway_relationship_adapter,
    ontology_adapter,
    analyte_ontology_relationship_adapter,
    reaction_adapter,
    reaction_class_adapter,
    reaction_class_relationship_adapter,
    reaction_reaction_class_relationship_adapter,
    metabolite_reaction_relationship_adapter,
    gene_reaction_relationship_adapter,
    version_meta_adapter
]

total_start_time = time.time()
for input_adapter in etl_input_list:
    print(f"Running: {input_adapter.name}")
    start_time = time.time()
    count = 0
    for each in input_adapter.next():
        csv_output_adapter.store(each)
        count += 1
    elapsed_time = time.time() - start_time
    print(f"\tElapsed time: {elapsed_time:.4f} seconds writing {count} lines")

total_elapsed_time = time.time() - total_start_time
print(f"\tTotal elapsed time: {total_elapsed_time:.4f} seconds")