from src.core.etl import ETL
from src.input_adapters.sqlite_ramp.analyte_ontology_relationship_adapter import MetaboliteOntologyRelationshipAdapter
from src.input_adapters.sqlite_ramp.analyte_pathway_relationship_adapter import MetabolitePathwayRelationshipAdapter, ProteinPathwayRelationshipAdapter
from src.input_adapters.sqlite_ramp.analyte_synonym_adapter import MetaboliteSynonymAdapter, ProteinSynonymAdapter
from src.input_adapters.sqlite_ramp.metabolite_chem_props_adapter import MetaboliteChemPropsAdapter
from src.input_adapters.sqlite_ramp.protein_adapter import ProteinAdapter
from src.input_adapters.sqlite_ramp.metabolite_adapter import MetaboliteAdapter
from src.input_adapters.sqlite_ramp.metabolite_class_adapter import MetaboliteClassAdapter
from src.input_adapters.sqlite_ramp.metabolite_class_relationship_adapter import MetaboliteClassRelationshipAdapter
from src.input_adapters.sqlite_ramp.metabolite_protein_relationship_adapter import MetaboliteProteinRelationshipAdapter
from src.input_adapters.sqlite_ramp.metabolite_reaction_relationship_adapter import MetaboliteReactionRelationshipAdapter
from src.input_adapters.sqlite_ramp.ontology_adapter import OntologyAdapter
from src.input_adapters.sqlite_ramp.pathway_adapter import PathwayAdapter
from src.input_adapters.sqlite_ramp.protein_reaction_relationship_adapter import ProteinReactionRelationshipAdapter
from src.input_adapters.sqlite_ramp.reaction_adapter import ReactionAdapter
from src.input_adapters.sqlite_ramp.reaction_class_relationship_adapter import ReactionClassRelationshipAdapter
from src.input_adapters.sqlite_ramp.reaction_reaction_class_relationship_adapter import ReactionReactionClassRelationshipAdapter
from src.input_adapters.sqlite_ramp.version_metadata_adapter import VersionMetaAdapter
from src.input_adapters.sqlite_ramp.reaction_class_adapter import ReactionClassAdapter
from src.output_adapters.neo4j_output_adapter import Neo4jOutputAdapter
from src.use_cases.secrets.local_neo4j import stuff2_neo4j_credentials as neo4j_credentials

sqlite_file="/Users/kelleherkj/IdeaProjects/RaMP-DB-clean/db/RaMP_SQLite_v2.5.4.sqlite"

# Input Adapters
met_adapter = MetaboliteAdapter(sqlite_file=sqlite_file)
met_chem_props_adapter = MetaboliteChemPropsAdapter(sqlite_file=sqlite_file)
met_class_adapter = MetaboliteClassAdapter(sqlite_file=sqlite_file)
met_class_relationship_adapter = MetaboliteClassRelationshipAdapter(sqlite_file=sqlite_file)
protein_adapter = ProteinAdapter(sqlite_file=sqlite_file)
metabolite_protein_relationship_adapter = MetaboliteProteinRelationshipAdapter(sqlite_file=sqlite_file)
pathway_adapter = PathwayAdapter(sqlite_file=sqlite_file)
metabolite_pathway_relationship_adapter = MetabolitePathwayRelationshipAdapter(sqlite_file=sqlite_file)
protein_pathway_relationship_adapter = ProteinPathwayRelationshipAdapter(sqlite_file=sqlite_file)
ontology_adapter = OntologyAdapter(sqlite_file=sqlite_file)
analyte_ontology_relationship_adapter = MetaboliteOntologyRelationshipAdapter(sqlite_file=sqlite_file)
reaction_adapter = ReactionAdapter(sqlite_file=sqlite_file)
reaction_class_adapter = ReactionClassAdapter(sqlite_file=sqlite_file)
reaction_class_relationship_adapter = ReactionClassRelationshipAdapter(sqlite_file=sqlite_file)
metabolite_reaction_relationship_adapter = MetaboliteReactionRelationshipAdapter(sqlite_file=sqlite_file)
reaction_reaction_class_relationship_adapter = ReactionReactionClassRelationshipAdapter(sqlite_file=sqlite_file)
protein_reaction_relationship_adapter = ProteinReactionRelationshipAdapter(sqlite_file=sqlite_file)
metabolite_synonym_list_adapter = MetaboliteSynonymAdapter(sqlite_file=sqlite_file)
protein_synonym_list_adapter = ProteinSynonymAdapter(sqlite_file=sqlite_file)
version_meta_adapter = VersionMetaAdapter(sqlite_file=sqlite_file)

# Output Adapters
neo4j_output_adapter = Neo4jOutputAdapter(credentials=neo4j_credentials)


etl_input_list = [
    met_adapter,
    met_chem_props_adapter,
    met_class_adapter,
    met_class_relationship_adapter,
    protein_adapter,
    metabolite_synonym_list_adapter,
    protein_synonym_list_adapter,
    metabolite_protein_relationship_adapter,
    pathway_adapter,
    metabolite_pathway_relationship_adapter,
    protein_pathway_relationship_adapter,
    ontology_adapter,
    analyte_ontology_relationship_adapter,
    reaction_adapter,
    reaction_class_adapter,
    reaction_class_relationship_adapter,
    reaction_reaction_class_relationship_adapter,
    metabolite_reaction_relationship_adapter,
    protein_reaction_relationship_adapter,
    version_meta_adapter
]

etl_output_list = [
    neo4j_output_adapter
]

etl = ETL(input_adapters=etl_input_list, output_adapters=etl_output_list)

etl.create_or_truncate_datastores()

etl.do_etl()
