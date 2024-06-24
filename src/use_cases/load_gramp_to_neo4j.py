import time

from src.output_adapters.generic_labels import NodeLabel, RelationshipLabel
from src.shared.neo4j_data_loader import Neo4jDataLoader

loader = Neo4jDataLoader(base_path="/Users/kelleherkj/IdeaProjects/NCATS_ODIN/output_files/ramp_neo4j_output/")

metabolite_class_csv = "MetaboliteClass.csv"
metabolite_csv = "Metabolite.csv"
metabolite_chem_props_csv = "MetaboliteChemProps.csv"
metabolite_chem_prop_relationships_csv = "MetaboliteChemPropsRelationship.csv"
metabolite_relationship_csv = "MetaboliteClassRelationship.csv"
protein_csv = "Protein.csv"
pathway_csv = "Pathway.csv"
analyte_relationship_csv = "AnalytePathwayRelationship.csv"
metabolite_protein_relationship_csv = "MetaboliteProteinRelationship.csv"
ontology_csv = "Ontology.csv"
analyte_ontology_relationship = "AnalyteOntologyRelationship.csv"
reaction_csv = "Reaction.csv"
reaction_class_csv = "ReactionClass.csv"
reaction_class_parent_csv = "ReactionClassParentRelationship.csv"
metabolite_reaction_relationship_csv = "MetaboliteReactionRelationship.csv"
reaction_reaction_class_relationship_csv = "ReactionReactionClassRelationship.csv"
protein_reaction_relationship_csv = "ProteinReactionRelationship.csv"
analyte_synonym_csv = "Analyte.synonyms.csv"
analyte_equiv_id_csv = "Analyte.equivalent_ids.csv"
database_version_csv = "DatabaseVersion.csv"
data_version_csv = "DataVersion.csv"
database_data_version_relationship_csv = "DatabaseDataVersionRelationship.csv"


def import_data():
    with loader.driver.session() as session:
        loader.load_node_csv(session, protein_csv, [NodeLabel.Protein, NodeLabel.Analyte])
        loader.load_node_csv(session, metabolite_csv, [NodeLabel.Metabolite, NodeLabel.Analyte])
        loader.load_node_csv(session, metabolite_chem_props_csv, NodeLabel.MetaboliteChemProps)
        loader.load_relationship_csv(session, metabolite_chem_prop_relationships_csv,
                              NodeLabel.Metabolite, RelationshipLabel.Metabolite_Has_Chem_Prop, NodeLabel.MetaboliteChemProps)
        loader.load_node_csv(session, analyte_synonym_csv, NodeLabel.Analyte)
        loader.load_node_csv(session, analyte_equiv_id_csv, NodeLabel.Analyte)
        loader.add_index(session, NodeLabel.Analyte, "equivalent_ids")

        loader.load_node_csv(session, metabolite_class_csv, NodeLabel.MetaboliteClass)
        loader.load_node_csv(session, pathway_csv, NodeLabel.Pathway)
        loader.load_node_csv(session, ontology_csv, NodeLabel.Ontology)
        loader.load_node_csv(session, reaction_csv, NodeLabel.Reaction)
        loader.load_node_csv(session, reaction_class_csv, NodeLabel.ReactionClass)

        loader.load_relationship_csv(session, metabolite_relationship_csv,
                              NodeLabel.Metabolite, RelationshipLabel.Analyte_Has_Class, NodeLabel.MetaboliteClass)
        loader.load_relationship_csv(session, metabolite_protein_relationship_csv,
                              NodeLabel.Metabolite, RelationshipLabel.Catalyzes, NodeLabel.Protein)
        loader.load_relationship_csv(session, analyte_relationship_csv,
                              NodeLabel.Analyte, RelationshipLabel.Analyte_Has_Pathway, NodeLabel.Pathway)
        loader.load_relationship_csv(session, analyte_ontology_relationship,
                              NodeLabel.Analyte, RelationshipLabel.Analyte_Has_Ontology, NodeLabel.Ontology)

        loader.load_relationship_csv(session, reaction_class_parent_csv,
                              NodeLabel.ReactionClass, RelationshipLabel.ReactionClass_Has_Parent, NodeLabel.ReactionClass)
        loader.load_relationship_csv(session, metabolite_reaction_relationship_csv,
                              NodeLabel.Metabolite, RelationshipLabel.Metabolite_Has_Reaction, NodeLabel.Reaction)
        loader.load_relationship_csv(session, reaction_reaction_class_relationship_csv,
                              NodeLabel.Reaction, RelationshipLabel.Reaction_Has_Class, NodeLabel.ReactionClass)
        loader.load_relationship_csv(session, protein_reaction_relationship_csv,
                              NodeLabel.Protein, RelationshipLabel.Protein_Has_Reaction, NodeLabel.Reaction)

        load_metadata(session)


def load_metadata(session):
    loader.load_node_csv(session, database_version_csv, NodeLabel.DatabaseVersion)
    loader.load_node_csv(session, data_version_csv, NodeLabel.DataVersion)
    loader.load_relationship_csv(session, database_data_version_relationship_csv,
                          NodeLabel.DatabaseVersion, RelationshipLabel.Database_Has_Data, NodeLabel.DataVersion)


if __name__ == "__main__":
    start_time = time.time()

    loader.delete_all_data_and_indexes()
    print(f"\tClean up time: {time.time() - start_time:.4f} seconds")

    import_data()
    loader.driver.close()
    print(f"\tTotal elapsed time: {time.time() - start_time:.4f} seconds")
