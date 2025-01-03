from src.interfaces.simple_enum import NodeLabel, RelationshipLabel


class GenericNodeLabel(NodeLabel):
    Analyte = "Analyte"
    Metabolite = "Metabolite"
    MetaboliteClass = "MetaboliteClass"
    MetaboliteChemProps = "MetaboliteChemProps"
    Protein = "Protein"
    Pathway = "Pathway"
    Ontology = "Ontology"
    Reaction = "Reaction"
    ReactionClass = "ReactionClass"
    DatabaseVersion = "DatabaseVersion"
    DataVersion = "DataVersion"


class GenericRelationshipLabel(RelationshipLabel):
    Analyte_Has_Class = "Analyte_Has_Class"
    Analyte_Has_Pathway = "Analyte_Has_Pathway"
    Catalyzes = "Catalyzes"
    Analyte_Has_Ontology = "Analyte_Has_Ontology"
    Reaction_Has_Class = "Reaction_Has_Class"
    ReactionClass_Has_Parent = "ReactionClass_Has_Parent"
    Metabolite_Has_Reaction = "Metabolite_Has_Reaction"
    Metabolite_Has_Chem_Prop = "Metabolite_Has_Chem_Prop"
    Protein_Has_Reaction = "Protein_Has_Reaction"
    Database_Has_Data = "Database_Has_Data"
