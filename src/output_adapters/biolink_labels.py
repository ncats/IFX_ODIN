from src.interfaces.simple_enum import NodeLabel, RelationshipLabel


class BiolinkNodeLabel(NodeLabel):
    Analyte = "Analyte"
    Metabolite = "Metabolite"
    MetaboliteClass = "MetaboliteClass"
    Gene = "biolink:Gene"
    Transcript = "biolink:Transcript"
    Disease = "biolink:Disease"
    Protein = "biolink:Protein"
    Pathway = "biolink:Pathway"
    Ontology = "Ontology"
    Reaction = "Reaction"
    ReactionClass = "ReactionClass"
    Ligand = "biolink:ChemicalEntity"
    DatabaseVersion = "DatabaseVersion"
    DataVersion = "DataVersion"


class BiolinkRelationshipLabel(RelationshipLabel):
    Analyte_Has_Class = "biolink:member_of"
    Analyte_Has_Pathway = "biolink:participates_in"
    Catalyzes = "biolink:catalyzes"
    Contributes_To = "biolink:contributes_to"
    Associated_With = "biolink:associated_with"
    Analyte_Has_Ontology = "biolink:member_of"
    Reaction_Has_Class = "biolink:member_of"
    ReactionClass_Has_Parent = "biolink:subclass_of"
    Metabolite_Has_Reaction = "biolink:participates_in"
    Protein_Has_Reaction = "biolink:participates_in"
    Transcribed_To = "biolink:transcribed_to"
    Translates_To = "biolink:translates_to"
    Database_Has_Data = "Database_Has_Data"
    Has_Canonical_Isoform = "Has_Canonical_Isoform"
    Interacts_With = "biolink:interacts_with"
