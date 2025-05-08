from dataclasses import dataclass

from src.interfaces.simple_enum import NodeLabel, RelationshipLabel

@dataclass(eq=False)
class BiolinkNodeLabel(NodeLabel):
    pass

BiolinkNodeLabel.Analyte = BiolinkNodeLabel.get("Analyte")
BiolinkNodeLabel.Metabolite = BiolinkNodeLabel.get("Metabolite")
BiolinkNodeLabel.MetaboliteClass = BiolinkNodeLabel.get("MetaboliteClass")
BiolinkNodeLabel.Gene = BiolinkNodeLabel.get("biolink:Gene")
BiolinkNodeLabel.Transcript = BiolinkNodeLabel.get("biolink:Transcript")
BiolinkNodeLabel.Disease = BiolinkNodeLabel.get("biolink:Disease")
BiolinkNodeLabel.Protein = BiolinkNodeLabel.get("biolink:Protein")
BiolinkNodeLabel.Pathway = BiolinkNodeLabel.get("biolink:Pathway")
BiolinkNodeLabel.Ontology = BiolinkNodeLabel.get("Ontology")
BiolinkNodeLabel.Reaction = BiolinkNodeLabel.get("Reaction")
BiolinkNodeLabel.ReactionClass = BiolinkNodeLabel.get("ReactionClass")
BiolinkNodeLabel.Ligand = BiolinkNodeLabel.get("biolink:ChemicalEntity")
BiolinkNodeLabel.DatabaseVersion = BiolinkNodeLabel.get("DatabaseVersion")
BiolinkNodeLabel.DataVersion = BiolinkNodeLabel.get("DataVersion")

@dataclass(eq=False)
class BiolinkRelationshipLabel(RelationshipLabel):
    pass

BiolinkRelationshipLabel.Analyte_Has_Class = BiolinkRelationshipLabel.get("biolink:member_of")
BiolinkRelationshipLabel.Analyte_Has_Pathway = BiolinkRelationshipLabel.get("biolink:participates_in")
BiolinkRelationshipLabel.Catalyzes = BiolinkRelationshipLabel.get("biolink:catalyzes")
BiolinkRelationshipLabel.Contributes_To = BiolinkRelationshipLabel.get("biolink:contributes_to")
BiolinkRelationshipLabel.Associated_With = BiolinkRelationshipLabel.get("biolink:associated_with")
BiolinkRelationshipLabel.Analyte_Has_Ontology = BiolinkRelationshipLabel.get("biolink:member_of")
BiolinkRelationshipLabel.Reaction_Has_Class = BiolinkRelationshipLabel.get("biolink:member_of")
BiolinkRelationshipLabel.ReactionClass_Has_Parent = BiolinkRelationshipLabel.get("biolink:subclass_of")
BiolinkRelationshipLabel.Metabolite_Has_Reaction = BiolinkRelationshipLabel.get("biolink:participates_in")
BiolinkRelationshipLabel.Protein_Has_Reaction = BiolinkRelationshipLabel.get("biolink:participates_in")
BiolinkRelationshipLabel.Transcribed_To = BiolinkRelationshipLabel.get("biolink:transcribed_to")
BiolinkRelationshipLabel.Translates_To = BiolinkRelationshipLabel.get("biolink:translates_to")
BiolinkRelationshipLabel.Database_Has_Data = BiolinkRelationshipLabel.get("Database_Has_Data")
BiolinkRelationshipLabel.Has_Canonical_Isoform = BiolinkRelationshipLabel.get("Has_Canonical_Isoform")
BiolinkRelationshipLabel.Interacts_With = BiolinkRelationshipLabel.get("biolink:interacts_with")
