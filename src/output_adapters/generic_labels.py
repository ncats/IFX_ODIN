from dataclasses import dataclass

from src.interfaces.simple_enum import NodeLabel, RelationshipLabel

@dataclass(eq=False)
class GenericNodeLabel(NodeLabel):
    pass

GenericNodeLabel.Analyte = GenericNodeLabel.get("Analyte")
GenericNodeLabel.Metabolite = GenericNodeLabel.get("Metabolite")
GenericNodeLabel.MetaboliteClass = GenericNodeLabel.get("MetaboliteClass")
GenericNodeLabel.MetaboliteChemProps = GenericNodeLabel.get("MetaboliteChemProps")
GenericNodeLabel.Protein = GenericNodeLabel.get("Protein")
GenericNodeLabel.Pathway = GenericNodeLabel.get("Pathway")
GenericNodeLabel.Ontology = GenericNodeLabel.get("Ontology")
GenericNodeLabel.Reaction = GenericNodeLabel.get("Reaction")
GenericNodeLabel.ReactionClass = GenericNodeLabel.get("ReactionClass")
GenericNodeLabel.DatabaseVersion = GenericNodeLabel.get("DatabaseVersion")
GenericNodeLabel.DataVersion = GenericNodeLabel.get("DataVersion")

@dataclass(eq=False)
class GenericRelationshipLabel(RelationshipLabel):
    pass

GenericRelationshipLabel.Analyte_Has_Class = GenericRelationshipLabel.get("Analyte_Has_Class")
GenericRelationshipLabel.Analyte_Has_Pathway = GenericRelationshipLabel.get("Analyte_Has_Pathway")
GenericRelationshipLabel.Catalyzes = GenericRelationshipLabel.get("Catalyzes")
GenericRelationshipLabel.Analyte_Has_Ontology = GenericRelationshipLabel.get("Analyte_Has_Ontology")
GenericRelationshipLabel.Reaction_Has_Class = GenericRelationshipLabel.get("Reaction_Has_Class")
GenericRelationshipLabel.ReactionClass_Has_Parent = GenericRelationshipLabel.get("ReactionClass_Has_Parent")
GenericRelationshipLabel.Metabolite_Has_Reaction = GenericRelationshipLabel.get("Metabolite_Has_Reaction")
GenericRelationshipLabel.Metabolite_Has_Chem_Prop = GenericRelationshipLabel.get("Metabolite_Has_Chem_Prop")
GenericRelationshipLabel.Protein_Has_Reaction = GenericRelationshipLabel.get("Protein_Has_Reaction")
GenericRelationshipLabel.Database_Has_Data = GenericRelationshipLabel.get("Database_Has_Data")
