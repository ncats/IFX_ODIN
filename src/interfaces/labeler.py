from typing import List, Union
from src.interfaces.simple_enum import SimpleEnum
from src.models.analyte import Analyte
from src.models.disease import Disease, GeneDiseaseRelationship
from src.models.gene import Gene
from src.models.ligand import Ligand, ProteinLigandRelationship
from src.models.metabolite import Metabolite
from src.models.node import Node, Relationship
from src.models.protein import Protein
from src.models.transcript import Transcript, GeneTranscriptRelationship, TranscriptProteinRelationship, \
    GeneProteinRelationship, IsoformProteinRelationship
from src.output_adapters.biolink_labels import BiolinkNodeLabel, BiolinkRelationshipLabel
from src.output_adapters.generic_labels import GenericNodeLabel


class Labeler:
    def assign_all_labels(self, obj_list: List[Union[Node, Relationship]]):
        for obj in obj_list:
            obj.labels = self.get_labels(obj)
            if isinstance(obj, Relationship):
                obj.start_node.labels = self.get_labels(obj.start_node)
                obj.end_node.labels = list(set(self.get_labels(obj.end_node)))

    def get_labels(self, obj):
        return list({obj.__class__.__name__, *obj.labels})

class BiolinkLabeler(Labeler):
    def get_class_labels(self, obj) -> List[SimpleEnum]:
        if isinstance(obj, Gene):
            return [BiolinkNodeLabel.Gene]
        if isinstance(obj, Disease):
            return [BiolinkNodeLabel.Disease]
        if isinstance(obj, Protein):
            return [BiolinkNodeLabel.Protein]
        if isinstance(obj, Ligand):
            return [BiolinkNodeLabel.Ligand]

        if isinstance(obj, GeneDiseaseRelationship):
            return [BiolinkRelationshipLabel.Associated_With]
        if isinstance(obj, Transcript):
            return [BiolinkNodeLabel.Transcript]
        if isinstance(obj, GeneTranscriptRelationship):
            return [BiolinkRelationshipLabel.Transcribed_To]
        if isinstance(obj, TranscriptProteinRelationship) or isinstance(obj, GeneProteinRelationship):
            return [BiolinkRelationshipLabel.Translates_To]
        if isinstance(obj, IsoformProteinRelationship):
            return [BiolinkRelationshipLabel.Has_Canonical_Isoform]
        if isinstance(obj, ProteinLigandRelationship):
            return [BiolinkRelationshipLabel.Interacts_With]

        return [obj.__class__.__name__]
    def get_labels(self, obj):
        class_labels = self.get_class_labels(obj)
        return list({*class_labels, *obj.labels})


class RaMPLabeler(Labeler):
    def get_labels(self, obj):
        if isinstance(obj, Metabolite):
            return [GenericNodeLabel.Metabolite, GenericNodeLabel.Analyte]
        if isinstance(obj, Protein):
            return [GenericNodeLabel.Protein, GenericNodeLabel.Analyte]
        return [obj.__class__.__name__]


class AuxLabeler(RaMPLabeler):
    aux_val: str

    def __init__(self, aux_val: str):
        self.aux_val = aux_val

    def get_labels(self, obj):
        if isinstance(obj, Metabolite):
            return [f"{self.aux_val}_Metabolite", f"{self.aux_val}_Analyte"]
        if isinstance(obj, Analyte):
            return [f"{self.aux_val}_Analyte"]
        return [obj.__class__.__name__]


class ComparingLabeler(RaMPLabeler):
    left_labeler: AuxLabeler
    right_labeler: AuxLabeler

    def set_left_labeler(self, labeler: AuxLabeler):
        self.left_labeler = labeler
        return self

    def set_right_labeler(self, labeler: AuxLabeler):
        self.right_labeler = labeler
        return self

    def assign_all_labels(self, obj_list: List[Union[Node, Relationship]]):
        for obj in obj_list:
            obj.labels = self.get_labels(obj)
            if isinstance(obj, Relationship):
                obj.start_node.labels = self.left_labeler.get_labels(obj.start_node)
                obj.end_node.labels = self.right_labeler.get_labels(obj.end_node)
