from dataclasses import dataclass
from typing import List, Union

from src.interfaces.simple_enum import Label
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

def default_label(obj):
    return Label.get(value = obj.__class__.__name__)


class Labeler:
    class_label_mapping = {}

    def assign_all_labels(self, obj_list: List[Union[Node, Relationship]]):
        for obj in obj_list:
            obj.labels = self.get_labels(obj)
            if isinstance(obj, Relationship):
                obj.start_node.labels = self.get_labels(obj.start_node)
                obj.end_node.labels = list(set(self.get_labels(obj.end_node)))

    def get_labels(self, obj):
        if obj.__class__ in self.class_label_mapping:
            class_labels = self.class_label_mapping[obj.__class__]
        else:
            class_labels = [default_label(obj)]
        return list({*class_labels, *obj.labels})

    def get_classes(self, label, keepClass: bool = False):
        class_list = []
        for class_name, labels in self.class_label_mapping.items():
            label_str = [label.value for label in labels]
            if label in label_str:
                if keepClass:
                    class_list.append(class_name)
                else:
                    class_list.append(class_name.__name__)
        if len(class_list) > 0:
            return class_list
        return [label]

    def get_labels_for_class_name(self, class_name):
        for cls, labels in self.class_label_mapping.items():
            if cls.__name__ == class_name:
                return [label.value for label in labels]
        return [class_name]


class BiolinkLabeler(Labeler):

    class_label_mapping = {
        Gene: [BiolinkNodeLabel.Gene],
        Disease: [BiolinkNodeLabel.Disease],
        Protein: [BiolinkNodeLabel.Protein],
        Ligand: [BiolinkNodeLabel.Ligand],
        GeneDiseaseRelationship: [BiolinkRelationshipLabel.Associated_With],
        Transcript: [BiolinkNodeLabel.Transcript],
        GeneTranscriptRelationship: [BiolinkRelationshipLabel.Transcribed_To],
        TranscriptProteinRelationship: [BiolinkRelationshipLabel.Translates_To],
        GeneProteinRelationship: [BiolinkRelationshipLabel.Translates_To],
        IsoformProteinRelationship: [BiolinkRelationshipLabel.Has_Canonical_Isoform],
        ProteinLigandRelationship: [BiolinkRelationshipLabel.Interacts_With],
    }


class RaMPLabeler(Labeler):
    class_label_mapping = {
        Metabolite: [GenericNodeLabel.Metabolite, GenericNodeLabel.Analyte],
        Protein: [GenericNodeLabel.Protein, GenericNodeLabel.Analyte]
    }


class AuxLabeler(RaMPLabeler):
    aux_val: str

    def __init__(self, aux_val: str):
        self.aux_val = aux_val

    def get_labels(self, obj):
        if isinstance(obj, Metabolite):
            return [f"{self.aux_val}_Metabolite", f"{self.aux_val}_Analyte"]
        if isinstance(obj, Analyte):
            return [f"{self.aux_val}_Analyte"]
        return [default_label(obj)]


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
