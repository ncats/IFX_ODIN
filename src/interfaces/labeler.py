from typing import List, Union

from src.models.analyte import Analyte
from src.models.metabolite import Metabolite
from src.models.node import Node, Relationship
from src.models.protein import Protein
from src.output_adapters.generic_labels import NodeLabel


class Labeler:

    def assign_all_labels(self, obj_list: List[Union[Node, Relationship]]):
        for obj in obj_list:
            obj.labels = self.get_labels(obj)
            if isinstance(obj, Relationship):
                obj.start_node.labels = self.get_labels(obj.start_node)
                obj.end_node.labels = self.get_labels(obj.end_node)

    def get_labels(self, obj):
        if isinstance(obj, Metabolite):
            return [NodeLabel.Metabolite, NodeLabel.Analyte]
        if isinstance(obj, Protein):
            return [NodeLabel.Protein, NodeLabel.Analyte]
        return [obj.__class__.__name__]

class AuxLabeler(Labeler):
    aux_val: str

    def __init__(self, aux_val: str):
        self.aux_val = aux_val

    def get_labels(self, obj):
        if isinstance(obj, Metabolite):
            return [f"{self.aux_val}_Metabolite", f"{self.aux_val}_Analyte"]
        if isinstance(obj, Analyte):
            return [f"{self.aux_val}_Analyte"]
        return [obj.__class__.__name__]


class ComparingLabeler(Labeler):
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

