from src.interfaces.labeler import Labeler
from src.interfaces.simple_enum import NodeLabel

class TestLabel(NodeLabel):
    Test = "Test"
    Node = "Node"

class TestLabeler(Labeler):

    def get_labels(self, obj):
        return super().get_labels(obj)