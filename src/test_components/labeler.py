from dataclasses import dataclass

from src.interfaces.labeler import Labeler
from src.interfaces.simple_enum import NodeLabel

@dataclass(eq=False)
class TestLabel(NodeLabel):
    pass

TestLabel.Test = TestLabel.get("Test")
TestLabel.Node = TestLabel.get("Node")

class TestLabeler(Labeler):

    def get_labels(self, obj):
        return super().get_labels(obj)