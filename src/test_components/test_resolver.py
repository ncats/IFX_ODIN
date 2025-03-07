from typing import List, Dict

from src.interfaces.id_resolver import IdResolver, IdMatch, NoMatchBehavior, MultiMatchBehavior
from src.models.node import Node


class TestResolver(IdResolver):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def resolve_internal(self, input_nodes: List[Node]) -> Dict[str, List[IdMatch]]:
        output: Dict[str, List[IdMatch]] = {}
        for node in input_nodes:
            a_matches = [
                IdMatch(input="A", match="A1"),
                IdMatch(input="A", match="A2")
            ]
            b_matches = [
                IdMatch(input="B", match="B1"),
                IdMatch(input="B", match="B2")
            ]
            if node.id == "A":
                output[node.id] = a_matches
            elif node.id == "B":
                output[node.id] = b_matches
        return output