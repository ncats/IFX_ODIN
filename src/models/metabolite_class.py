from dataclasses import dataclass
from src.models.metabolite import Metabolite
from src.models.node import Node, Relationship


@dataclass
class MetaboliteClass(Node):
    level: str = None
    name: str = None
    source: str = None

    @staticmethod
    def compiled_name(level: str, name: str):
        return f'{level}-{name}'


@dataclass
class MetaboliteClassEdge(Relationship):
    start_node: Metabolite
    end_node: MetaboliteClass
    source: str = None
