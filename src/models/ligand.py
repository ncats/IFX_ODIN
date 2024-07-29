from dataclasses import dataclass, field
from typing import List

from src.models.node import Node, Relationship
from src.models.protein import Protein


@dataclass
class Ligand(Node):
    name: str = None
    isDrug: bool = None
    smiles: str = None
    description: str = None


@dataclass
class ProteinLigandRelationship(Relationship):
    start_node: Protein
    end_node: Ligand
    has_moa: bool = None
    act_values: List[float] = field(default_factory=list)
    act_types: List[str] = field(default_factory=list)
    action_types: List[str] = field(default_factory=list)
    references: List[str] = field(default_factory=list)
    sources: List[str] = field(default_factory=list)
    pmids: List[str] = field(default_factory=list)