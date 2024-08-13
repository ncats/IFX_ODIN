import csv
import os
from dataclasses import dataclass

from src.interfaces.simple_enum import SimpleEnum
from src.models.node import Node, Relationship
from src.models.protein import Protein

current_directory = os.path.dirname(os.path.abspath(__file__))

@dataclass
class GoEvidence:
    code: str

    # static variable
    _eco_mapping: dict = None

    @staticmethod
    def _get_eco_map():
        if GoEvidence._eco_mapping is None:
            GoEvidence._eco_mapping = dict()
            with open(os.path.join(current_directory, 'eco_mappings.csv'), 'r', encoding='utf-8-sig') as file:
                reader: csv.DictReader = csv.DictReader(file)

                for row in reader:
                    GoEvidence._eco_mapping[row['eco']] = {
                        "category": row['category'],
                        "text": row['text'],
                        "abbreviation": row['abbreviation']
                    }
        return GoEvidence._eco_mapping

    def abbreviation(self):
        return GoEvidence._get_eco_map().get(self.code)['abbreviation']

    def category(self):
        return GoEvidence._get_eco_map().get(self.code)['category']

    def text(self):
        return GoEvidence._get_eco_map().get(self.code)['text']

@dataclass
class GoType(SimpleEnum):
    Component = 'C'
    Process = 'P'
    Function = 'F'

    def __repr__(self):
        return f"{type(self).__name__}({self.name})"

    def get_abbreviation(self):
        if self.name == 'Component':
            return 'C'
        if self.name == 'Function':
            return 'F'
        if self.name == 'Process':
            return 'P'
        return '?'


@dataclass
class GoTerm(Node):
    id: str
    type: GoType = None
    term: str = None
    is_leaf: bool = None


@dataclass
class ProteinGoTermRelationship(Relationship):
    start_node: Protein
    end_node: GoTerm
    evidence: GoEvidence = None
    assigned_by: str = None
