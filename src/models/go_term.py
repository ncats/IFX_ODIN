import csv
import os
from dataclasses import dataclass, field
from typing import List, Optional

from src.interfaces.simple_enum import SimpleEnum
from src.models.node import Node, Relationship
from src.models.protein import Protein

current_directory = os.path.dirname(os.path.abspath(__file__))

_eco_mapping: dict = None

@dataclass
class GoEvidence:
    code: str
    assigned_by: str

    def to_dict(self):
        ret_dict = {}
        ret_dict['evidence'] = self.code
        ret_dict['abbreviation'] = self.abbreviation()
        ret_dict['category'] = self.category()
        ret_dict['text'] = self.text()
        ret_dict['assigned_by'] = self.assigned_by
        return ret_dict

    @classmethod
    def from_dict(cls, data: dict):
        return GoEvidence(code=data.get('evidence'), assigned_by=data.get('assigned_by'))

    @staticmethod
    def no_data_codes():
        return ['ND']

    @staticmethod
    def _get_eco_map():
        global _eco_mapping
        if _eco_mapping is None:
            _eco_mapping = {}
            with open(os.path.join(current_directory, 'eco_mappings.csv'), 'r', encoding='utf-8-sig') as file:
                reader: csv.DictReader = csv.DictReader(file)

                for row in reader:
                    eco_obj = {
                        "category": row['category'],
                        "text": row['text'],
                        "abbreviation": row['abbreviation']
                    }
                    _eco_mapping[row['eco']] = eco_obj
                    _eco_mapping[row['abbreviation']] = eco_obj

        return _eco_mapping

    @staticmethod
    def parse_by_abbreviation(abbreviation: str, assigned_by: str):
        for key, value in GoEvidence._get_eco_map().items():
            if value['abbreviation'] == abbreviation:
                return GoEvidence(code=key, assigned_by=assigned_by)
        return None

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


    @classmethod
    def parse(cls, input_value: str):
        if input_value == 'biological_process':
            return GoType.Process
        if input_value == 'cellular_component':
            return GoType.Component
        if input_value == 'molecular_function':
            return GoType.Function
        return super().parse(input_value)



@dataclass
class GoTerm(Node):
    id: str
    type: GoType = None
    term: str = None
    definition: Optional[str] = None
    subsets: Optional[List[str]] = field(default_factory=list)
    is_leaf: Optional[bool] = None

@dataclass
class GoTermHasParent(Relationship):
    start_node: GoTerm
    end_node: GoTerm



@dataclass
class ProteinGoTermRelationship(Relationship):
    start_node: Protein
    end_node: GoTerm
    evidence: List[GoEvidence] = field(default_factory=list)
