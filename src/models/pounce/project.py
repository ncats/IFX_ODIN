from dataclasses import dataclass, field, asdict
from typing import List, Dict, Union, TYPE_CHECKING
from src.interfaces.simple_enum import SimpleEnum
from src.models.node import Node, Relationship
from datetime import date

if TYPE_CHECKING:
    from src.models.pounce.biosample import Biosample

class AccessLevel(SimpleEnum):
    private = "private"
    ncats = "ncats"
    public = "public"

@dataclass
class Person(Node):
    name: str = None
    email: str = None

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)

    @classmethod
    def make(cls, name: str, email: str = None) -> 'Person':
        person_id = email if email else name.replace(' ', '_')
        return cls(id=person_id.lower(), name=name, email=email)


@dataclass
class Project(Node):
    name: str = None
    description: str = None
    date: date = None
    lab_groups: List[str] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)
    access: AccessLevel = None
    project_type: List[str] = field(default_factory=list)
    rare_disease_focus: bool = None
    sample_preparation: str = None

    @staticmethod
    def id_from_display_id(display_id: str) -> int:
        try:
            return int(display_id.split('_')[-1])
        except (ValueError, IndexError):
            raise ValueError(f"Invalid project display ID format: {display_id}")

    @staticmethod
    def display_id_from_id(id: Union[str, int]) -> str:
        if isinstance(id, str):
            id = int(id)
        return f"pounce_proj_{id:05d}"

@dataclass
class ProjectPersonEdge(Relationship):
    start_node: Project
    end_node: Person
    role: str = None


@dataclass
class ProjectBiosampleEdge(Relationship):
    start_node: "Project" = None
    end_node: "Biosample" = None