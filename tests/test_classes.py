from dataclasses import dataclass
from enum import Enum
from typing import List, Union


class NonDataClass:
    id: str


@dataclass
class SimpleFieldClass:
    id: str
    num: int
    yes: bool
    dec: float


@dataclass
class ListFieldClass:
    id:  List[str]
    num: List[int]
    yes: List[bool]
    dec: List[float]

@dataclass
class ParentClass:
    id: str
    data: str = None

@dataclass
class ChildClass:
    id: str
    data: str
    parent: ParentClass = None

@dataclass
class ChildClass2:
    id: str
    parents: List[ParentClass] = None

class Category(Enum):
    A = "A"
    B = "B"
    C = "C"

@dataclass
class ClassWithEnum:
    id: str
    cat: Category
    cats: List[Category]

@dataclass
class ClassWithNestedEnumClass:
    id: str
    cwe: ClassWithEnum

@dataclass
class ClassWithUnion:
    id: str
    optional: Union[str, None]
    mixed: Union[str, int]
    two_dc: Union[SimpleFieldClass, ChildClass]
    two_mixed: Union[int, SimpleFieldClass]
