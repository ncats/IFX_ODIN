from dataclasses import dataclass
from enum import Enum
from typing import List, Union


class SimpleEnum(str, Enum):
    def __str__(self):
        return self.value

    def __add__(self, other: str):
        return str.__add__(self.value, other)

    @classmethod
    def parse(cls, input_value: Union[str, Enum]):
        if isinstance(input_value, Enum):
            return input_value
        for member in cls:
            if member.value.lower() == input_value.lower().replace('_', '.'):
                return member
            if member.value.lower() == input_value.lower().replace('-', '.'):
                return member
            if member.value.lower() == input_value.lower():
                return member
        print(f"couldn't parse this value: {input_value}")

@dataclass(eq=False)
class Label:
    _known = {}
    value: str

    def __init__(self, value: str):
        self.value = value

    @classmethod
    def get(cls, value: str):
        if value in cls._known:
            return cls._known[value]
        obj = cls(value=value)
        obj.value = value
        cls._known[value] = obj
        return obj


    def __str__(self):
        return self.value

    def __repr__(self):
        return f"{self.__class__.__name__}({self.value!r})"

    def __eq__(self, other):
        return isinstance(other, Label) and self.value == other.value

    def __hash__(self):
        return hash(self.value)



    @classmethod
    def parse(cls, input_value):
        if isinstance(input_value, cls):
            return input_value
        if isinstance(input_value, str):
            return cls.get(input_value)
        raise TypeError(f"Cannot parse {input_value!r} into a {cls.__name__}")


@dataclass(eq=False)
class NodeLabel(Label):
    pass

@dataclass(eq=False)
class RelationshipLabel(Label):
    pass

