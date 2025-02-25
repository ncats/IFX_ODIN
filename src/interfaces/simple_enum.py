from enum import Enum
from typing import List, Union


class SimpleEnum(Enum):
    def __str__(self):
        return self.value

    def __add__(self, other: str):
        return str.__add__(self.value, other)

    @staticmethod
    def to_list(val_list: List[Enum], delimiter: str = "-"):
        val_list = list(set(val_list))
        return delimiter.join([val.value if hasattr(val, 'value') else val for val in val_list])


    @classmethod
    def parse(cls, input_value: Union[str, Enum]):
        if isinstance(input_value, Enum):
            return input_value
        for member in cls:
            if member.value.lower() == input_value.lower().replace('_', '.'):
                return member
            if member.value.lower() == input_value.lower():
                return member
        print(f"couldn't parse this value: {input_value}")


class NodeLabel(SimpleEnum):
    pass

class RelationshipLabel(SimpleEnum):
    pass