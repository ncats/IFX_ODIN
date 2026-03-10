from enum import Enum
from typing import Union


class SimpleEnum(str, Enum):
    def __str__(self):
        return self.value

    def __add__(self, other: str):
        return str.__add__(self.value, other)

    @classmethod
    def parse(cls, input_value: Union[str, Enum]):
        if input_value is None:
            return None
        if isinstance(input_value, Enum):
            return input_value
        for member in cls:
            if member.value.lower() == input_value.strip().lower().replace('_', '.'):
                return member
            if member.value.lower() == input_value.strip().lower().replace('-', '.'):
                return member
            if member.value.lower() == input_value.strip().lower():
                return member
        print(f"couldn't parse this value: {input_value}")


class LabeledIntEnum(int, Enum):
    """Enum whose .value is a stable integer ID (stored in DB) and .label is the display string.

    Use this when a controlled vocabulary needs a durable integer key in MySQL.
    The integer .value is serialized to ArangoDB and MySQL child tables.

    Usage::

        class MyType(LabeledIntEnum):
            FOO = (1, "Foo Display Name")
            BAR = (2, "Bar Display Name")
    """

    def __new__(cls, id: int, label: str):
        obj = int.__new__(cls, id)
        obj._value_ = id
        obj.label = label
        return obj

    def __str__(self):
        return self.label

    @classmethod
    def parse(cls, input_value: Union[str, int, 'LabeledIntEnum', None]):
        if input_value is None:
            return None
        if isinstance(input_value, cls):
            return input_value
        if isinstance(input_value, int):
            for member in cls:
                if member.value == input_value:
                    return member
        if isinstance(input_value, str):
            for member in cls:
                if member.label.lower() == input_value.strip().lower():
                    return member
        print(f"couldn't parse {cls.__name__} value: {input_value!r}")
        return None
