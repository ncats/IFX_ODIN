from enum import Enum


class SimpleEnum(Enum):
    def __str__(self):
        return self.value
