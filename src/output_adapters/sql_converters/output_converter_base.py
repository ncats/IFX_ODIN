from abc import ABC, abstractmethod
from typing import Union, List

from sqlalchemy.orm import DeclarativeBase


class SQLOutputConverter(ABC):
    sql_base: type[DeclarativeBase]

    def __init__(self, sql_base: type[DeclarativeBase]):
        self.sql_base = sql_base

    @abstractmethod
    def get_object_converters(self, obj_cls) -> Union[callable, List[callable], None]:
        raise NotImplementedError("Derived classes must implement convert")

