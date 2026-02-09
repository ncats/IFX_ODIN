from abc import ABC, abstractmethod
from typing import Union, List

from sqlalchemy.orm import DeclarativeBase


class SQLOutputConverter(ABC):
    sql_base: type[DeclarativeBase]
    id_mapping = {}

    def __init__(self, sql_base: type[DeclarativeBase]):
        self.sql_base = sql_base

    @abstractmethod
    def get_object_converters(self, obj_cls) -> Union[callable, List[callable], None]:
        raise NotImplementedError("Derived classes must implement convert")


    def get_preload_queries(self, session):
        return []

    def preload_id_mappings(self, session):
        self.id_mapping = {}
        for preload_obj in self.get_preload_queries(session):
            table = preload_obj['table']
            data = preload_obj['data']
            id_format_function = preload_obj['id_format_function'] if 'id_format_function' in preload_obj else lambda x: x[1]

            try:
                mapping = {}
                for row in data:
                    lookup_id = id_format_function(row)
                    ifxid = row[0]
                    if ifxid is not None:
                        mapping[lookup_id] = ifxid
                self.id_mapping[table] = mapping
            except Exception as e:
                print(f"Error preloading id mappings for table {table}: {e}")
                # if preload fails (no DB/session available), leave mapping empty
                self.id_mapping[table] = {}

    def resolve_id(self, table, id):
        if table not in self.id_mapping:
            self.id_mapping[table] = {}
        mapping = self.id_mapping[table]
        if id not in mapping:
            mapping[id] = len(mapping.values()) + 1
        return mapping[id]