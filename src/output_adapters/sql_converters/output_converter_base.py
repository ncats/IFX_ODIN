from abc import ABC, abstractmethod
from typing import Union, List

from sqlalchemy.orm import DeclarativeBase


class SQLOutputConverter(ABC):
    sql_base: type[DeclarativeBase]
    id_mapping = {}

    def __init__(self, sql_base: type[DeclarativeBase]):
        self.sql_base = sql_base
        self._next_ids = {}
        self._used_ids = {}

    @abstractmethod
    def get_object_converters(self, obj_cls) -> Union[callable, List[callable], None]:
        raise NotImplementedError("Derived classes must implement get_object_converters")

    def get_preload_queries(self, session):
        return []

    def preload_id_mappings(self, session):
        self.id_mapping = {}
        self._next_ids = {}
        self._used_ids = {}
        for preload_obj in self.get_preload_queries(session):
            table = preload_obj['table']
            data = preload_obj['data']
            id_format_function = preload_obj.get('id_format_function', lambda x: x[1])
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
                self.id_mapping[table] = {}

    def resolve_id(self, table, id):
        if table not in self.id_mapping:
            self.id_mapping[table] = {}
        mapping = self.id_mapping[table]
        if id not in mapping:
            mapping[id] = self._next_id(table, mapping)
        return mapping[id]

    def _next_id(self, table, mapping):
        used_ids = self._used_ids.get(table)
        if used_ids is None:
            used_ids = set(mapping.values())
            self._used_ids[table] = used_ids

        next_id = self._next_ids.get(table)
        if next_id is None:
            numeric_ids = [
                value
                for value in used_ids
                if isinstance(value, int) and not isinstance(value, bool)
            ]
            next_id = max(numeric_ids, default=0) + 1

        while next_id in used_ids:
            next_id += 1

        used_ids.add(next_id)
        self._next_ids[table] = next_id + 1
        return next_id
