from abc import ABC
from datetime import datetime
from sqlalchemy import inspect as sqlalchemy_inspect
from sqlalchemy.dialects.mysql import insert as mysql_insert
from src.input_adapters.sql_adapter import MySqlAdapter
from src.interfaces.output_adapter import OutputAdapter
from src.output_adapters.sql_converters.output_converter_base import SQLOutputConverter
from src.output_adapters.sql_converters.tcrd import TCRDOutputConverter
from src.output_adapters.sql_converters.test import TestSQLOutputConverter
from src.shared.db_credentials import DBCredentials
from src.shared.sqlalchemy_tables.test_tables import Base as TestBase


class MySQLOutputAdapter(OutputAdapter, MySqlAdapter, ABC):
    database_name: str
    truncate_tables: bool
    output_converter: SQLOutputConverter

    def __init__(self, credentials: DBCredentials, database_name: str, truncate_tables: bool = True):
        self.database_name = database_name
        self.truncate_tables = truncate_tables
        OutputAdapter.__init__(self)
        MySqlAdapter.__init__(self, credentials)

    @staticmethod
    def _serialize_rows(converted_objects):
        raw_rows = []
        for obj in converted_objects:
            mapper = sqlalchemy_inspect(type(obj))
            attr_to_column = {
                attr.key: attr.columns[0].name
                for attr in mapper.column_attrs
                if len(attr.columns) == 1
            }
            raw_rows.append({
                attr_to_column.get(k, k): v
                for k, v in obj.__dict__.items()
                if k != '_sa_instance_state'
            })
        all_keys = set().union(*(row.keys() for row in raw_rows))
        return [{key: row.get(key) for key in all_keys} for row in raw_rows]

    def store(self, objects, single_source=False) -> bool:
        if not isinstance(objects, list):
            objects = [objects]

        object_groups = self.sort_and_convert_objects(objects, keep_nested_objects=True)
        session = self.get_session()

        try:
            for obj_list, labels, is_relationship, start_labels, end_labels, obj_cls in object_groups.values():
                converters = self.output_converter.get_object_converters(obj_cls)
                if converters is None:
                    continue

                if not isinstance(converters, list):
                    converters = [converters]

                for converter in converters:
                    start_time = datetime.now()
                    converted_objects = []
                    for obj in obj_list:
                        result = converter(obj)
                        if isinstance(result, list):
                            converted_objects.extend(result)
                        elif result is not None:
                            converted_objects.append(result)

                    if not converted_objects:
                        continue

                    table_class = converted_objects[0].__class__
                    print(f"Inserting {len(converted_objects)} objects of type {table_class.__name__}")
                    rows = self._serialize_rows(converted_objects)
                    stmt = mysql_insert(table_class.__table__).prefix_with('IGNORE')
                    session.execute(stmt, rows)
                    session.commit()
                    duration = (datetime.now() - start_time).total_seconds()
                    print(f"Processed {len(converted_objects)} objects in {duration:.2f} seconds.")

            return True

        except Exception as e:
            session.rollback()
            print("Error during insert:", e)
            raise

        finally:
            session.close()

    def create_or_truncate_datastore(self) -> bool:
        self.recreate_mysql_db(self.database_name, self.truncate_tables)
        return True


class TestOutputAdapter(MySQLOutputAdapter):
    output_converter: TestSQLOutputConverter

    def __init__(self, credentials: DBCredentials, database_name: str):
        MySQLOutputAdapter.__init__(self, credentials, database_name)
        self.output_converter = TestSQLOutputConverter(sql_base=TestBase)

    def create_or_truncate_datastore(self) -> bool:
        super().create_or_truncate_datastore()
        self.output_converter.sql_base.metadata.create_all(self.get_engine())
        return True


class TCRDOutputAdapter(MySQLOutputAdapter):
    output_converter = TCRDOutputConverter

    def __init__(self, credentials: DBCredentials, database_name: str, truncate_tables: bool):
        MySQLOutputAdapter.__init__(self, credentials, database_name, truncate_tables=truncate_tables)
        self.output_converter = TCRDOutputConverter()

    def create_or_truncate_datastore(self) -> bool:
        super().create_or_truncate_datastore()
        self.output_converter.sql_base.metadata.create_all(self.get_engine())
        self.output_converter.preload_id_mappings(self.get_session())
        return True
