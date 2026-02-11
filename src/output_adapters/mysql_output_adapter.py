from abc import ABC
from datetime import datetime
from sqlalchemy import inspect, tuple_
from src.input_adapters.sql_adapter import MySqlAdapter
from src.interfaces.output_adapter import OutputAdapter
from src.output_adapters.sql_converters.output_converter_base import SQLOutputConverter
from src.output_adapters.sql_converters.tcrd import TCRDOutputConverter
from src.output_adapters.sql_converters.test import TestSQLOutputConverter
from src.shared.db_credentials import DBCredentials
from src.shared.record_merger import RecordMerger, FieldConflictBehavior
from src.shared.sqlalchemy_tables.test_tables import Base as TestBase


class MySQLOutputAdapter(OutputAdapter, MySqlAdapter, ABC):
    database_name: str
    truncate_tables: bool
    no_merge: bool
    output_converter: SQLOutputConverter

    def __init__(self, credentials: DBCredentials, database_name: str, truncate_tables: bool = True, no_merge: bool = True):
        self.database_name = database_name
        self.truncate_tables = truncate_tables
        self.no_merge = no_merge
        OutputAdapter.__init__(self)
        MySqlAdapter.__init__(self, credentials)

    def store(self, objects, single_source=False) -> bool:
        merger = RecordMerger(field_conflict_behavior=FieldConflictBehavior.KeepLast)

        if not isinstance(objects, list):
            objects = [objects]

        object_groups = self.sort_and_convert_objects(objects, keep_nested_objects = True)
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

                    example = converted_objects[0]
                    table_class = example.__class__
                    mapper = inspect(table_class)
                    pk_columns = mapper.primary_key
                    merge_fields = getattr(converter, 'merge_fields', None)
                    if merge_fields:
                        pk_columns = tuple((c for c in mapper.columns if c.name in merge_fields))

                    if not pk_columns:
                        raise ValueError(f"No primary key defined for {table_class.__name__}")

                    pk_values = [
                        tuple(getattr(obj, col.name) for col in pk_columns)
                        for obj in converted_objects
                        if not any(getattr(obj, col.name) is None for col in pk_columns)
                    ]

                    if self.no_merge and not getattr(converter, 'merge_anyway', False):
                        existing_rows = []
                    else:
                        existing_rows = session.query(table_class).filter(tuple_(*pk_columns).in_(pk_values)).all()

                    existing_lookup = {
                        tuple(str(getattr(row, col.name)) for col in pk_columns): row
                        for row in existing_rows
                    }
                    to_insert, to_update = merger.merge_objects(converted_objects, existing_lookup, mapper, pk_columns, merge_anyway=getattr(converter, 'merge_anyway', False))

                    if len(to_insert) > 0:
                        if getattr(converter, 'deduplicate', False):
                            to_insert = list({tuple(getattr(rr, col.name) for col in pk_columns): rr for rr in to_insert}.values())
                        print(f"Inserting {len(to_insert)} objects of type {table_class.__name__} using converter {converter.__name__}")
                        session.bulk_save_objects(to_insert)
                    if len(to_update) > 0:
                        print(f"Merging {len(to_update)} objects of type {table_class.__name__} using converter {converter.__name__}")
                        session.bulk_save_objects(to_update, update_changed_only=True)

                    session.commit()
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
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
        MySQLOutputAdapter.__init__(self, credentials, database_name, no_merge = False)
        self.output_converter = TestSQLOutputConverter(sql_base=TestBase)

    def create_or_truncate_datastore(self) -> bool:
        super().create_or_truncate_datastore()
        self.output_converter.sql_base.metadata.create_all(self.get_engine())
        return True


class TCRDOutputAdapter(MySQLOutputAdapter):
    output_converter = TCRDOutputConverter

    def __init__(self, credentials: DBCredentials, database_name: str, truncate_tables: bool):
        self.truncate_tables = truncate_tables
        MySQLOutputAdapter.__init__(self, credentials, database_name, truncate_tables = truncate_tables, no_merge = True)
        self.output_converter = TCRDOutputConverter()

    def create_or_truncate_datastore(self) -> bool:
        super().create_or_truncate_datastore()
        self.output_converter.sql_base.metadata.create_all(self.get_engine())
        self.output_converter.preload_id_mappings(self.get_session())
        return True
