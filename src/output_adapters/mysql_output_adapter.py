from abc import ABC
from datetime import datetime
from sqlalchemy import inspect as sqlalchemy_inspect
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.exc import IntegrityError
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
        self.update_database(database_name)

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

    @staticmethod
    def _is_fk_integrity_error(exc: Exception) -> bool:
        message = str(exc).lower()
        return isinstance(exc, IntegrityError) and (
            "foreign key constraint fails" in message
            or "cannot add or update a child row" in message
        )

    def _diagnose_fk_batch_failure(self, table_class, rows):
        stmt = mysql_insert(table_class.__table__)
        print(f"Batch insert failed for {table_class.__name__}; retrying row-by-row to isolate FK issue")
        bad_rows = []

        for index, row in enumerate(rows, start=1):
            temp_session = self.get_session()
            try:
                temp_session.execute(stmt, [row])
                temp_session.flush()
            except Exception as row_exc:
                temp_session.rollback()
                bad_rows.append((index, row, row_exc))
                print(f"Bad row {index} for {table_class.__name__}: {row}")
                print(f"Row error: {row_exc}")
                break
            finally:
                temp_session.rollback()
                temp_session.close()

        if bad_rows:
            index, row, row_exc = bad_rows[0]
            raise RuntimeError(
                f"Foreign key insert failed for {table_class.__name__} at row {index}: {row}"
            ) from row_exc
        raise RuntimeError(
            f"Foreign key insert failed for {table_class.__name__}, but row-by-row replay did not isolate a bad row"
        )

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
                    stmt = mysql_insert(table_class.__table__)
                    try:
                        session.execute(stmt, rows)
                        session.commit()
                    except Exception as e:
                        session.rollback()
                        if self._is_fk_integrity_error(e):
                            self._diagnose_fk_batch_failure(table_class, rows)
                        raise
                    duration = (datetime.now() - start_time).total_seconds()
                    print(f"Processed {len(converted_objects)} objects in {duration:.2f} seconds.")

            return True

        except Exception as e:
            session.rollback()
            print("Error during insert:", e)
            raise

        finally:
            session.close()

    def create_or_truncate_datastore(self, truncate_tables: bool = None) -> bool:
        effective_truncate = self.truncate_tables if truncate_tables is None else truncate_tables
        self.recreate_mysql_db(self.database_name, effective_truncate)
        return True


class TestOutputAdapter(MySQLOutputAdapter):
    output_converter: TestSQLOutputConverter

    def __init__(self, credentials: DBCredentials, database_name: str):
        MySQLOutputAdapter.__init__(self, credentials, database_name)
        self.output_converter = TestSQLOutputConverter(sql_base=TestBase)

    def create_or_truncate_datastore(self, truncate_tables: bool = None) -> bool:
        super().create_or_truncate_datastore(truncate_tables=truncate_tables)
        self.output_converter.sql_base.metadata.create_all(self.get_engine())
        return True


class TCRDOutputAdapter(MySQLOutputAdapter):
    output_converter = TCRDOutputConverter

    def __init__(self, credentials: DBCredentials, database_name: str, truncate_tables: bool):
        MySQLOutputAdapter.__init__(self, credentials, database_name, truncate_tables=truncate_tables)
        self.output_converter = TCRDOutputConverter()

    def do_pre_processing(self) -> None:
        session = self.get_session()
        try:
            self.output_converter.preload_id_mappings(session)
        finally:
            session.close()

    def create_or_truncate_datastore(self, truncate_tables: bool = None) -> bool:
        super().create_or_truncate_datastore(truncate_tables=truncate_tables)
        self.output_converter.sql_base.metadata.create_all(self.get_engine())
        self.output_converter.preload_id_mappings(self.get_session())
        return True
