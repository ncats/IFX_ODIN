from abc import abstractmethod, ABC
from datetime import datetime
from typing import List, Union

from sqlalchemy import inspect, tuple_

from src.input_adapters.pharos_mysql.new_tables import Base
from src.input_adapters.sql_adapter import MySqlAdapter
from src.interfaces.output_adapter import OutputAdapter
from src.models.generif import GeneGeneRifRelationship
from src.models.go_term import GoTerm, GoTermHasParent, ProteinGoTermRelationship
from src.models.ligand import Ligand, ProteinLigandRelationship
from src.models.protein import Protein
from src.models.test_models import TestNode, TestRelationship
from src.output_adapters.converters.tcrd import goterm_converter, goterm_parent_converter, protein_converter, \
    t2tc_converter, target_converter, goa_converter, protein_alias_converter, tdl_info_converter, generif_converter, \
    generif_assoc_converter, p2p_converter, ligand_converter, ligand_edge_converter
from src.output_adapters.converters.test import TestBase, node_converter, rel_converter
from src.shared.db_credentials import DBCredentials
from src.shared.record_merger import RecordMerger, FieldConflictBehavior


class MySQLOutputAdapter(OutputAdapter, MySqlAdapter, ABC):
    database_name: str
    no_merge: bool

    def __init__(self, credentials: DBCredentials, database_name: str, no_merge: bool = True):
        self.database_name = database_name
        self.no_merge = no_merge
        OutputAdapter.__init__(self)
        MySqlAdapter.__init__(self, credentials)

    @abstractmethod
    def get_output_converter(self, obj_cls) -> Union[callable, List[callable], None]:
        raise NotImplementedError("Derived classes must implement get_table")

    def store(self, objects) -> bool:
        merger = RecordMerger(field_conflict_behavior=FieldConflictBehavior.KeepLast)

        if not isinstance(objects, list):
            objects = [objects]

        object_groups = self.sort_and_convert_objects(objects, keep_nested_objects = True)
        session = self.get_session()

        try:
            for obj_list, labels, is_relationship, start_labels, end_labels, obj_cls in object_groups.values():
                converters = self.get_output_converter(obj_cls)
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

                    if not pk_columns:
                        raise ValueError(f"No primary key defined for {table_class.__name__}")

                    if len(pk_columns) == 1 and mapper.mapped_table.autoincrement_column is not None:
                        to_insert = merger.create_autoinc_objects(converted_objects)
                        to_update = []
                    else:
                        pk_values = [
                            tuple(getattr(obj, col.name) for col in pk_columns)
                            for obj in converted_objects
                        ]
                        if self.no_merge and not getattr(converter, 'merge_anyway', False):
                            existing_rows = []
                        else:
                            existing_rows = session.query(table_class).filter(tuple_(*pk_columns).in_(pk_values)).all()

                        existing_lookup = {
                            tuple(str(getattr(row, col.name)) for col in pk_columns): row
                            for row in existing_rows
                        }
                        to_insert, to_update = merger.merge_objects(converted_objects, existing_lookup, mapper)


                    if len(to_insert) > 0:
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
        self.recreate_mysql_db(self.database_name)
        return True


class TestOutputAdapter(MySQLOutputAdapter):

    def create_or_truncate_datastore(self) -> bool:
        super().create_or_truncate_datastore()
        TestBase.metadata.create_all(self.get_engine())
        return True

    def get_output_converter(self, obj_cls) -> Union[callable, List[callable], None]:
        if obj_cls == TestNode:
            return node_converter
        if obj_cls == TestRelationship:
            return rel_converter
        return None

class TCRDOutputAdapter(MySQLOutputAdapter):

    def create_or_truncate_datastore(self) -> bool:
        super().create_or_truncate_datastore()
        Base.metadata.create_all(self.get_engine())
        return True

    def get_output_converter(self, obj_cls) -> Union[callable, List[callable], None]:
        if obj_cls == GoTerm:
            return goterm_converter
        if obj_cls == GoTermHasParent:
            return goterm_parent_converter
        if obj_cls == Protein:
            return [protein_converter, target_converter, t2tc_converter, protein_alias_converter, tdl_info_converter]
        if obj_cls == ProteinGoTermRelationship:
            return goa_converter
        if obj_cls == GeneGeneRifRelationship:
            return [generif_converter, generif_assoc_converter, p2p_converter]
        if obj_cls == Ligand:
            return ligand_converter
        if obj_cls == ProteinLigandRelationship:
            return ligand_edge_converter
        return None
