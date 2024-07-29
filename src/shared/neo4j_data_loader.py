import ast
import csv
import numbers
import time
from enum import Enum
from typing import List, Union

from neo4j import Driver, GraphDatabase, Session

from src.output_adapters.generic_labels import NodeLabel, RelationshipLabel
from src.shared.db_credentials import DBCredentials

class FieldConflictBehavior(Enum):
    KeepFirst = "KeepFirst"
    KeepLast = "KeepLast"

class Neo4jDataLoader:
    base_path: str
    driver: Driver
    field_conflict_behavior: FieldConflictBehavior

    def __init__(self, credentials: DBCredentials, base_path: str = None, field_conflict_behavior:
            FieldConflictBehavior = FieldConflictBehavior.KeepLast):
        self.field_conflict_behavior = field_conflict_behavior
        self.base_path = base_path
        self.driver = GraphDatabase.driver(credentials.url, auth=(credentials.user, credentials.password))

    def delete_all_data_and_indexes(self) -> bool:
        batch_size = 50000
        with self.driver.session() as session:
            print("deleting nodes")
            result = session.run("MATCH (n) RETURN count(n) as total")
            total = result.single()["total"]

            while total > 0:
                session.run(f"MATCH (n) WITH n LIMIT {batch_size} DETACH DELETE n")
                result = session.run("MATCH (n) RETURN count(n) as total")
                total = result.single()["total"]
                print(f"{total} nodes remaining")

            print("deleting constraints and stuff")
            session.run("CALL apoc.schema.assert({}, {})")
        return True


    def get_list_type(self, list):
        for item in list:
            if item is not None:
                return type(item)
        return str

    def is_numeric_type(self, tp):
        return issubclass(tp, numbers.Integral) or issubclass(tp, numbers.Real)

    def get_none_val_for_type(self, tp):
        if self.is_numeric_type(tp):
            return -1
        return ''

    def remove_none_values_from_list(self, list):
        type = self.get_list_type(list)
        return [self.get_none_val_for_type(type) if val is None else val for val in list]

    def parse_and_clean_string_value(self, value):
        try:
            clean_val = ast.literal_eval(value)
            if clean_val is None:
                return ""
            elif isinstance(clean_val, list):
                return self.remove_none_values_from_list(clean_val)
            else:
                return clean_val
        except (ValueError, SyntaxError):
            return str(value)

    def read_csv_to_list(self, csv_file: str) -> List[dict]:
        print(f"loading {csv_file}")
        records = []
        with open(f"{self.base_path}{csv_file}") as csvfile:
            reader: csv.DictReader = csv.DictReader(csvfile)
            for row in reader:
                for key, value in row.items():
                    row[key] = self.parse_and_clean_string_value(value)
                records.append(row)
        return records

    def index_exists(self, session: Session, index_name: str) -> bool:
        result = session.run("show indexes")
        for record in result:
            if record['name'] == index_name:
                print(f"{index_name} already exists")
                return True
        return False

    def add_index(self, session: Session, label: NodeLabel, field: str):
        index_name = (f"{label}_{field}_index".lower()
                      .replace(':', '_')
                      .replace(' ', '_')
                      .replace('-', '_')
                      )
        if not self.index_exists(session, index_name):
            session.run(f"CREATE INDEX {index_name} FOR (n:`{label}`) ON (n.{field})")

    def ensure_list(self, possible_list):
        if not isinstance(possible_list, list):
            return [possible_list]
        return possible_list

    def generate_node_insert_query(self,
                                   example_record: dict,
                                   labels: [NodeLabel]):
        labels = self.ensure_list(labels)
        lables_str = [l.value if hasattr(l, 'value') else l for l in labels]
        conjugate_label_str = "`&`".join(lables_str)

        forbidden_keys = ['id', 'labels']
        list_keys = []
        field_keys = []
        for prop in example_record.keys():
            if prop in forbidden_keys:
                continue
            if isinstance(example_record[prop], list):
                list_keys.append(prop)
            else:
                field_keys.append(prop)

        if self.field_conflict_behavior == FieldConflictBehavior.KeepLast:
            field_set_stmts = [f"n.{prop} = CASE WHEN rec.{prop} IS NULL THEN n.{prop} ELSE rec.{prop} END" for prop in field_keys]
        else:
            field_set_stmts = [f"n.{prop} = CASE WHEN n.{prop} IS NULL THEN rec.{prop} ELSE n.{prop} END" for prop in field_keys]

        list_set_stmts = [f"n.{prop} = CASE WHEN n.{prop} IS NULL THEN rec.{prop} ELSE n.{prop} + rec.{prop} END" for prop in list_keys]

        prop_str = ", ".join([*field_set_stmts, *list_set_stmts])

        query = f"""
            UNWIND $records as rec
                MERGE (n:`{conjugate_label_str}` {{id: rec.id}})
                SET {prop_str}
            """
        return query

    def generate_relationship_insert_query(self,
                                           example_record: dict,
                                           start_labels: List[NodeLabel],
                                           rel_labels: List[RelationshipLabel],
                                           end_labels: List[NodeLabel]
                                           ):
        start_labels = self.ensure_list(start_labels)
        rel_labels = self.ensure_list(rel_labels)
        end_labels = self.ensure_list(end_labels)

        start_labels_str = [l.value if hasattr(l, 'value') else l for l in start_labels]
        conjugate_start_label_str = "`&`".join(start_labels_str)

        rel_labels_str = [l.value if hasattr(l, 'value') else l for l in rel_labels]
        conjugate_label_str = "`&`".join(rel_labels_str)

        end_labels_str = [l.value if hasattr(l, 'value') else l for l in end_labels]
        conjugate_end_label_str = "`&`".join(end_labels_str)


        forbidden_keys = ['start_id', 'end_id', 'labels']
        list_keys = []
        field_keys = []
        for prop in example_record.keys():
            if prop in forbidden_keys:
                continue
            if isinstance(example_record[prop], list):
                list_keys.append(prop)
            else:
                field_keys.append(prop)

        if self.field_conflict_behavior == FieldConflictBehavior.KeepLast:
            field_set_stmts = [f"rel.{prop} = CASE WHEN relRecord.{prop} IS NULL THEN rel.{prop} ELSE relRecord.{prop} END" for prop in field_keys]
        else:
            field_set_stmts = [f"rel.{prop} = CASE WHEN rel.{prop} IS NULL THEN relRecord.{prop} ELSE rel.{prop} END" for prop in field_keys]

        list_set_stmts = [f"rel.{prop} = CASE WHEN rel.{prop} IS NULL THEN relRecord.{prop} ELSE rel.{prop} + relRecord.{prop} END" for prop in list_keys]

        prop_str = ", ".join([*field_set_stmts, *list_set_stmts])

        query = f"""
            UNWIND $records AS relRecord
                MATCH (source: `{conjugate_start_label_str}` {{ id: relRecord.start_id }})
                MATCH (target: `{conjugate_end_label_str}` {{ id: relRecord.end_id }})
                MERGE (source)-[rel: `{conjugate_label_str}`]->(target)
                SET {prop_str}
            """
        return query

    def load_node_records(self, session: Session, records: List[dict], labels: Union[NodeLabel, List[NodeLabel]]):
        start_time = time.time()
        labels = self.ensure_list(labels)
        for label in labels:
            self.add_index(session, label, 'id')
        query = self.generate_node_insert_query(records[0], labels)
        print(records[0])
        print(query)
        session.run(query, records=records)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"\tElapsed time: {elapsed_time:.4f} seconds merging {len(records)} nodes")

    def load_node_csv(self, session: Session, csv_file: str, labels: Union[NodeLabel, List[NodeLabel]]):
        records = self.read_csv_to_list(csv_file)
        self.load_node_records(session, records, labels)

    def load_relationship_records(self, session: Session, records: List[dict], start_labels: List[NodeLabel],
                                  rel_labels: Union[RelationshipLabel, List[RelationshipLabel]],
                                  end_labels: List[NodeLabel]):
        start_time = time.time()
        rel_labels = self.ensure_list(rel_labels)
        query = self.generate_relationship_insert_query(records[0], start_labels, rel_labels, end_labels)
        print(query)
        session.run(query, records=records)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"\tElapsed time: {elapsed_time:.4f} seconds merging {len(records)} relationships")

    def load_relationship_csv(self, session: Session, csv_file: str, start_label: NodeLabel,
                              rel_labels: Union[RelationshipLabel, List[RelationshipLabel]],
                              end_label: NodeLabel):
        records = self.read_csv_to_list(csv_file)
        self.load_relationship_records(session, records, start_label, rel_labels, end_label)

