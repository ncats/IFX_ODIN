import ast
import numbers
import time
from typing import List, Union

from neo4j import Driver, GraphDatabase, Session

from src.interfaces.simple_enum import NodeLabel, RelationshipLabel, SimpleEnum
from src.shared.db_credentials import DBCredentials


class FieldConflictBehavior(SimpleEnum):
    KeepFirst = "KeepFirst"
    KeepLast = "KeepLast"


class GraphDBDataLoader:
    base_path: str
    driver: Driver
    field_conflict_behavior: FieldConflictBehavior

    def __init__(self, credentials: DBCredentials, base_path: str = None, field_conflict_behavior:
                 FieldConflictBehavior = FieldConflictBehavior.KeepLast):
        self.field_conflict_behavior = FieldConflictBehavior.parse(field_conflict_behavior)
        self.base_path = base_path
        self.driver = GraphDatabase.driver(credentials.url, auth=(credentials.user, credentials.password),
                                           encrypted=False)

    def delete_all_data_and_indexes(self) -> bool:
        node_batch_size = 25000
        relationship_batch_size = 4 * node_batch_size
        with self.driver.session() as session:
            print("deleting relationships")
            result = session.run("Match ()-[r]-() RETURN count(r) as total")
            total = result.single()['total']
            while total > 0:
                session.run(f"MATCH ()-[r]-() WITH r limit {relationship_batch_size} DELETE r")
                result = session.run("Match ()-[r]-() RETURN count(r) as total")
                total = result.single()['total']
                print(f"{total} relationships remaining")

            print("deleting nodes")
            result = session.run("MATCH (n) RETURN count(n) as total")
            total = result.single()["total"]

            while total > 0:
                session.run(f"MATCH (n) WITH n LIMIT {node_batch_size} DETACH DELETE n")
                result = session.run("MATCH (n) RETURN count(n) as total")
                total = result.single()["total"]
                print(f"{total} nodes remaining")

            self.delete_constraints_and_stuff(session)
        return True

    def delete_constraints_and_stuff(self, session):
        print("deleting constraints and stuff")

        indexes = session.run("SHOW INDEX INFO;")

        # Step 2: Drop each index
        for record in indexes:
            label = record["label"]
            prop = record["property"]
            session.run(f"DROP INDEX ON :`{label}`(`{prop}`)")


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
        if len(list) == 0:
            return None
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

    def index_exists(self, session: Session, label: str, field: str) -> bool:
        indexes = session.run("SHOW INDEX INFO;")
        for record in indexes:
            if label == record['label'] and field == record['property']:
                return True
        return False

    def create_index(self, session: Session, label: str, field: str):
        print(f'creating index {label}: {field}')
        session.run(f"CREATE INDEX ON :`{label}`({field})")

    def add_index(self, session: Session, label: NodeLabel, field: str):
        label_str = label.value if hasattr(label, 'value') else label
        if not self.index_exists(session, label_str, field):
            self.create_index(session, label_str, field)

    def ensure_list(self, possible_list):
        if not isinstance(possible_list, list):
            return [possible_list]
        return possible_list

    def get_example_record(self, records: List[dict]):
        example_record = {}
        for rec in records:
            for k, v in rec.items():
                if k not in example_record:
                    if v is None:
                        continue
                    if isinstance(v, list) and len(v) == 0:
                        continue
                    example_record[k] = v
        return example_record

    def generate_node_insert_query(self,
                                   records: List[dict],
                                   labels: [NodeLabel]):

        example_record = self.get_example_record(records)

        conjugate_label_str = self.get_conjugate_label_str(labels)

        field_keys, list_keys = self.parse_list_and_field_keys(example_record)

        if self.field_conflict_behavior == FieldConflictBehavior.KeepLast:
            field_set_stmts = [f"graph_node.{prop} = COALESCE(new_entry.{prop}, graph_node.{prop})"
                    for prop in field_keys]
        else:
            field_set_stmts = [f"graph_node.{prop} = COALESCE(graph_node.{prop}, new_entry.{prop})"
                    for prop in field_keys]

        update_field_prov_stmts = [
            f"""
            CASE 
                WHEN new_entry.{prop} IS NOT NULL
                    THEN ["{prop}\t" + substring(toString(new_entry.{prop}), 0, 50) + "\t" + toString(new_entry.provenance)]
                ELSE [] 
            END"""
            for prop in field_keys
        ]
        update_field_prov_stmts.insert(0, "COALESCE(graph_node.node_updates, [])")
        update_prov_stmt = f"""graph_node.node_updates = 
            {" + ".join(update_field_prov_stmts)}
            """


        creation_prov_stmt = """graph_node.node_creation = CASE WHEN graph_node.node_creation IS NULL
                              THEN new_entry.provenance
                              ELSE graph_node.node_creation
                              END"""

        # set as set fields
        resolved_id_statment = f"graph_node.resolved_ids = COALESCE(graph_node.resolved_ids, []) + COALESCE([new_entry.entity_resolution], [])"
        list_set_stmts = [f"graph_node.{prop} = COALESCE(graph_node.{prop}, []) + COALESCE(new_entry.{prop}, [])"
                          for prop in list_keys]


        # xref statement has to be last
        xref_set_stmt = """
                    WITH graph_node, new_entry
                        WHERE graph_node.xref IS NULL
                        SET graph_node.xref = new_entry.xref 
                    """


        all_properties = [creation_prov_stmt, resolved_id_statment, *field_set_stmts, *list_set_stmts]
        if len(field_keys) > 0:
            all_properties.insert(0, update_prov_stmt)

        prop_str = ", ".join(all_properties)
        query = f"""UNWIND $records as new_entry
                        MERGE (graph_node:`{conjugate_label_str}` {{id: new_entry.id}})
                        SET {prop_str}
                        {xref_set_stmt}"""
        return query

    def parse_list_and_field_keys(self, example_record):
        forbidden_keys = ['id', 'start_id', 'end_id', 'labels']
        list_keys = []
        field_keys = []

        special_handling_fields = ['xref', 'provenance', 'entity_resolution']

        for prop in example_record.keys():
            if prop in special_handling_fields:
                continue
            if prop in forbidden_keys:
                continue
            if isinstance(example_record[prop], list):
                list_keys.append(prop)
            else:
                field_keys.append(prop)

        return field_keys, list_keys

    def get_conjugate_label_str(self, labels):
        labels = self.ensure_list(labels)
        lables_str = [l.value if hasattr(l, 'value') else l for l in labels]
        return "`&`".join(lables_str)

    def generate_relationship_insert_query(self,
                                           records: List[dict],
                                           start_labels: List[NodeLabel],
                                           rel_labels: List[RelationshipLabel],
                                           end_labels: List[NodeLabel]
                                           ):
        example_record = self.get_example_record(records)

        conjugate_start_label_str = self.get_conjugate_label_str(start_labels)
        conjugate_label_str = self.get_conjugate_label_str(rel_labels)
        conjugate_end_label_str = self.get_conjugate_label_str(end_labels)

        field_keys, list_keys = self.parse_list_and_field_keys(example_record)

        provenance_updates = ['COALESCE(rel.edge_updates, [])']

        provenance_updates.extend([
            f"CASE WHEN relRecord.{prop} IS NOT NULL AND (rel.{prop} IS NULL OR relRecord.{prop} <> rel.{prop}) "
            f"THEN ['{prop}\t' + COALESCE(rel.{prop}, 'NULL') + '\t' + relRecord.{prop} + '\t' + relRecord.provenance + '\t{self.field_conflict_behavior.value}'] ELSE [] END"
            for prop in field_keys
        ])

        if self.field_conflict_behavior == FieldConflictBehavior.KeepLast:
            field_set_stmts = [
                f"rel.{prop} = CASE WHEN relRecord.{prop} IS NULL THEN rel.{prop} ELSE relRecord.{prop} END" for prop in
                field_keys]
        else:
            field_set_stmts = [f"rel.{prop} = CASE WHEN rel.{prop} IS NULL THEN relRecord.{prop} ELSE rel.{prop} END"
                               for prop in field_keys]

        list_set_stmts = [
            f"rel.{prop} = COALESCE(rel.{prop}, []) + COALESCE(relRecord.{prop}, [])"
            for prop in list_keys]

        provenance_set_stmt = f"rel.edge_creation = CASE WHEN rel.edge_creation IS NULL THEN relRecord.provenance ELSE rel.edge_creation END"

        provenance_update_stmt = f"rel.edge_updates = CASE WHEN rel.edge_updates IS NULL THEN [] ELSE {' + '.join(provenance_updates)} END"

        resolved_id_statment = f"rel.resolved_ids = COALESCE(rel.resolved_ids, []) + COALESCE([relRecord.entity_resolution], [])"

        prop_str = ", ".join([*field_set_stmts, *list_set_stmts, provenance_set_stmt, provenance_update_stmt, resolved_id_statment])

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
        query = self.generate_node_insert_query(records, labels)
        print(records[0])
        print(query)

        load_to_graph(session, query, records)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"\tElapsed time: {elapsed_time:.4f} seconds merging {len(records)} nodes")

    def load_relationship_records(self, session: Session, records: List[dict], start_labels: List[NodeLabel],
                                  rel_labels: Union[RelationshipLabel, List[RelationshipLabel]],
                                  end_labels: List[NodeLabel]):
        start_time = time.time()
        rel_labels = self.ensure_list(rel_labels)
        query = self.generate_relationship_insert_query(records, start_labels, rel_labels, end_labels)
        print(records[0])
        print(query)
        load_to_graph(session, query, records)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"\tElapsed time: {elapsed_time:.4f} seconds merging {len(records)} relationships")


def batch(iterable, batch_size):
    l = len(iterable)
    if l > batch_size:
        print(f"batching records into size: {batch_size}")
    for ndx in range(0, l, batch_size):
        if l > batch_size:
            print(f"running: {ndx + 1}-{min(ndx + batch_size, l)} of {l}")
        yield iterable[ndx:min(ndx + batch_size, l)]

def load_to_graph(session, query, records, batch_size=50050):
    for record_batch in batch(records, batch_size):
        retries = 3
        while retries > 0:
            try:
                res = session.run(query, records=record_batch)
                res.consume()
                break
            except Exception as e:
                print(record_batch[0])
                print(f"Error: {e}, retrying...")
                retries -= 1
                if retries == 0:
                    raise
                time.sleep(1)  # Add a delay before retrying

class MemgraphDataLoader(GraphDBDataLoader):
    pass


class Neo4jDataLoader(GraphDBDataLoader):

    def delete_constraints_and_stuff(self, session):
        print("deleting constraints and stuff")
        session.run("CALL apoc.schema.assert({}, {})")

    def _get_index_name(self, label: str, field: str):
        index_name = (f"{label}_{field}_index".lower()
                      .replace(':', '_')
                      .replace(' ', '_')
                      .replace('-', '_')
                      )
        return index_name

    def index_exists(self, session: Session, label: str, field: str) -> bool:
        result = session.run("show indexes")
        index_name = self._get_index_name(label, field)
        for record in result:
            if record['name'] == index_name:
                print(f"{index_name} already exists")
                return True
        return False

    def create_index(self, session: Session, label: str, field: str):
        index_name = self._get_index_name(label, field)
        session.run(f"CREATE INDEX {index_name} FOR (n:`{label}`) ON (n.{field})")