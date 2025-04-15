import ast
import numbers
import time
from abc import ABC, abstractmethod
from typing import List, Union

from neo4j import Driver, GraphDatabase, Session
from gqlalchemy import Memgraph

from src.interfaces.simple_enum import NodeLabel, RelationshipLabel
from src.shared.db_credentials import DBCredentials
from src.shared.record_merger import FieldConflictBehavior, RecordMerger

class GraphDBDataLoader(ABC):
    base_path: str
    merger: RecordMerger

    def __init__(self, base_path: str = None, field_conflict_behavior: FieldConflictBehavior = FieldConflictBehavior.KeepLast):
        self.base_path = base_path
        self.merger = RecordMerger(field_conflict_behavior=field_conflict_behavior)


    @abstractmethod
    def delete_all_data_and_indexes(self) -> bool:
        pass

    @abstractmethod
    def load_to_graph(self, query, records, batch_size=1):
        pass

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



    @abstractmethod
    def add_index(self, label: NodeLabel, field: str):
        pass

    def ensure_list(self, possible_list):
        if not isinstance(possible_list, list):
            return [possible_list]
        return possible_list


    def generate_node_insert_query(self,
                                   records: List[dict],
                                   labels: [NodeLabel]):

        example_record = self.merger.get_example_record(records)

        conjugate_label_str = self.get_conjugate_label_str(labels)

        field_keys, list_keys = self.merger.parse_list_and_field_keys(example_record)

        if self.merger.field_conflict_behavior == FieldConflictBehavior.KeepLast:
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
        example_record = self.merger.get_example_record(records)

        conjugate_start_label_str = self.get_conjugate_label_str(start_labels)
        conjugate_label_str = self.get_conjugate_label_str(rel_labels)
        conjugate_end_label_str = self.get_conjugate_label_str(end_labels)

        field_keys, list_keys = self.merger.parse_list_and_field_keys(example_record)

        provenance_updates = ['COALESCE(rel.edge_updates, [])']

        provenance_updates.extend([
            f"CASE WHEN relRecord.{prop} IS NOT NULL AND (rel.{prop} IS NULL OR relRecord.{prop} <> rel.{prop}) "
            f"THEN ['{prop}\t' + COALESCE(rel.{prop}, 'NULL') + '\t' + relRecord.{prop} + '\t' + relRecord.provenance + '\t{self.merger.field_conflict_behavior.value}'] ELSE [] END"
            for prop in field_keys
        ])

        if self.merger.field_conflict_behavior == FieldConflictBehavior.KeepLast:
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

    def load_node_records(self, records: List[dict], labels: Union[NodeLabel, List[NodeLabel]]):
        labels = self.ensure_list(labels)
        for label in labels:
            self.add_index(label, 'id')
        query = self.generate_node_insert_query(records, labels)
        print(records[0])
        print(query)

        self.load_to_graph(query, records)

    def load_relationship_records(self, records: List[dict], start_labels: List[NodeLabel],
                                  rel_labels: Union[RelationshipLabel, List[RelationshipLabel]],
                                  end_labels: List[NodeLabel]):
        rel_labels = self.ensure_list(rel_labels)
        query = self.generate_relationship_insert_query(records, start_labels, rel_labels, end_labels)
        print(records[0])
        print(query)
        self.load_to_graph(query, records)


def batch(iterable, batch_size):
    l = len(iterable)
    if l > batch_size:
        print(f"batching records into size: {batch_size}")
    for ndx in range(0, l, batch_size):
        if l > batch_size:
            print(f"running: {ndx + 1}-{min(ndx + batch_size, l)} of {l}")
        yield iterable[ndx:min(ndx + batch_size, l)]

class MemgraphDataLoader(GraphDBDataLoader):

    memgraph: Memgraph

    def __init__(self, credentials: DBCredentials, **kwargs):
        GraphDBDataLoader.__init__(self, **kwargs)
        self.memgraph = Memgraph(credentials.url, credentials.port, credentials.user, credentials.password)

    def index_exists(self, label: str, field: str) -> bool:
        indexes = self.memgraph.execute_and_fetch("SHOW INDEX INFO;")
        for record in indexes:
            if label == record['label'] and field == record['property']:
                return True
        return False

    def create_index(self, label: str, field: str):
        print(f'creating index {label}: {field}')
        self.memgraph.execute(f"CREATE INDEX ON :`{label}`(`{field}`)")

    def add_index(self, label: NodeLabel, field: str):
        label_str = label.value if hasattr(label, 'value') else label
        if not self.index_exists(label_str, field):
            self.create_index(label_str, field)


    def load_relationship_records(self, records: List[dict], start_labels: List[NodeLabel],
                                  rel_labels: Union[RelationshipLabel, List[RelationshipLabel]],
                                  end_labels: List[NodeLabel]):
        rel_labels = self.ensure_list(rel_labels)

        conjugate_start_label_str = self.get_conjugate_label_str(start_labels)
        conjugate_label_str = self.get_conjugate_label_str(rel_labels)
        conjugate_end_label_str = self.get_conjugate_label_str(end_labels)

        unique_pairs = [[record['start_id'], record['end_id']] for record in records]

        edges = self.memgraph.execute_and_fetch(f"""
        UNWIND $unique_pairs AS pair
            MATCH (source:`{conjugate_start_label_str}` {{id: pair[0]}})-[r:`{conjugate_label_str}`]->(target:`{conjugate_end_label_str}` {{id: pair[1]}})
            RETURN properties(r) as props""", {'unique_pairs': list(unique_pairs)})

        existing_edge_map = {(record['props']['start_id'], record['props']['end_id']): record['props'] for record in edges}

        if len(existing_edge_map) > 0:
            self.memgraph.execute(f"""
            UNWIND $unique_pairs AS pair
                MATCH (source:`{conjugate_start_label_str}` {{id: pair[0]}})-[r:`{conjugate_label_str}`]->(target:`{conjugate_end_label_str}` {{id: pair[1]}})
                DELETE r""", {'unique_pairs': list(unique_pairs)})

        records = self.merger.merge_records(records, existing_edge_map, 'edges')

        query = f"""UNWIND $records as new_entry
        MATCH (source:`{conjugate_start_label_str}` {{id: new_entry.start_id}})
        MATCH (target:`{conjugate_end_label_str}` {{id: new_entry.end_id}})
        CREATE (source)-[rel:`{conjugate_label_str}`]->(target)
        SET rel = new_entry
        RETURN rel"""

        print(records[0])
        print(query)

        self.load_to_graph(query, records)

    def load_node_records(self, records: List[dict], labels: Union[NodeLabel, List[NodeLabel]]):
        labels = self.ensure_list(labels)
        for label in labels:
            self.add_index(label, 'id')

        conjugate_label_str = self.get_conjugate_label_str(labels)

        ids = [record['id'] for record in records]
        ids = list(set(ids))

        nodes = self.memgraph.execute_and_fetch(f"""
        UNWIND $ids AS id
            MATCH (n:`{conjugate_label_str}` {{id: id}})
            RETURN properties(n) as props""", {'ids': ids})
        existing_node_map = {record['props']['id']: record['props'] for record in nodes}

        records = self.merger.merge_records(records, existing_node_map, 'nodes')

        query = f"""UNWIND $records as new_entry
        MERGE (n:`{conjugate_label_str}` {{id: new_entry.id}})
        SET n = new_entry
        RETURN n"""
        print(records[0])
        print(query)

        self.load_to_graph(query, records)

    def load_to_graph(self, query, records, batch_size=50050):
        for record_batch in batch(records, batch_size):
            retries = 3
            while retries > 0:
                try:
                    self.memgraph.execute(query, {'records': record_batch})
                    break
                except Exception as e:
                    print(record_batch[0])
                    print(f"Error: {e}, retrying...")
                    retries -= 1
                    if retries == 0:
                        raise
                    time.sleep(1)  # Add a delay before retrying

    def delete_all_data_and_indexes(self) -> bool:
        node_batch_size = 25000
        relationship_batch_size = 4 * node_batch_size
        print("deleting relationships")
        result = self.memgraph.execute_and_fetch("Match ()-[r]-() RETURN count(r) as total")
        total = next(result, {}).get("total", 0)
        while total > 0:
            self.memgraph.execute(f"MATCH ()-[r]-() WITH r limit {relationship_batch_size} DELETE r")

            result = self.memgraph.execute_and_fetch("Match ()-[r]-() RETURN count(r) as total")
            total = next(result, {}).get("total", 0)
            print(f"{total} relationships remaining")

        print("deleting nodes")

        result = self.memgraph.execute_and_fetch("MATCH (n) RETURN count(n) as total")
        total = next(result, {}).get("total", 0)

        while total > 0:
            self.memgraph.execute(f"MATCH (n) WITH n LIMIT {node_batch_size} DETACH DELETE n")
            result = self.memgraph.execute_and_fetch("MATCH (n) RETURN count(n) as total")
            total = next(result, {}).get("total", 0)
            print(f"{total} nodes remaining")

        self.delete_constraints_and_stuff()
        return True

    def delete_constraints_and_stuff(self):
        print("deleting constraints and stuff")

        indexes = self.memgraph.execute_and_fetch("SHOW INDEX INFO;")

        # Step 2: Drop each index
        for record in indexes:
            label = record["label"]
            prop = record["property"]
            self.memgraph.execute(f"DROP INDEX ON :`{label}`(`{prop}`)")

class Neo4jDataLoader(GraphDBDataLoader):

    driver: Driver

    def __init__(self, credentials: DBCredentials, **kwargs):
        GraphDBDataLoader.__init__(self, **kwargs)
        self.driver = GraphDatabase.driver(credentials.url, auth=(credentials.user, credentials.password),
                                               encrypted=False)

    def add_index(self, label: NodeLabel, field: str):
        label_str = label.value if hasattr(label, 'value') else label

        with self.driver.session() as session:
            if not self.index_exists(session, label_str, field):
                self.create_index(session, label_str, field)

    def load_to_graph(self, query, records, batch_size=50050):
        with self.driver.session() as session:
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


