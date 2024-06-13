import ast
import numbers
from typing import Union, List

from neo4j import GraphDatabase, Session
import csv
import time

from src.output_adapters.generic_labels import NodeLabel, RelationshipLabel
from src.use_cases.secrets.local_neo4j import uri, username, password

# CSV file path
base_path = "/Users/kelleherkj/IdeaProjects/NCATS_ODIN/output_files/ramp_neo4j_output/"

metabolite_class_csv = "MetaboliteClass.csv"
metabolite_csv = "Metabolite.csv"
metabolite_chem_props_csv = "MetaboliteChemProps.csv"
metabolite_chem_prop_relationships_csv = "MetaboliteChemPropsRelationship.csv"
metabolite_relationship_csv = "MetaboliteClassRelationship.csv"
protein_csv = "Protein.csv"
pathway_csv = "Pathway.csv"
analyte_relationship_csv = "AnalytePathwayRelationship.csv"
metabolite_protein_relationship_csv = "MetaboliteProteinRelationship.csv"
ontology_csv = "Ontology.csv"
analyte_ontology_relationship = "AnalyteOntologyRelationship.csv"
reaction_csv = "Reaction.csv"
reaction_class_csv = "ReactionClass.csv"
reaction_class_parent_csv = "ReactionClassParentRelationship.csv"
metabolite_reaction_relationship_csv = "MetaboliteReactionRelationship.csv"
reaction_reaction_class_relationship_csv = "ReactionReactionClassRelationship.csv"
protein_reaction_relationship_csv = "ProteinReactionRelationship.csv"
analyte_synonym_csv = "Analyte.synonyms.csv"
analyte_equiv_id_csv = "Analyte.equivalent_ids.csv"
database_version_csv = "DatabaseVersion.csv"
data_version_csv = "DataVersion.csv"
database_data_version_relationship_csv = "DatabaseDataVersionRelationship.csv"

driver = GraphDatabase.driver(uri, auth=(username, password))


def delete_all_data_and_indexes():
    with driver.session() as session:
        print("deleting nodes")
        session.run("MATCH (n) DETACH DELETE n")
        print("deleting constraints and stuff")
        session.run("CALL apoc.schema.assert({}, {})")

def get_list_type(list):
    for item in list:
        if item is not None:
            return type(item)
    return str

def is_numeric_type(tp):
    return issubclass(tp, numbers.Integral) or issubclass(tp, numbers.Real)

def get_none_val_for_type(tp):
    if is_numeric_type(tp):
        return -1
    return ''

def remove_none_values_from_list(list):
    type = get_list_type(list)
    return [get_none_val_for_type(type) if val is None else val for val in list]

def parse_and_clean_string_value(value):
    try:
        clean_val = ast.literal_eval(value)
        if clean_val is None:
            return ""
        elif isinstance(clean_val, list):
            return remove_none_values_from_list(clean_val)
        else:
            return clean_val
    except (ValueError, SyntaxError):
        return str(value)


def read_csv_to_list(csv_file: str) -> List[dict]:
    print(f"loading {csv_file}")
    records = []
    with open(f"{base_path}{csv_file}") as csvfile:
        reader: dict = csv.DictReader(csvfile)
        for row in reader:
            for key, value in row.items():
                row[key] = parse_and_clean_string_value(value)
            records.append(row)
    return records


def index_exists(session: Session, index_name: str) -> bool:
    result = session.run("show indexes")
    for record in result:
        if record['name'] == index_name:
            print(f"{index_name} already exists")
            return True
    return False


def add_index(session: Session, label: NodeLabel, field: str):
    index_name = f"{label}_{field}_index".lower().replace(':', '_')
    if not index_exists(session, index_name):
        session.run(f"CREATE INDEX {index_name} FOR (n:`{label}`) ON (n.{field})")


def ensure_list(possible_list):
    if not isinstance(possible_list, list):
        return [possible_list]
    return possible_list


def generate_node_insert_query(example_record: dict, labels: [NodeLabel]):
    lables_str = [l.value if hasattr(l, 'value') else l for l in labels]
    conjugate_label_str = "`&`".join(lables_str)
    property_str = ", ".join([f"n.{prop} = rec.{prop}" for prop in example_record.keys() if prop != 'id'])
    query = f"""
        UNWIND $records as rec
            MERGE (n:`{conjugate_label_str}` {{id: rec.id}})
            SET {property_str}
        """
    return query


def generate_relationship_insert_query(
        example_record: dict,
        start_label: NodeLabel,
        rel_labels: [RelationshipLabel],
        end_label: NodeLabel
):
    rel_labels = ensure_list(rel_labels)
    rel_labels_str = [l.value if hasattr(l, 'value') else l for l in rel_labels]
    conjugate_label_str = "`&`".join(rel_labels_str)

    forbidden_keys = ['start_id', 'end_id']
    rel_props = [prop for prop in example_record.keys() if prop not in forbidden_keys]
    property_str = ""
    if len(rel_props) > 0:
        property_str = "SET " + ", ".join([f"rel.`{prop}` = relRecord.`{prop}`" for prop in rel_props])

    query = f"""
        UNWIND $records AS relRecord
            MATCH (source: `{start_label}` {{ id: relRecord.start_id }})
            MATCH (target: `{end_label}` {{ id: relRecord.end_id }})
            MERGE (source)-[rel: `{conjugate_label_str}`]->(target)
            {property_str}
        """
    return query


def load_node_csv(session: Session, csv_file: str, labels: Union[NodeLabel, List[NodeLabel]]):
    start_time = time.time()
    records = read_csv_to_list(csv_file)
    labels = ensure_list(labels)
    for label in labels:
        add_index(session, label, 'id')
    query = generate_node_insert_query(records[0], labels)
    print(query)
    session.run(query, records=records)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\tElapsed time: {elapsed_time:.4f} seconds inserting {len(records)} nodes")


def load_relationship_csv(session: Session, csv_file: str, start_label: NodeLabel,
                          rel_labels: Union[RelationshipLabel, List[RelationshipLabel]],
                          end_label: NodeLabel):
    start_time = time.time()
    records = read_csv_to_list(csv_file)
    rel_labels = ensure_list(rel_labels)
    query = generate_relationship_insert_query(records[0], start_label, rel_labels, end_label)
    print(query)
    session.run(query, records=records)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\tElapsed time: {elapsed_time:.4f} seconds merging {len(records)} relationships")


def import_data():
    with driver.session() as session:
        load_node_csv(session, protein_csv, [NodeLabel.Protein, NodeLabel.Analyte])
        load_node_csv(session, metabolite_csv, [NodeLabel.Metabolite, NodeLabel.Analyte])
        load_node_csv(session, metabolite_chem_props_csv, NodeLabel.MetaboliteChemProps)
        load_relationship_csv(session, metabolite_chem_prop_relationships_csv,
                              NodeLabel.Metabolite, RelationshipLabel.Metabolite_Has_Chem_Prop, NodeLabel.MetaboliteChemProps)
        load_node_csv(session, analyte_synonym_csv, NodeLabel.Analyte)
        load_node_csv(session, analyte_equiv_id_csv, NodeLabel.Analyte)
        add_index(session, NodeLabel.Analyte, "equivalent_ids")

        load_node_csv(session, metabolite_class_csv, NodeLabel.MetaboliteClass)
        load_node_csv(session, pathway_csv, NodeLabel.Pathway)
        load_node_csv(session, ontology_csv, NodeLabel.Ontology)
        load_node_csv(session, reaction_csv, NodeLabel.Reaction)
        load_node_csv(session, reaction_class_csv, NodeLabel.ReactionClass)

        load_relationship_csv(session, metabolite_relationship_csv,
                              NodeLabel.Metabolite, RelationshipLabel.Analyte_Has_Class, NodeLabel.MetaboliteClass)
        load_relationship_csv(session, metabolite_protein_relationship_csv,
                              NodeLabel.Metabolite, RelationshipLabel.Catalyzes, NodeLabel.Protein)
        load_relationship_csv(session, analyte_relationship_csv,
                              NodeLabel.Analyte, RelationshipLabel.Analyte_Has_Pathway, NodeLabel.Pathway)
        load_relationship_csv(session, analyte_ontology_relationship,
                              NodeLabel.Analyte, RelationshipLabel.Analyte_Has_Ontology, NodeLabel.Ontology)

        load_relationship_csv(session, reaction_class_parent_csv,
                              NodeLabel.ReactionClass, RelationshipLabel.ReactionClass_Has_Parent, NodeLabel.ReactionClass)
        load_relationship_csv(session, metabolite_reaction_relationship_csv,
                              NodeLabel.Metabolite, RelationshipLabel.Metabolite_Has_Reaction, NodeLabel.Reaction)
        load_relationship_csv(session, reaction_reaction_class_relationship_csv,
                              NodeLabel.Reaction, RelationshipLabel.Reaction_Has_Class, NodeLabel.ReactionClass)
        load_relationship_csv(session, protein_reaction_relationship_csv,
                              NodeLabel.Protein, RelationshipLabel.Protein_Has_Reaction, NodeLabel.Reaction)

        load_metadata(session)


def load_metadata(session):
    load_node_csv(session, database_version_csv, NodeLabel.DatabaseVersion)
    load_node_csv(session, data_version_csv, NodeLabel.DataVersion)
    load_relationship_csv(session, database_data_version_relationship_csv,
                          NodeLabel.DatabaseVersion, RelationshipLabel.Database_Has_Data, NodeLabel.DataVersion)


if __name__ == "__main__":
    start_time = time.time()

    delete_all_data_and_indexes()
    print(f"\tClean up time: {time.time() - start_time:.4f} seconds")

    import_data()
    driver.close()
    print(f"\tTotal elapsed time: {time.time() - start_time:.4f} seconds")
