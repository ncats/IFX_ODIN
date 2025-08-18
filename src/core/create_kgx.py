import os

from src.interfaces.result_types import ListQueryContext
from src.models.datasource_version_info import DataSourceDetails

yaml_file = "./src/use_cases/api/cure_dashboard.yaml"

from src.use_cases.build_from_yaml import HostDashboardFromYaml


dashboard = HostDashboardFromYaml(yaml_file=yaml_file)
api = dashboard.api_adapter

batch_size = 10

output_directory = "./output"

def get_node_file_name(directory: str):
    return f"{directory}/nodes.jsonl"

def get_edge_file_name(directory: str):
    return f"{directory}/edges.jsonl"


def clear_output_files():
    node_file = get_node_file_name(output_directory)
    edge_file = get_edge_file_name(output_directory)

    os.makedirs(os.path.dirname(node_file), exist_ok=True)
    os.makedirs(os.path.dirname(edge_file), exist_ok=True)
    with open(node_file, 'w') as nf:
        nf.write('')
    with open(edge_file, 'w') as ef:
        ef.write('')

def convert_to_biolink_label(label: str):
    if label.startswith('biolink:'):
        return label
    if label == 'Drug':
        return 'biolink:ChemicalEntity'
    if label == 'Gene':
        return 'biolink:Gene'
    if label == 'Transcript':
        return 'biolink:Transcript'
    if label == 'Protein':
        return 'biolink:Protein'
    if label == 'Disease':
        return 'biolink:Disease'
    if label == 'Pathway':
        return 'biolink:Pathway'
    if label == 'SequenceVariant':
        return 'biolink:SequenceVariant'
    if label == 'PhenotypicFeature':
        return 'biolink:PhenotypicFeature'
    if label == 'AdverseEvent':
        return 'biolink:DiseaseOrPhenotypicFeature'

    raise Exception(f"label not mapped: {label}")

def parse_sources(item):
    sources = set()
    if hasattr(item, 'sources') and item.sources is not None:
        for source_str in item.sources:
            source_obj = DataSourceDetails.parse_tsv(source_str)
            sources.add(source_obj.name)
    return list(sources)

def write_nodes(items):
    file_name = get_node_file_name(output_directory)
    print(f"Writing {len(items)} nodes to {file_name}")
    with open(file_name, 'a') as f:
        for item in items:
            item_dict = {k: v for k, v in item.__dict__.copy().items() if v is not None and k not in ['sources', 'labels', 'creation', 'updates']}
            if 'xref' in item_dict and isinstance(item_dict['xref'], list):
                item_dict['xref'] = [x.id_str() for x in item_dict['xref']]
            item_dict['category'] = [convert_to_biolink_label(lbl.value) for lbl in item.labels]
            item_dict['provided_by'] = parse_sources(item)
            item_dict['aggregator_knowledge_source'] = ['NCATS - IFX - Ontology and Data Integration Network (ODIN)']
            f.write(f"{item_dict}\n")

def write_edges(items):
    file_name = get_edge_file_name(output_directory)
    print(f"Writing {len(items)} edges to {file_name}")
    with open(file_name, 'a') as f:
        for item in items:
            item_dict = {k: v for k, v in item.__dict__.copy().items() if v is not None and k not in ['start_node', 'end_node', 'biolink_label', 'biolink_category', 'sources', 'labels', 'creation', 'updates']}
            item_dict['subject'] = item.start_node.id
            item_dict['predicate'] = item.biolink_label
            item_dict['object'] = item.end_node.id
            item_dict['category'] = [item.biolink_category]
            item_dict['provided_by'] = parse_sources(item)
            item_dict['aggregator_knowledge_source'] = ['NCATS - IFX - Ontology and Data Integration Network (ODIN)']
            f.write(f"{item_dict}\n")

clear_output_files()

for model in api.list_nodes():
    context = ListQueryContext(source_data_model=model)

    skip = 0
    while True:
        node_list = api.get_list(context, top=batch_size, skip=skip)
        if not node_list:
            break

        write_nodes(node_list)
        skip += batch_size

for model in api.list_edges():
    context = ListQueryContext(source_data_model=model)

    skip = 0
    while True:
        edge_list = api.get_list(context, top=batch_size, skip=skip)
        if not edge_list:
            break

        write_edges(edge_list)
        skip += batch_size

