import csv
from typing import Generator, List, Union

from src.constants import DataSourceName
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.models.cure.models import Drug, AdverseEvent, PhenotypicFeature, Disease, SequenceVariant, Gene, CureNode, \
    SequenceVariantDiseaseEdge, CureEdge, DrugDiseaseEdge, DrugPhenotypicFeatureEdge, GeneSequenceVariantEdge, \
    DrugAdverseEventEdge, GeneDiseaseEdge, DiseasePhenotypicFeatureEdge
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import Node, Relationship


class CUREAdapter(FlatFileAdapter):
    node_map = {}

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:

        edges = []

        with open(self.file_path, mode='r') as file:
            csv_reader = csv.DictReader(file, delimiter='\t')
            for row in csv_reader:
                subject_obj = self.get_or_create_node(row, 'subject')
                object_obj = self.get_or_create_node(row, 'object')

                edges.append(self.create_edge(row, subject_obj, object_obj))

        for cls in self.node_map:
            yield self.node_map[cls].values()

        yield edges

    def get_edge_class(self, subject: CureNode, object: CureNode) -> type[CureEdge]:
        if isinstance(subject, SequenceVariant) and isinstance(object, Disease):
            return SequenceVariantDiseaseEdge
        elif isinstance(subject, Drug) and isinstance(object, Disease):
            return DrugDiseaseEdge
        elif isinstance(subject, Disease) and isinstance(object, PhenotypicFeature):
            return DiseasePhenotypicFeatureEdge
        elif isinstance(subject, Drug) and isinstance(object, PhenotypicFeature):
            return DrugPhenotypicFeatureEdge
        elif isinstance(subject, Gene) and isinstance(object, SequenceVariant):
            return GeneSequenceVariantEdge
        elif isinstance(subject, Drug) and isinstance(object, AdverseEvent):
            return DrugAdverseEventEdge
        elif isinstance(subject, Gene) and isinstance(object, Disease):
            return GeneDiseaseEdge
        else:
            raise Exception(f'Unknown edge class for subject {type(subject)} and object {type(object)}')

    def create_edge(self, row: dict, s_obj: CureNode, o_obj: CureNode) -> CureEdge:
        edge_class = self.get_edge_class(s_obj, o_obj)
        edge_obj = edge_class(start_node = s_obj, end_node = o_obj)

        edge_obj.report_id = row['report_id']
        pmid_str = row['pmid']
        if pmid_str is not None and pmid_str != '':
            edge_obj.pmid = int(pmid_str)
        edge_obj.link = row['link']
        edge_obj.outcome = row['outcome']

        edge_obj.biolink_label = row['biolink_predicate']
        edge_obj.biolink_category = row['association_category']

        return edge_obj

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.CURE

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version="4.5.7",
            download_date=self.download_date
        )

    def get_or_create_node(self, row: dict, prefix: str) -> CureNode:
        obj_class = self.get_class(row, prefix)
        id = self.get_id(row, prefix)
        if obj_class not in self.node_map:
            self.node_map[obj_class] = {}

        if id in self.node_map[obj_class]:
            return self.node_map[obj_class][id]
        else:
            node_obj = obj_class(id=id)
            self.set_node_data(node_obj, row, prefix)
            self.node_map[obj_class][id] = node_obj
            return node_obj

    def get_id(self, row: dict, prefix: str) -> str:
        curie_field = f'{prefix}_curie'
        label_field = f'{prefix}_label'
        if row[curie_field] is not None and row[curie_field] != '':
            return row[curie_field]
        return row[label_field]

    def get_node_class(self, type_name: str) -> type[CureNode]:
        if type_name == 'Gene':
            return Gene
        if type_name == 'Disease':
            return Disease
        if type_name == 'Drug':
            return Drug
        if type_name == 'SequenceVariant':
            return SequenceVariant
        if type_name == 'AdverseEvent':
            return AdverseEvent
        if type_name == 'PhenotypicFeature':
            return PhenotypicFeature
        raise Exception(f'Unknown node type: {type_name}')

    def get_class(self, row: dict, prefix: str) -> type[CureNode]:
        type = row[f'{prefix}_type']
        return self.get_node_class(type)

    def set_node_data(self, node: CureNode, row: dict, prefix: str):
        node.name = row[f'{prefix}_label']
        node.resolution = row[f'{prefix}_resolution_source']
        node.confidence = float(row[f'{prefix}_confidence'])
        node.review_flag = row[f'{prefix}_review_flag'] == 'Y'
        node.review_reason = row[f'{prefix}_review_reason']