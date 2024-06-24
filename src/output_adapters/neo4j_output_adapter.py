from dataclasses import fields
from enum import Enum
from typing import Union, List

from src.interfaces.output_adapter import OutputAdapter
from src.models.analyte import Analyte
from src.models.node import Relationship, Node

from src.output_adapters.generic_labels import NodeLabel
from src.shared.db_credentials import DBCredentials
from src.shared.neo4j_data_loader import Neo4jDataLoader


class Neo4jOutputAdapter(OutputAdapter):
    loader: Neo4jDataLoader

    def __init__(self, credentials: DBCredentials):
        self.loader = Neo4jDataLoader(credentials)

    def create_or_truncate_datastore(self) -> bool:
        return self.loader.delete_all_data_and_indexes()

    @staticmethod
    def default_headers_and_data(obj):
        headers = [field.name for field in fields(obj)]
        data = [getattr(obj, field) for field in headers]
        if isinstance(obj, Relationship):
            start_node = headers.index('start_node')
            end_node = headers.index('end_node')
            headers[start_node] = 'start_id'
            headers[end_node] = 'end_id'
            data[start_node] = obj.start_node.id
            data[end_node] = obj.end_node.id
        return headers, data

    def merge_nested_object_props_into_dict(self, ret_dict, obj):
        if isinstance(obj, Node):
            if hasattr(obj, 'equivalent_ids'):
                ret_dict['equivalent_ids'] = self.loader.remove_none_values_from_list(
                    list(set([equiv.id for equiv in obj.equivalent_ids])))
                ret_dict['equivalent_id_types'] = self.loader.remove_none_values_from_list(
                    list(set([equiv.type for equiv in obj.equivalent_ids])))
                ret_dict['equivalent_id_statuses'] = self.loader.remove_none_values_from_list(
                    list(set([equiv.status for equiv in obj.equivalent_ids])))
                ret_dict['equivalent_id_sources'] = self.loader.remove_none_values_from_list(
                    list(set([equiv.source for equiv in obj.equivalent_ids])))
        if isinstance(obj, Analyte):
            if hasattr(obj, 'synonyms'):
                ret_dict['synonyms'] = self.loader.remove_none_values_from_list(
                    list(set([syn.term for syn in obj.synonyms])))
                ret_dict['synonym_sources'] = self.loader.remove_none_values_from_list(
                    list(set([syn.source for syn in obj.synonyms])))

    def clean_dict(self, obj):
        def _clean_dict(obj):
            forbidden_keys = ['labels']
            if isinstance(obj, Relationship):
                forbidden_keys.extend(['start_node', 'end_node'])
            temp_dict = {}
            for key, val in obj.__dict__.items():
                if key in forbidden_keys:
                    continue
                if isinstance(val, Enum):
                    temp_dict[key] = val.value
                else:
                    temp_dict[key] = val
            return temp_dict

        ret_dict = _clean_dict(obj)
        self.merge_nested_object_props_into_dict(ret_dict, obj)
        return ret_dict

    def sort_and_convert_objects(self, objects: List[Union[Node, Relationship]]):
        object_lists = {}
        for obj in objects:

            obj_type = type(obj).__name__
            obj_labels = NodeLabel.to_list(obj.labels)
            obj_key = f"{obj_type}:{obj_labels}"

            if obj_key in object_lists:
                obj_list, _, _, _, _ = object_lists[obj_key]
                one_obj = self.clean_dict(obj)
                if isinstance(obj, Relationship):
                    one_obj['start_id'] = obj.start_node.id
                    one_obj['end_id'] = obj.end_node.id
                obj_list.append(one_obj)
            else:
                one_obj = self.clean_dict(obj)
                if isinstance(obj, Relationship):
                    one_obj['start_id'] = obj.start_node.id
                    one_obj['end_id'] = obj.end_node.id
                    object_lists[obj_key] = ([one_obj], obj.labels, True,
                                             NodeLabel.to_list(obj.start_node.labels),
                                             NodeLabel.to_list(obj.end_node.labels))
                else:
                    object_lists[obj_key] = [one_obj], obj.labels, False, None, None

        return object_lists

    def store(self, objects) -> bool:
        if not isinstance(objects, list):
            objects = [objects]

        with self.loader.driver.session() as session:
            object_groups = self.sort_and_convert_objects(objects)
            for obj_list, labels, is_relationship, start_label, end_label in object_groups.values():
                if is_relationship:
                    self.loader.load_relationship_records(session, obj_list, start_label, labels, end_label)
                else:
                    self.loader.load_node_records(session, obj_list, labels)
        return True
