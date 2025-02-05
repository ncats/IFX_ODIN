from dataclasses import fields
from enum import Enum
from typing import Union, List

from src.interfaces.output_adapter import OutputAdapter
from src.interfaces.simple_enum import NodeLabel, SimpleEnum
from src.models.analyte import Analyte
from src.models.generif import GeneRif
from src.models.node import Relationship, Node
from src.models.pounce.investigator import InvestigatorRelationship

from src.shared.db_credentials import DBCredentials
from src.shared.neo4j_data_loader import Neo4jDataLoader


class Neo4jOutputAdapter(OutputAdapter):
    loader: Neo4jDataLoader
    post_processing: List[str]

    def __init__(self, credentials: DBCredentials, post_processing: List[str] = [], **kwargs):
        self.loader = Neo4jDataLoader(credentials, **kwargs)
        self.post_processing = post_processing

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
        for key, value in vars(obj).items():
            if isinstance(value, NodeLabel):
                ret_dict[key] = value.value
            if isinstance(value, list):
                ret_dict[key] = self.loader.remove_none_values_from_list(value)
                if ret_dict[key] is not None:
                    ret_dict[key] = [l.value if isinstance(l, SimpleEnum) else l for l in ret_dict[key]]
            if hasattr(value, 'to_dict') and callable(getattr(value, 'to_dict')):
                del ret_dict[key]
                flat_dict = value.to_dict()
                ret_dict.update(flat_dict)
            if key == "extra_properties":
                del ret_dict[key]
                for k in value:
                    if not k.startswith('_'):
                        ret_dict[k] = value[k]
        if isinstance(obj, Node):
            if hasattr(obj, 'xref'):
                ret_dict['xref'] = self.loader.remove_none_values_from_list(
                    list(set([x.id_str() for x in obj.xref]))
                )
        if isinstance(obj, Analyte):
            if hasattr(obj, 'synonyms'):
                ret_dict['synonyms'] = self.loader.remove_none_values_from_list(
                    list(set([syn.term for syn in obj.synonyms])))
                ret_dict['synonym_sources'] = self.loader.remove_none_values_from_list(
                    list(set([syn.source for syn in obj.synonyms])))
        if isinstance(obj, GeneRif):
            ret_dict['pmids'] = list(obj.pmids)
        if isinstance(obj, InvestigatorRelationship):
            ret_dict['roles'] = [role.value for role in obj.roles]

    def clean_dict(self, obj):
        def _clean_dict(obj):
            forbidden_keys = ['labels']
            if isinstance(obj, Relationship):
                forbidden_keys.extend(['start_node', 'end_node'])
            temp_dict = {}
            for key, val in obj.__dict__.items():
                if key in forbidden_keys:
                    continue
                if isinstance(val, list) and len(val) == 0:
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
            if isinstance(obj, Relationship):
                obj_key = f"{obj.start_node.labels}:{obj_labels}:{obj.end_node.labels}"

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
                                             obj.start_node.labels,
                                             obj.end_node.labels)
                else:
                    object_lists[obj_key] = [one_obj], obj.labels, False, None, None

        return object_lists

    def store(self, objects) -> bool:
        if not isinstance(objects, list):
            objects = [objects]

        with self.loader.driver.session() as session:
            object_groups = self.sort_and_convert_objects(objects)
            for obj_list, labels, is_relationship, start_labels, end_labels in object_groups.values():
                if is_relationship:
                    self.loader.load_relationship_records(session, obj_list, start_labels, labels, end_labels)
                else:
                    self.loader.load_node_records(session, obj_list, labels)
        return True

    def do_post_processing(self) -> None:
        for post_process in self.post_processing:
            print('Running post processing step:')
            print(post_process)
            with self.loader.driver.session() as session:
                session.run(post_process)
