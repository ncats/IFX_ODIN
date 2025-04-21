import numbers
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import List, Union

from src.interfaces.simple_enum import NodeLabel, SimpleEnum
from src.models.analyte import Analyte
from src.models.generif import GeneRif
from src.models.ligand import ProteinLigandRelationship
from src.models.node import Node, Relationship
from src.models.pounce.investigator import InvestigatorRelationship


class OutputAdapter(ABC):
    name: str

    @abstractmethod
    def store(self, objects) -> bool:
        pass

    def do_post_processing(self) -> None:
        pass

    @abstractmethod
    def create_or_truncate_datastore(self) -> bool:
        pass

    def preprocess_objects(self, objects):
        return objects

    def get_list_type(self, list):
        for item in list:
            if item is not None:
                return type(item)
        return str

    def remove_none_values_from_list(self, list):
        if len(list) == 0:
            return None
        type = self.get_list_type(list)
        return [self.get_none_val_for_type(type) if val is None else val for val in list]

    def is_numeric_type(self, tp):
        return issubclass(tp, numbers.Integral) or issubclass(tp, numbers.Real)

    def get_none_val_for_type(self, tp):
        if self.is_numeric_type(tp):
            return -1
        return ''

    def merge_nested_object_props_into_dict(self, ret_dict, obj):
        for key, value in vars(obj).items():
            if isinstance(value, NodeLabel):
                ret_dict[key] = value.value
            if isinstance(value, list):
                ret_dict[key] = self.remove_none_values_from_list(value)
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
                ret_dict['xref'] = self.remove_none_values_from_list(
                    list(set([x.id_str() for x in obj.xref]))
                )
        if isinstance(obj, ProteinLigandRelationship):
            if hasattr(obj, 'details'):
                del ret_dict['details']
                for details_obj in obj.details:
                    for key, value in vars(details_obj).items():
                        if value is not None:
                            if key in ret_dict:
                                ret_dict[key].append(value)
                            else:
                                ret_dict[key] = [value]

        if isinstance(obj, Analyte):
            if hasattr(obj, 'synonyms'):
                ret_dict['synonyms'] = self.remove_none_values_from_list(
                    list(set([syn.term for syn in obj.synonyms])))
                ret_dict['synonym_sources'] = self.remove_none_values_from_list(
                    list(set([syn.source for syn in obj.synonyms])))
        if isinstance(obj, GeneRif):
            ret_dict['pmids'] = list(obj.pmids)
        if isinstance(obj, InvestigatorRelationship):
            ret_dict['roles'] = [role.value for role in obj.roles]

    def clean_dict(self, obj, convert_dates: bool):
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
                if convert_dates and isinstance(val, datetime):
                    temp_dict[key] = val.isoformat()
                else:
                    temp_dict[key] = val
            return temp_dict

        ret_dict = _clean_dict(obj)
        self.merge_nested_object_props_into_dict(ret_dict, obj)
        return ret_dict

    def sort_and_convert_objects(self, objects: List[Union[Node, Relationship]], convert_dates: bool = False):
        object_lists = {}
        for obj in objects:

            obj_type = type(obj).__name__
            obj_labels = NodeLabel.to_list(obj.labels)
            obj_key = f"{obj_type}:{obj_labels}"
            if isinstance(obj, Relationship):
                obj_key = f"{obj.start_node.labels}:{obj_labels}:{obj.end_node.labels}"

            if obj_key in object_lists:
                obj_list, _, _, _, _ = object_lists[obj_key]
                one_obj = self.clean_dict(obj, convert_dates)
                if isinstance(obj, Relationship):
                    one_obj['start_id'] = obj.start_node.id
                    one_obj['end_id'] = obj.end_node.id
                obj_list.append(one_obj)
            else:
                one_obj = self.clean_dict(obj, convert_dates)
                if isinstance(obj, Relationship):
                    one_obj['start_id'] = obj.start_node.id
                    one_obj['end_id'] = obj.end_node.id
                    object_lists[obj_key] = ([one_obj], obj.labels, True,
                                             obj.start_node.labels,
                                             obj.end_node.labels)
                else:
                    object_lists[obj_key] = [one_obj], obj.labels, False, None, None

        return object_lists