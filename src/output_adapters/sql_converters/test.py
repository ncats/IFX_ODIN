from typing import Union, List

from src.models.test_models import TestNode, TestRelationship, TwoKeyAutoIncNode, AutoIncNode
from src.output_adapters.sql_converters.output_converter_base import SQLOutputConverter
from src.shared.sqlalchemy_tables.test_tables import Node, AutoIncNode as mysqlAIN, Relationship, TwoKeyAutoInc


class TestSQLOutputConverter(SQLOutputConverter):

    def get_object_converters(self, obj_cls) -> Union[callable, List[callable], None]:
        if obj_cls == TestNode:
            return node_converter
        if obj_cls == TestRelationship:
            return rel_converter
        if obj_cls == AutoIncNode:
            return auto_inc_node_converter
        if obj_cls == TwoKeyAutoIncNode:
            return two_key_auto_inc_node_converter
        return None



def node_converter(obj: dict) -> Node:
    return Node(
        id = obj['id'],
        field_1 = obj['field_1'],
        field_2 = obj['field_2'],
        field_3 = obj['field_3'],
        provenance = obj['provenance']
    )

def auto_inc_node_converter(obj: dict) -> mysqlAIN:
    return mysqlAIN(
        identifier = obj['identifier'],
        value = obj['value'],
        provenance = obj['provenance']
    )
setattr(auto_inc_node_converter, 'merge_fields', ['identifier'])

def two_key_auto_inc_node_converter(obj: dict) -> TwoKeyAutoInc:
    return TwoKeyAutoInc(
        key1 = obj['key1'],
        key2 = obj['key2'],
        value = obj['value'],
        provenance = obj['provenance']
    )
setattr(two_key_auto_inc_node_converter, 'merge_fields', ['key1', 'key2'])

def rel_converter(obj: dict) -> Relationship:
    return Relationship(
        start_node = obj['start_id'],
        end_node = obj['end_id'],
        field_1 = obj['field_1'],
        field_2 = obj['field_2'],
        field_3 = obj['field_3'],
        provenance = obj['provenance']
    )

