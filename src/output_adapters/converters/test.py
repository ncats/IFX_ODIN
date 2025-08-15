from src.input_adapters.test_mysql.test import create_test_classes
from src.models.test_models import TestRelationship

test_classes = create_test_classes()
TestBase = test_classes['Base']
mysql_TestNode = test_classes['Node']
mysql_TestRelationship = test_classes['Relationship']


def node_converter(obj: dict) -> mysql_TestNode:
    return mysql_TestNode(
        id = obj['id'],
        field_1 = obj['field_1'],
        field_2 = obj['field_2'],
        field_3 = obj['field_3'],
        provenance = obj['provenance']
    )


def rel_converter(obj: dict) -> TestRelationship:
    return mysql_TestRelationship(
        start_node = obj['start_id'],
        end_node = obj['end_id'],
        field_1 = obj['field_1'],
        field_2 = obj['field_2'],
        field_3 = obj['field_3'],
        provenance = obj['provenance']
    )
