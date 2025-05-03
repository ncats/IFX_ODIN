import dataclasses
from dataclasses import dataclass
from typing import List

import graphene
import pytest
from humanfriendly.terminal import output

from src.api_adapters.graphene_utils import GrapheneConverter
from src.models.protein import Protein
from tests.test_classes import NonDataClass, SimpleFieldClass, ListFieldClass, ChildClass, ParentClass, ChildClass2, \
    ClassWithEnum, ClassWithUnion


def test_input_class_must_be_dataclass():
    # Arrange
    input_class = NonDataClass
    # Act
    with pytest.raises(TypeError):
        output_class = GrapheneConverter.from_dataclass(input_class)


def test_classes_with_simple_fields():
    # Arrange
    input_class = SimpleFieldClass
    # Act
    output_class = GrapheneConverter.from_dataclass(input_class)
    # Assert
    assert output_class.__name__ == "SimpleFieldClass"

    assert 'id' in output_class._meta.fields

    assert len(output_class._meta.fields) == 4

    assert isinstance(output_class._meta.fields['id'], graphene.types.field.Field)
    assert output_class._meta.fields['id'].type == graphene.String

    assert isinstance(output_class._meta.fields['num'], graphene.types.field.Field)
    assert output_class._meta.fields['num'].type == graphene.Int

    assert isinstance(output_class._meta.fields['yes'], graphene.types.field.Field)
    assert output_class._meta.fields['yes'].type == graphene.Boolean

    assert isinstance(output_class._meta.fields['dec'], graphene.types.field.Field)
    assert output_class._meta.fields['dec'].type == graphene.Float


def test_classes_with_list_fields():
    # Arrange
    input_class = ListFieldClass
    # Act
    output_class = GrapheneConverter.from_dataclass(input_class)
    # Assert
    assert output_class.__name__ == "ListFieldClass"

    assert len(output_class._meta.fields) == 4

    assert isinstance(output_class._meta.fields['id'], graphene.types.field.Field)
    assert output_class._meta.fields['id'].type == graphene.List(graphene.String)

    assert isinstance(output_class._meta.fields['num'], graphene.types.field.Field)
    assert output_class._meta.fields['num'].type == graphene.List(graphene.Int)

    assert isinstance(output_class._meta.fields['yes'], graphene.types.field.Field)
    assert output_class._meta.fields['yes'].type == graphene.List(graphene.Boolean)

    assert isinstance(output_class._meta.fields['dec'], graphene.types.field.Field)
    assert output_class._meta.fields['dec'].type == graphene.List(graphene.Float)

def test_nested_class():
    # Arrange
    input_class = ChildClass
    # Act
    output_class = GrapheneConverter.from_dataclass(input_class)
    # Assert
    assert output_class.__name__ == "ChildClass"
    assert len(output_class._meta.fields) == 3

    assert isinstance(output_class._meta.fields['id'], graphene.types.field.Field)
    assert output_class._meta.fields['id'].type == graphene.String
    assert isinstance(output_class._meta.fields['data'], graphene.types.field.Field)
    assert output_class._meta.fields['data'].type == graphene.String
    assert isinstance(output_class._meta.fields['parent'], graphene.types.field.Field)

    output_parent_class = output_class._meta.fields['parent'].type
    assert output_parent_class.__name__ == "ParentClass"
    assert len(output_parent_class._meta.fields) == 2
    assert isinstance(output_parent_class._meta.fields['id'], graphene.types.field.Field)
    assert output_parent_class._meta.fields['id'].type == graphene.String
    assert isinstance(output_parent_class._meta.fields['data'], graphene.types.field.Field)
    assert output_parent_class._meta.fields['data'].type == graphene.String

def test_nested_class2():
    # Arrange
    child_class = ChildClass2

    # Act
    output_class = GrapheneConverter.from_dataclass(child_class)

    # Assert
    assert output_class.__name__ == "ChildClass2"
    assert len(output_class._meta.fields) == 2

    assert isinstance(output_class._meta.fields['id'], graphene.types.field.Field)
    assert output_class._meta.fields['id'].type == graphene.String

    parent_field = output_class._meta.fields['parents']
    assert isinstance(parent_field, graphene.types.field.Field)
    assert isinstance(parent_field.type, graphene.List)

    output_parent_class = parent_field.type.of_type
    assert output_parent_class.__name__ == "ParentClass"
    assert len(output_parent_class._meta.fields) == 2
    assert isinstance(output_parent_class._meta.fields['id'], graphene.types.field.Field)
    assert output_parent_class._meta.fields['id'].type == graphene.String
    assert isinstance(output_parent_class._meta.fields['data'], graphene.types.field.Field)
    assert output_parent_class._meta.fields['data'].type == graphene.String

def test_classes_with_enum():
    # Arrange
    input_class = ClassWithEnum
    # Act
    output_class = GrapheneConverter.from_dataclass(input_class)
    # Assert
    assert output_class.__name__ == "ClassWithEnum"
    assert len(output_class._meta.fields) == 3

    assert isinstance(output_class._meta.fields['id'], graphene.types.field.Field)
    assert output_class._meta.fields['id'].type == graphene.String

    cat_field = output_class._meta.fields['cat']
    assert isinstance(cat_field, graphene.types.field.Field)
    assert cat_field.type.__name__ == "Category"
    assert issubclass(cat_field.type, graphene.Enum)

    cats_field = output_class._meta.fields['cats']
    assert isinstance(cats_field, graphene.types.field.Field)
    assert isinstance(cats_field.type, graphene.List)
    assert cats_field.type.of_type.__name__ == "Category"
    assert issubclass(cats_field.type.of_type, graphene.Enum)

def test_union_classes():
    # Arrange
    input_class = ClassWithUnion
    # Act
    output_class = GrapheneConverter.from_dataclass(input_class)
    # Assert
    assert output_class.__name__ == "ClassWithUnion"
    assert len(output_class._meta.fields) == 5


def test_protein():
    input_class = Protein
    output_class = GrapheneConverter.from_dataclass(Protein)
    assert output_class.__name__ == "Protein"