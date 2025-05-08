from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Optional

from src.models.node import generate_class_from_dict, Node


@dataclass
class MyClass:
    id: str

def test_simple_creation():
    cls = MyClass
    data = {'id': "id"}

    obj = generate_class_from_dict(cls, data)

    assert isinstance(obj, MyClass)
    assert obj.id == data['id']

@dataclass
class ParentClass:
    id: str
    sub: MyClass

def test_nested_creation():
    cls = ParentClass
    data = {'id': "id", 'sub': {'id': "sub_id"}}

    obj = generate_class_from_dict(cls, data)

    assert isinstance(obj, ParentClass)
    assert obj.id == data['id']
    assert isinstance(obj.sub, MyClass)
    assert obj.sub.id == data['sub']['id']

@dataclass
class ParentClassWithList:
    id: str
    sub: list[MyClass]
    sub2: List[MyClass]

def test_list_creation():
    cls = ParentClassWithList
    data = {'id': "id", 'sub': [{'id': "sub_id"}], 'sub2': [{'id': "sub_id2"}]}

    obj = generate_class_from_dict(cls, data)

    assert isinstance(obj, ParentClassWithList)
    assert obj.id == data['id']

    assert isinstance(obj.sub, list)
    assert len(obj.sub) == 1
    assert isinstance(obj.sub[0], MyClass)
    assert obj.sub[0].id == data['sub'][0]['id']

    assert isinstance(obj.sub2, list)
    assert len(obj.sub2) == 1
    assert isinstance(obj.sub2[0], MyClass)
    assert obj.sub2[0].id == data['sub2'][0]['id']


@dataclass
class Complex:
    prefix: str
    id: str

    @staticmethod
    def parse(input: str):
        prefix, id = input.split(":", 1)
        return Complex(prefix=prefix, id=id)

@dataclass
class ReverseComplex:
    prefix: str
    id: str

    @classmethod
    def parse(cls, input: str):
        prefix, id = input.split(":", 1)
        return ReverseComplex(prefix=id, id=prefix)

@dataclass
class ClassWithComplex:
    id: str
    complex: Complex
    complex2: ReverseComplex
    complexList: List[Complex]
    complex2List: List[ReverseComplex]

def test_complex_creation():
    cls = ClassWithComplex
    data = {
        'id': "id",
        'complex': 'prefix1:id1',
        'complex2': 'prefix2:id2',
        'complexList': ['prefix3:id3'],
        'complex2List': ['prefix4:id4']
    }
    obj = generate_class_from_dict(cls, data)
    assert isinstance(obj, ClassWithComplex)
    assert obj.id == data['id']

    assert isinstance(obj.complex, Complex)
    assert obj.complex.prefix == 'prefix1'
    assert obj.complex.id == 'id1'

    assert isinstance(obj.complex2, ReverseComplex)
    assert obj.complex2.prefix == 'id2'
    assert obj.complex2.id == 'prefix2'

    assert isinstance(obj.complexList, list)
    assert len(obj.complexList) == 1
    assert isinstance(obj.complexList[0], Complex)
    assert obj.complexList[0].prefix == 'prefix3'
    assert obj.complexList[0].id == 'id3'

    assert isinstance(obj.complex2List, list)
    assert len(obj.complex2List) == 1
    assert isinstance(obj.complex2List[0], ReverseComplex)
    assert obj.complex2List[0].prefix == 'id4'
    assert obj.complex2List[0].id == 'prefix4'

@dataclass
class GrandParentClass:
    id: str
    sub: ParentClass

def test_grandparent_creation():
    cls = GrandParentClass
    data = {'id': "id", 'sub': {'id': "sub_id", 'sub': {'id': "sub_sub_id"}}}

    obj = generate_class_from_dict(cls, data)

    assert isinstance(obj, GrandParentClass)
    assert obj.id == data['id']
    assert isinstance(obj.sub, ParentClass)
    assert obj.sub.id == data['sub']['id']
    assert isinstance(obj.sub.sub, MyClass)
    assert obj.sub.sub.id == data['sub']['sub']['id']

@dataclass
class ClassWithDates:
    id: str
    created: date
    updated: datetime

def test_class_with_dates():
    cls = ClassWithDates
    data = {'id': "id", 'created': "2023-10-01", 'updated': "2023-10-02T12:00:00"}

    obj = generate_class_from_dict(cls, data)

    assert isinstance(obj, ClassWithDates)
    assert obj.id == data['id']
    assert isinstance(obj.updated, datetime)
    assert obj.updated == datetime.fromisoformat(data['updated'])
    assert isinstance(obj.created, date)
    assert obj.created == date.fromisoformat(data['created'])

@dataclass
class ClassWithOptionals:
    id: str
    optStr: Optional[str]
    optCls: Optional[MyClass]
    optStrList: Optional[List[str]]
    optClsList: Optional[List[MyClass]]

def test_class_with_optionals():
    cls = ClassWithOptionals
    data = {
        'id': "id",
        'optStr': "optStr",
        'optCls': {'id': "optCls"},
        'optStrList': ['optStrList'],
        'optClsList': [{'id': "optClsList"}]
    }

    obj = generate_class_from_dict(cls, data)
    assert isinstance(obj, ClassWithOptionals)

