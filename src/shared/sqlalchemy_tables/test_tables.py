from sqlalchemy import Column, Text, String, ForeignKey, Integer
from sqlalchemy.orm import declared_attr, declarative_base


class BaseMixin:
    @declared_attr
    def provenance(cls):
        return Column(Text)

Base = declarative_base(cls=BaseMixin)

class Node(Base):
    __tablename__ = 'node'
    id = Column(String(18), primary_key=True, nullable=False)
    field_1 = Column(String(50))
    field_2 = Column(String(50))
    field_3 = Column(String(50))

class Relationship(Base):
    __tablename__ = 'relationship'
    start_node = Column(String(18), ForeignKey('node.id'), primary_key=True, nullable=False)
    end_node = Column(String(18), ForeignKey('node.id'), primary_key=True, nullable=False)
    field_1 = Column(String(50))
    field_2 = Column(String(50))
    field_3 = Column(String(50))

class AutoIncNode(Base):
    __tablename__ = 'auto_inc_node'
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    identifier = Column(String(50), nullable=False)
    value = Column(String(50))

class TwoKeyAutoInc(Base):
    __tablename__ = "two_key_auto_inc_node"
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    key1 = Column(String(50), nullable=False)
    key2 = Column(String(50), nullable=False)
    value = Column(String(50))