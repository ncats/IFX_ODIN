from sqlalchemy import Column, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import declared_attr


class BaseMixin:
    @declared_attr
    def provenance(cls):
        return Column(Text)

Base = declarative_base(cls=BaseMixin)

