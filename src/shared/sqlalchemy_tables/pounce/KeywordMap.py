from sqlalchemy import Column, String, Integer, UniqueConstraint
from .Base import Base

class KeywordMap(Base):
    __tablename__ = 'keyword_map'

    id = Column(Integer, primary_key=True)
    keyword = Column(String(128), nullable=False)
    entity_type = Column(String(45), nullable=False)
    entity_id = Column(Integer, nullable=False)

UniqueConstraint(KeywordMap.keyword, KeywordMap.entity_type, KeywordMap.entity_id, name='keyword_unique_idx')
