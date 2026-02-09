from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint
from .Base import Base

class PlatformAnnotation(Base):
    __tablename__ = 'platform_annotation'

    id = Column(Integer, primary_key=True, autoincrement=True)
    expt_id = Column(Integer, ForeignKey('experiment.id'), nullable=False)
    platform_name = Column(String(45), nullable=False)
    data_key = Column(String(128), nullable=False)
    data = Column(String(4096), nullable=False)

UniqueConstraint(PlatformAnnotation.expt_id, PlatformAnnotation.platform_name, PlatformAnnotation.data_key, name='platmeta_expt_name_key_unique')

