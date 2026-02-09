from sqlalchemy import Column, Integer, String, BLOB, ForeignKey
from .Base import Base

class Experiment(Base):
    __tablename__ = 'experiment'

    id = Column(Integer, primary_key=True, autoincrement=True)
    expt_display_id = Column(String(32))
    project_id = Column(Integer, ForeignKey('project.id'))
    name = Column(String(256), unique=True)
    description = Column(String(4096))
    expt_design = Column(String(4096))
    expt_run_date = Column(String(32))
    doc = Column(BLOB)
    data_type = Column(String(45))
    sample_count = Column(Integer)
    analyte_count = Column(Integer)

