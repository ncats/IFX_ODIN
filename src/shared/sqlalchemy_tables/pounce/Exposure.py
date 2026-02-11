from sqlalchemy import Column, Integer, ForeignKey, String, Float, Index, UniqueConstraint

from src.shared.sqlalchemy_tables.pounce.Base import Base


class Exposure(Base):
    __tablename__ = 'exposure'
    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String(256))
    category = Column(String(256))
    category_value = Column(String(256))
    concentration = Column(Float)
    concentration_unit = Column(String(256))
    duration = Column(Float)
    duration_unit = Column(String(256))
    start_time = Column(String(256))
    end_time = Column(String(256))
    growth_media = Column(String(256))
    condition_category = Column(String(256))
    condition_category_value = Column(String(256))

class ExposureNames(Base):
    __tablename__ = 'exposure_names'
    id = Column(Integer, primary_key=True, autoincrement=True)
    exposure_id = Column(Integer, ForeignKey('exposure.id'), nullable=False)
    name = Column(String(256), nullable=False)

Index(None, ExposureNames.exposure_id)
Index(None, ExposureNames.name)
UniqueConstraint(ExposureNames.exposure_id, ExposureNames.name)

class BiosampleExposure(Base):
    __tablename__ = 'biosample_exposure'
    biosample_id = Column(Integer, ForeignKey('biosample.id'), primary_key=True)
    exposure_id = Column(Integer, ForeignKey('exposure.id'), primary_key=True)