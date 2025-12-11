from sqlalchemy import Column, Integer, String, UniqueConstraint
from .Base import Base

class VarMap(Base):
    __tablename__ = 'var_map'

    var_id = Column(Integer, primary_key=True, autoincrement=True)
    pounce_var_key = Column(String(64), nullable=False)
    pounce_var_descr = Column(String(1024), nullable=False)
    valid_vals_str = Column(String(256), nullable=False)
    var_category = Column(String(64), nullable=False)


UniqueConstraint(VarMap.pounce_var_key, VarMap.var_category, name='var_key_var_cat_uidx')
