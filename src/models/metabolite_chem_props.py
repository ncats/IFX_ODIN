from dataclasses import dataclass
from typing import Optional

from src.models.node import Node


@dataclass
class MetaboliteChemProps(Node):
    chem_data_source: str = None
    chem_source_id: str = None
    iso_smiles: Optional[str] = None
    inchi_key_prefix: Optional[str] = None
    inchi_key: Optional[str] = None
    inchi: Optional[str] = None
    mw: Optional[float] = None
    monoisotop_mass: Optional[float] = None
    common_name: Optional[str] = None
    mol_formula: Optional[str] = None

    def set_id(self):
        filtered_attributes = {k: v for k, v in self.__dict__.items() if not isinstance(v, list)}
        attributes = tuple(sorted(filtered_attributes.items()))
        self.id = str(hash(attributes))
