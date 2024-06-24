from dataclasses import dataclass
from src.models.node import Node


@dataclass
class MetaboliteChemProps(Node):
    chem_data_source: str = None
    chem_source_id: str = None
    iso_smiles: str = None
    inchi_key_prefix: str = None
    inchi_key: str = None
    inchi: str = None
    mw: float = None
    monoisotop_mass: float = None
    common_name: str = None
    mol_formula: str = None

    def set_id(self):
        filtered_attributes = {k: v for k, v in self.__dict__.items() if not isinstance(v, list)}
        attributes = tuple(sorted(filtered_attributes.items()))
        self.id = str(hash(attributes))
