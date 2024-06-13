from dataclasses import dataclass


@dataclass
class MetaboliteChemProps:
    chem_data_source: str
    chem_source_id: str
    iso_smiles: str
    inchi_key_prefix: str
    inchi_key: str
    inchi: str
    mw: float
    monoisotop_mass: float
    common_name: str
    mol_formula: str
    id: str = None

    def set_id(self):
        attributes = tuple(sorted(self.__dict__.items()))
        self.id = str(hash(attributes))
