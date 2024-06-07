from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.util.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.metabolite import Metabolite
from src.input_adapters.sqlite_ramp.tables import Analyte as SqliteAnalyte, ChemProps as SqliteChemProps


class MetaboliteAdapter(InputAdapter, SqliteAdapter):
    name = "RaMP Metabolite Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all_metabolites(self):
        results = (self.get_session().query(
            SqliteAnalyte.rampId,
            SqliteChemProps.chem_data_source,
            SqliteChemProps.chem_source_id,
            SqliteChemProps.iso_smiles,
            SqliteChemProps.inchi_key_prefix,
            SqliteChemProps.inchi_key,
            SqliteChemProps.inchi,
            SqliteChemProps.mw,
            SqliteChemProps.monoisotop_mass,
            SqliteChemProps.common_name,
            SqliteChemProps.mol_formula
        ).outerjoin(SqliteAnalyte, SqliteAnalyte.rampId == SqliteChemProps.ramp_id)
                   .filter(SqliteAnalyte.type == "compound").all())

        metabolite_dict = {}
        for row in results:
            ramp_id = row[0]
            if ramp_id in metabolite_dict:
                metabolite_dict[ramp_id].append(row)
            else:
                metabolite_dict[ramp_id] = [row]

        metabolites: [Metabolite] = [
            Metabolite(
                id=key,
                chem_data_source=[row[1] for row in prop_list],
                chem_source_id=[row[2] for row in prop_list],
                iso_smiles=[row[3] for row in prop_list],
                inchi_key_prefix=[row[4] for row in prop_list],
                inchi_key=[row[5] for row in prop_list],
                inchi=[row[6] for row in prop_list],
                mw=[row[7] for row in prop_list],
                monoisotop_mass=[row[8] for row in prop_list],
                common_name=[row[9] for row in prop_list],
                mol_formula=[row[10] for row in prop_list]
            ) for key, prop_list in metabolite_dict.items()
        ]
        return metabolites

    def next(self):
        metabolites = self.get_all_metabolites()
        for met in metabolites:
            yield met
