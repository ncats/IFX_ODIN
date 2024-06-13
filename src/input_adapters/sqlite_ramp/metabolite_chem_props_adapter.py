from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import NodeInputAdapter
from src.models.metabolite import Metabolite, MetaboliteChemProps, MetaboliteChemPropsRelationship
from src.input_adapters.sqlite_ramp.tables import ChemProps as SqliteChemProps


class MetaboliteChemPropsAdapter(NodeInputAdapter, SqliteAdapter):
    name = "RaMP Metabolite ChemProps Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        NodeInputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteChemProps.ramp_id,
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
        ).all()

        nodes_and_relationships = []
        for row in results:
            chem_prop_obj = MetaboliteChemProps(
                chem_data_source=row[1],
                chem_source_id=row[2],
                iso_smiles=row[3],
                inchi_key_prefix=row[4],
                inchi_key=row[5],
                inchi=row[6],
                mw=row[7],
                monoisotop_mass=row[8],
                common_name=row[9],
                mol_formula=row[10]
            )
            chem_prop_obj.set_id()
            nodes_and_relationships.append(chem_prop_obj)
            nodes_and_relationships.append(MetaboliteChemPropsRelationship(
                metabolite=Metabolite(id=row[0]),
                chem_prop=chem_prop_obj
            ))
        return nodes_and_relationships

