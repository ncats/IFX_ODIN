from typing import List, Union

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.interfaces.input_adapter import NodeInputAdapter, RelationshipInputAdapter
from src.models.metabolite import Metabolite, MetaboliteChemProps, MetaboliteChemPropsRelationship
from src.input_adapters.sqlite_ramp.tables import ChemProps as SqliteChemProps


class MetaboliteChemPropsAdapter(NodeInputAdapter, RelationshipInputAdapter, RaMPSqliteAdapter):
    def get_audit_trail_entries(self, obj: Union[MetaboliteChemProps, MetaboliteChemPropsRelationship]) -> List[str]:
        if isinstance(obj, MetaboliteChemProps):
            data_version = self.get_data_version(obj.chem_data_source)
        else:
            data_version = self.get_data_version(obj.end_node.chem_data_source)
        return [f"Chemical Properties from {data_version.name} ({data_version.version})"]

    name = "RaMP Metabolite ChemProps Adapter"

    def __init__(self, sqlite_file):
        NodeInputAdapter.__init__(self)
        RelationshipInputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

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
                mol_formula=row[10],
                id="temp"
            )
            chem_prop_obj.set_id()
            nodes_and_relationships.append(chem_prop_obj)
            nodes_and_relationships.append(MetaboliteChemPropsRelationship(
                start_node=Metabolite(id=row[0]),
                end_node=chem_prop_obj
            ))
        return nodes_and_relationships
