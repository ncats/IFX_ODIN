from typing import List

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.interfaces.input_adapter import RelationshipInputAdapter
from src.models.metabolite import Metabolite
from src.models.metabolite_class import MetaboliteClassRelationship, MetaboliteClass
from src.input_adapters.sqlite_ramp.tables import MetaboliteClass as SqliteMetaboliteClass


class MetaboliteClassRelationshipAdapter(RelationshipInputAdapter, RaMPSqliteAdapter):
    name = "RaMP Metabolite Class Relationship Adapter"

    def get_audit_trail_entries(self, obj: MetaboliteClassRelationship) -> List[str]:
        data_version = self.get_data_version(obj.source)
        return [f"Metabolite Class Relationship from {data_version.name} ({data_version.version})"]

    def __init__(self, sqlite_file):
        RelationshipInputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteMetaboliteClass.ramp_id,
            SqliteMetaboliteClass.class_level_name,
            SqliteMetaboliteClass.class_name,
            SqliteMetaboliteClass.source
        ).all()

        metabolite_class_relationships: [MetaboliteClassRelationship] = [
            MetaboliteClassRelationship(
                start_node=Metabolite(id=row[0]),
                end_node=MetaboliteClass(
                    id=MetaboliteClass.compiled_name(row[1], row[2]),
                    level=row[1],
                    name=row[2]),
                source=row[3]
            ) for row in results
        ]
        return metabolite_class_relationships

