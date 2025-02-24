from typing import List

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.metabolite_class import MetaboliteClass
from src.input_adapters.sqlite_ramp.tables import MetaboliteClass as SqliteMetaboliteClass


class MetaboliteClassAdapter(InputAdapter, RaMPSqliteAdapter):
    def get_audit_trail_entries(self, obj: MetaboliteClass) -> List[str]:
        data_version = self.get_data_version(obj.source)
        return [f"Metabolite Class from {data_version.name} ({data_version.version})"]

    name = "RaMP Metabolite Class Adapter"

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteMetaboliteClass.class_level_name,
            SqliteMetaboliteClass.class_name,
            SqliteMetaboliteClass.source
        ).distinct().all()

        metabolite_classes: [MetaboliteClass] = [
            MetaboliteClass(
                id=MetaboliteClass.compiled_name(row[0], row[1]),
                level=row[0],
                name=row[1],
                source=row[2]) for row in
            results
        ]
        return metabolite_classes
