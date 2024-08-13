from typing import List

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.input_adapters.sqlite_ramp.tables import ReactionClass as SqliteReactionClass
from src.interfaces.input_adapter import NodeInputAdapter
from src.models.reaction import ReactionClass


class ReactionClassAdapter(NodeInputAdapter, RaMPSqliteAdapter):
    name = "RaMP Reaction Class Adapter"
    def get_audit_trail_entries(self, obj) -> List[str]:
        data_version = self.get_data_version('rhea')
        return [f"Reaction Class from {data_version.name} ({data_version.version})"]

    def __init__(self, sqlite_file):
        NodeInputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteReactionClass.rxn_class_ec,
            SqliteReactionClass.ec_level,
            SqliteReactionClass.rxn_class
        ).distinct()

        reaction_classes: [ReactionClass] = [
            ReactionClass(
                id=row[0],
                level=row[1],
                name=row[2]
            ) for row in results
        ]
        return reaction_classes

