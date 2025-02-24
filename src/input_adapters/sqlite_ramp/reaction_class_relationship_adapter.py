from typing import List

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.input_adapters.sqlite_ramp.tables import ReactionClass as SqliteReactionClass
from src.models.reaction import ReactionClass, ReactionClassParentRelationship


def get_parent_ec(ec: str):
    root, grand, parent, leaf = ec.split('.')
    if grand == '-':
        return None
    if parent == '-':
        return f'{root}.-.-.-'
    if leaf == '-':
        return f'{root}.{grand}.-.-'
    return f'{root}.{grand}.{parent}.-'


class ReactionClassRelationshipAdapter(RelationshipInputAdapter, RaMPSqliteAdapter):
    name = "RaMP Reaction Class Relationship Adapter"

    def get_audit_trail_entries(self, obj) -> List[str]:
        data_version = self.get_data_version('rhea')
        return [f"Reaction Class Relationship from {data_version.name} ({data_version.version})"]

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteReactionClass.rxn_class_ec
        ).filter(SqliteReactionClass.ec_level == 4).distinct().all()

        relationship_set = set()

        for row in results:
            ec = row[0]
            parent = get_parent_ec(ec)
            while parent is not None:
                relationship_set.add(f"{ec}|{parent}")
                ec = parent
                parent = get_parent_ec(ec)

        relationships: [ReactionClassParentRelationship] = [
            ReactionClassParentRelationship(
                start_node=ReactionClass(id=pair.split('|')[0]),
                end_node=ReactionClass(id=pair.split('|')[1])
            ) for pair in relationship_set
        ]
        return relationships

