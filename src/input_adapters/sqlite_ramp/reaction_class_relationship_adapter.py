from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import RelationshipInputAdapter
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


class ReactionClassRelationshipAdapter(RelationshipInputAdapter, SqliteAdapter):
    name = "RaMP Reaction Class Relationship Adapter"
    start_id_normalizer = PassthroughNormalizer()
    end_id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        RelationshipInputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

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
                reaction_class=ReactionClass(id=pair.split('|')[0]),
                parent_class=ReactionClass(id=pair.split('|')[1])
            ) for pair in relationship_set
        ]
        return relationships

