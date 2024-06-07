from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.util.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.metabolite import Metabolite
from src.models.metabolite_class import MetaboliteClassRelationship, MetaboliteClass
from src.input_adapters.sqlite_ramp.tables import MetaboliteClass as SqliteMetaboliteClass


class MetaboliteClassRelationshipAdapter(InputAdapter, SqliteAdapter):
    name = "RaMP Metabolite Class Relationship Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all_metabolite_relationships(self):
        results = self.get_session().query(
            SqliteMetaboliteClass.ramp_id,
            SqliteMetaboliteClass.class_level_name,
            SqliteMetaboliteClass.class_name,
            SqliteMetaboliteClass.source
        ).all()

        metabolite_class_relationships: [MetaboliteClassRelationship] = [
            MetaboliteClassRelationship(
                metabolite=Metabolite(id=row[0]),
                met_class=MetaboliteClass(level=row[1], name=row[2]),
                source=row[3]
            ) for row in results
        ]
        return metabolite_class_relationships

    def next(self):
        metabolite_relationship = self.get_all_metabolite_relationships()
        for met_rel in metabolite_relationship:
            yield met_rel
