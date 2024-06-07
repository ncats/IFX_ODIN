from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.util.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.metabolite_class import MetaboliteClass
from src.input_adapters.sqlite_ramp.tables import MetaboliteClass as SqliteMetaboliteClass


class MetaboliteClassAdapter(InputAdapter, SqliteAdapter):
    name = "RaMP Metabolite Class Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all_metabolite_classes(self):
        results = self.get_session().query(
            SqliteMetaboliteClass.class_level_name,
            SqliteMetaboliteClass.class_name
        ).distinct().all()

        metabolite_classes: [MetaboliteClass] = [
            MetaboliteClass(level=row[0], name=row[1]) for row in results
        ]
        return metabolite_classes

    def next(self):
        met_classes = self.get_all_metabolite_classes()
        for met_class in met_classes:
            yield met_class
