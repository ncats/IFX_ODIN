from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.sqlite_ramp.tables import Ontology as SqliteOntology
from src.input_adapters.util.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.ontology import Ontology


class OntologyAdapter(InputAdapter, SqliteAdapter):
    name = "RaMP Ontology Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_ontologies(self):
        results = self.get_session().query(
            SqliteOntology.rampOntologyId,
            SqliteOntology.commonName,
            SqliteOntology.HMDBOntologyType
        ).all()

        ontologies: [Ontology] = [
            Ontology(
                id=row[0],
                commonName=row[1],
                HMDBOntologyType=row[2]
            ) for row in results
        ]
        return ontologies

    def next(self):
        ontologies = self.get_ontologies()
        for ontology in ontologies:
            yield ontology
