from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import RelationshipInputAdapter
from src.input_adapters.sqlite_ramp.tables import AnalyteHasOntology as SqliteAnalyteHasOntology
from src.models.analyte import Analyte
from src.models.ontology import AnalyteOntologyRelationship, Ontology


class MetaboliteOntologyRelationshipAdapter(RelationshipInputAdapter, SqliteAdapter):
    name = "RaMP Metabolite Ontology Relationship Adapter"
    start_id_normalizer = PassthroughNormalizer()
    end_id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        RelationshipInputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteAnalyteHasOntology.rampCompoundId,
            SqliteAnalyteHasOntology.rampOntologyId
        ).all()

        analyte_ontology_relationships: [AnalyteOntologyRelationship] = [
            AnalyteOntologyRelationship(
                analyte=Analyte(id=row[0]),
                ontology=Ontology(id=row[1])
            ) for row in results
        ]
        return analyte_ontology_relationships

