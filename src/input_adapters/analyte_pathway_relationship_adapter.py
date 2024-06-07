from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.util.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.input_adapters.sqlite_ramp.tables import AnalytePathwayRelationship as SqliteAnalytePathwayRelationship
from src.models.analyte import Analyte
from src.models.pathway import AnalytePathwayRelationship, Pathway


class AnalytePathwayRelationshipAdapter(InputAdapter, SqliteAdapter):
    name = "RaMP Analyte Pathway Relationship Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all_pathway_relationships(self):
        results = self.get_session().query(
            SqliteAnalytePathwayRelationship.rampId,
            SqliteAnalytePathwayRelationship.pathwayRampId
        ).all()

        analyte_pathway_relationships: [AnalytePathwayRelationship] = [
            AnalytePathwayRelationship(
                analyte=Analyte(id=row[0]),
                pathway=Pathway(id=row[1])
            ) for row in results
        ]
        return analyte_pathway_relationships

    def next(self):
        pathway_relationships = self.get_all_pathway_relationships()
        for path_rel in pathway_relationships:
            yield path_rel
