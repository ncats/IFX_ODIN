from typing import List, Generator

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.input_adapters.sqlite_ramp.tables import Catalyzed as SqliteCatalyzed
from src.models.protein import Protein
from src.models.metabolite import MetaboliteProteinRelationship, Metabolite


class MetaboliteProteinRelationshipAdapter(RaMPSqliteAdapter):
    cached_audit_trail_info = None

    def __init__(self, sqlite_file):
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self) -> Generator[List[MetaboliteProteinRelationship], None, None]:
        results = self.get_session().query(
            SqliteCatalyzed.rampCompoundId,
            SqliteCatalyzed.rampGeneId
        ).all()

        relationships: List[MetaboliteProteinRelationship] = [
            MetaboliteProteinRelationship(
                start_node=Metabolite(id=row[0]),
                end_node=Protein(id=row[1])
            ) for row in results
        ]
        yield relationships
