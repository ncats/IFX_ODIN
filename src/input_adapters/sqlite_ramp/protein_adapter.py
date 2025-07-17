from typing import List, Generator

from src.input_adapters.sqlite_ramp.analyte_adapter import AnalyteAdapter
from src.input_adapters.sqlite_ramp.tables import (
    Analyte as SqliteAnalyte,
    Catalyzed as SqliteCatalyzed)
from src.models.protein import Protein


class ProteinAdapter(AnalyteAdapter):
    cached_child_audit_trail_info = None

    def get_source_prefix(self):
        return 'RAMP_G'

    def get_all(self) -> Generator[List[Protein], None, None]:
        results = (self.get_session().query(
            SqliteAnalyte.rampId,
            SqliteAnalyte.common_name,
            SqliteCatalyzed.proteinType
        ).outerjoin(SqliteCatalyzed, SqliteAnalyte.rampId == SqliteCatalyzed.rampGeneId)
                   .filter(SqliteAnalyte.type == "gene").distinct())

        nodes: List[Protein] = [
            Protein(id=row[0], name=row[1], protein_type=row[2]) for row in results
        ]

        self.add_equivalent_ids(nodes)

        yield nodes
