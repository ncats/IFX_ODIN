from typing import List, Generator

from src.input_adapters.sqlite_ramp.analyte_adapter import AnalyteAdapter
from src.models.metabolite import Metabolite
from src.input_adapters.sqlite_ramp.tables import Analyte as SqliteAnalyte


class MetaboliteAdapter(AnalyteAdapter):
    def get_source_prefix(self):
        return 'RAMP_C'

    def get_all(self) -> Generator[List[Metabolite], None, None]:
        results = self.get_session().query(
            SqliteAnalyte.rampId
        ).filter(SqliteAnalyte.type == "compound").all()

        metabolites: List[Metabolite] = [
            Metabolite(
                id=row[0]
            ) for row in results
        ]

        self.add_equivalent_ids(metabolites)

        yield metabolites
