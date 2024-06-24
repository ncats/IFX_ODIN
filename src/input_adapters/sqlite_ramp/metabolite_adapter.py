from src.input_adapters.sqlite_ramp.analyte_adapter import AnalyteAdapter
from src.models.metabolite import Metabolite
from src.input_adapters.sqlite_ramp.tables import Analyte as SqliteAnalyte
from src.output_adapters.generic_labels import NodeLabel


class MetaboliteAdapter(AnalyteAdapter):
    name = "RaMP Metabolite Adapter"

    def get_source_prefix(self):
        return 'RAMP_C'

    def get_all(self):
        results = self.get_session().query(
            SqliteAnalyte.rampId
        ).filter(SqliteAnalyte.type == "compound").all()

        metabolites: [Metabolite] = [
            Metabolite(
                id=row[0],
                labels=[NodeLabel.Metabolite, NodeLabel.Analyte]) for row in results
        ]

        self.add_equivalent_ids(metabolites)

        return metabolites
