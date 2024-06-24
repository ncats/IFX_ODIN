from typing import List

from src.input_adapters.sqlite_ramp.analyte_adapter import AnalyteAdapter
from src.input_adapters.sqlite_ramp.tables import (
    Analyte as SqliteAnalyte,
    Catalyzed as SqliteCatalyzed)
from src.models.protein import Protein
from src.output_adapters.generic_labels import NodeLabel


class ProteinAdapter(AnalyteAdapter):
    name = "RaMP Protein Adapter"
    cached_child_audit_trail_info = None

    def get_audit_trail_entries(self, obj) -> List[str]:
        version_info = AnalyteAdapter.get_audit_trail_entries(self, obj)
        version_info.append(f"protein_type field based on HMDB version: {self.get_data_version('hmdb').version}")
        return version_info

    def get_source_prefix(self):
        return 'RAMP_G'

    def get_all(self):
        results = (self.get_session().query(
            SqliteAnalyte.rampId,
            SqliteCatalyzed.proteinType
        ).outerjoin(SqliteCatalyzed, SqliteAnalyte.rampId == SqliteCatalyzed.rampGeneId)
                   .filter(SqliteAnalyte.type == "gene").distinct())

        nodes: [Protein] = [
            Protein(id=row[0], protein_type=row[1], labels=[NodeLabel.Protein, NodeLabel.Analyte]) for row in results
        ]

        self.add_equivalent_ids(nodes)

        return nodes
