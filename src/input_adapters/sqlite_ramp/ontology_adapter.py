from typing import List

from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.input_adapters.sqlite_ramp.tables import Ontology as SqliteOntology
from src.interfaces.input_adapter import NodeInputAdapter
from src.models.ontology import Ontology
from src.output_adapters.generic_labels import NodeLabel


class OntologyAdapter(NodeInputAdapter, RaMPSqliteAdapter):
    def get_audit_trail_entries(self, obj) -> List[str]:
        data_version = self.get_data_version('hmdb')
        return [f"Ontology from {data_version.name} ({data_version.version})"]

    name = "RaMP Ontology Adapter"

    def __init__(self, sqlite_file):
        NodeInputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteOntology.rampOntologyId,
            SqliteOntology.commonName,
            SqliteOntology.HMDBOntologyType
        ).all()

        ontologies: [Ontology] = [
            Ontology(
                id=row[0],
                commonName=row[1],
                HMDBOntologyType=row[2],
                labels=[NodeLabel.Ontology]
            ) for row in results
        ]
        return ontologies

