from typing import List

from sqlalchemy import desc
from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.input_adapters.sqlite_ramp.tables import DBVersion as SqliteDBVersion, VersionInfo as SqliteVersionInfo
from src.interfaces.input_adapter import NodeInputAdapter, RelationshipInputAdapter
from src.models.version import DatabaseVersion, DataVersion, DatabaseDataVersionRelationship


class VersionMetaAdapter(NodeInputAdapter, RelationshipInputAdapter, RaMPSqliteAdapter):
    name = "RaMP Metadata Adapter"

    def get_audit_trail_entries(self, obj) -> List[str]:
        return [
            f"RaMP data version: {self.get_database_version().id}"
        ]

    def __init__(self, sqlite_file):
        NodeInputAdapter.__init__(self)
        RelationshipInputAdapter.__init__(self)
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        result = self.get_session().query(
            SqliteDBVersion.ramp_version,
            SqliteDBVersion.load_timestamp,
            SqliteDBVersion.version_notes
        ).order_by(desc(SqliteDBVersion.ramp_version)).first()

        db_version = DatabaseVersion(
            id = result[0],
            timestamp = result[1],
            notes= result[2]
        )

        results = self.get_session().query(
            SqliteVersionInfo.data_source_id,
            SqliteVersionInfo.data_source_name,
            SqliteVersionInfo.data_source_url,
            SqliteVersionInfo.data_source_version
        ).filter(SqliteVersionInfo.status == 'current').all()

        data_versions: [DataVersion] = [
            DataVersion(
                id=row[0],
                name=row[1],
                url=row[2],
                version=row[3]
            ) for row in results
        ]

        relationships: [DatabaseDataVersionRelationship] = [
            DatabaseDataVersionRelationship(
                start_node=db_version,
                end_node=data_version
            ) for data_version in data_versions
        ]
        return [db_version, *data_versions, *relationships]

