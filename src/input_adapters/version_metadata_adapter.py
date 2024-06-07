from sqlalchemy import desc

from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.sqlite_ramp.tables import DBVersion as SqliteDBVersion, VersionInfo as SqliteVersionInfo
from src.input_adapters.util.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.version import DatabaseVersion, DataVersion, DatabaseDataVersionRelationship


class VersionMetaAdapter(InputAdapter, SqliteAdapter):
    name = "RaMP Metadata Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

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
                database=db_version,
                data=data_version
            ) for data_version in data_versions
        ]
        return [db_version, *data_versions, *relationships]

    def next(self):
        nodes = self.get_all()
        for node in nodes:
            yield node
