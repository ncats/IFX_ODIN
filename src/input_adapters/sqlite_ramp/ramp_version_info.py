from typing import List

from sqlalchemy import desc
from sqlalchemy.orm import Session

from src.input_adapters.sqlite_ramp.tables import (
    VersionInfo as SqliteVersionInfo,
    DBVersion as SqliteDBVersion)
from src.models.version import DatabaseVersion, DataVersion


class RaMPVersionInfo:
    db_version: DatabaseVersion
    data_versions: List[DataVersion]

    def initialize(self, session: Session):
        result = session.query(
            SqliteDBVersion.ramp_version,
            SqliteDBVersion.load_timestamp,
            SqliteDBVersion.version_notes
        ).order_by(desc(SqliteDBVersion.ramp_version)).first()

        self.db_version = DatabaseVersion(
            id = result[0],
            timestamp = result[1],
            notes= result[2]
        )

        results = session.query(
            SqliteVersionInfo.data_source_id,
            SqliteVersionInfo.data_source_name,
            SqliteVersionInfo.data_source_url,
            SqliteVersionInfo.data_source_version
        ).filter(SqliteVersionInfo.status == 'current').all()

        self.data_versions: [DataVersion] = [DataVersion(
            id=row[0],
            name=row[1],
            url=row[2],
            version=row[3]
        ) for row in results]