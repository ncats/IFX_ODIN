from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pymysql
from pymysql.cursors import DictCursor

from scripts.pubmed_mirror.source_client import RemoteArchive


class PubMedMirrorStateRepository:
    def is_processed(self, archive_name: str, connection: pymysql.connections.Connection) -> bool:
        with connection.cursor(DictCursor) as cursor:
            cursor.execute(
                "SELECT status FROM pubmed_mirror_file_state WHERE archive_name = %s",
                (archive_name,),
            )
            row = cursor.fetchone()
        return row is not None and row["status"] == "processed"

    def mark_downloaded(
        self,
        archive: RemoteArchive,
        md5_value: str,
        connection: pymysql.connections.Connection,
    ) -> None:
        self._upsert_state(
            archive=archive,
            connection=connection,
            status="downloaded",
            md5_value=md5_value,
            error_message=None,
            downloaded_at=self._utcnow(),
            processed_at=None,
        )

    def mark_processed(
        self,
        archive: RemoteArchive,
        md5_value: str,
        connection: pymysql.connections.Connection,
    ) -> None:
        now = self._utcnow()
        self._upsert_state(
            archive=archive,
            connection=connection,
            status="processed",
            md5_value=md5_value,
            error_message=None,
            downloaded_at=now,
            processed_at=now,
        )

    def mark_failure(
        self,
        archive: RemoteArchive,
        status: str,
        error_message: str,
        connection: pymysql.connections.Connection,
        md5_value: Optional[str] = None,
    ) -> None:
        self._upsert_state(
            archive=archive,
            connection=connection,
            status=status,
            md5_value=md5_value,
            error_message=error_message[:10000],
            downloaded_at=self._utcnow(),
            processed_at=None,
        )

    def _upsert_state(
        self,
        archive: RemoteArchive,
        connection: pymysql.connections.Connection,
        status: str,
        md5_value: Optional[str],
        error_message: Optional[str],
        downloaded_at: Optional[datetime],
        processed_at: Optional[datetime],
    ) -> None:
        sql = """
            INSERT INTO pubmed_mirror_file_state (
                archive_name,
                archive_group,
                remote_last_modified,
                md5,
                downloaded_at,
                processed_at,
                status,
                error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                archive_group = VALUES(archive_group),
                remote_last_modified = VALUES(remote_last_modified),
                md5 = COALESCE(VALUES(md5), md5),
                downloaded_at = COALESCE(VALUES(downloaded_at), downloaded_at),
                processed_at = VALUES(processed_at),
                status = VALUES(status),
                error_message = VALUES(error_message)
        """
        with connection.cursor() as cursor:
            cursor.execute(
                sql,
                (
                    archive.name,
                    archive.group,
                    archive.remote_last_modified,
                    md5_value,
                    downloaded_at,
                    processed_at,
                    status,
                    error_message,
                ),
            )

    @staticmethod
    def _utcnow() -> datetime:
        return datetime.now(timezone.utc).replace(tzinfo=None)
