from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, Optional

import pymysql
import yaml
from pymysql.cursors import DictCursor

from scripts.pubmed_mirror.config import MirrorConfig
from scripts.pubmed_mirror.parser import ArticleRecord, MeshAssignment
from src.shared.db_credentials import DBCredentials


class PubMedRepository:
    def __init__(self, config: MirrorConfig):
        self.config = config
        with config.credentials_yaml.open("r", encoding="utf-8") as handle:
            self.credentials = DBCredentials.from_yaml(yaml.safe_load(handle))

    def ensure_database(self) -> None:
        with self._connect(use_schema=False) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{self.config.schema_name}` "
                    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
            connection.commit()

    def ensure_schema(self) -> None:
        self.ensure_database()
        schema_sql = (Path(__file__).with_name("schema.sql")).read_text(encoding="utf-8")
        with self._connect() as connection:
            with connection.cursor() as cursor:
                for statement in self._split_sql_statements(schema_sql):
                    cursor.execute(statement)
                self._ensure_pubmed_columns(cursor)
            connection.commit()

    def truncate_all(self) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
                cursor.execute("TRUNCATE TABLE `pubmed_mesh`")
                cursor.execute("TRUNCATE TABLE `mesh_qualifier`")
                cursor.execute("TRUNCATE TABLE `mesh_descriptor`")
                cursor.execute("TRUNCATE TABLE `pubmed`")
                cursor.execute("TRUNCATE TABLE `pubmed_mirror_file_state`")
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            connection.commit()

    @contextmanager
    def transaction(self) -> Iterator[pymysql.connections.Connection]:
        connection = self._connect()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def upsert_articles(
        self,
        connection: pymysql.connections.Connection,
        articles: Iterable[ArticleRecord],
        fetch_date: datetime,
        source_file: str,
        batch_size: int,
    ) -> int:
        article_list = list(articles)
        article_rows = [
            (
                article.pmid,
                article.title,
                article.journal,
                article.publication_date,
                article.pub_year,
                article.authors,
                article.abstract,
                article.pmc_id,
                article.doi,
                article.publication_status,
                article.publication_type,
                article.language,
                fetch_date,
                source_file,
            )
            for article in article_list
        ]
        if not article_rows:
            return 0

        sql = """
            INSERT INTO pubmed (
                id, title, journal, date, pub_year, authors, abstract, pmc_id, doi,
                publication_status, publication_type, language, fetch_date, source_file
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                title = VALUES(title),
                journal = VALUES(journal),
                date = VALUES(date),
                pub_year = VALUES(pub_year),
                authors = VALUES(authors),
                abstract = VALUES(abstract),
                pmc_id = VALUES(pmc_id),
                doi = VALUES(doi),
                publication_status = VALUES(publication_status),
                publication_type = VALUES(publication_type),
                language = VALUES(language),
                fetch_date = VALUES(fetch_date),
                source_file = VALUES(source_file)
        """
        with connection.cursor() as cursor:
            for offset in range(0, len(article_rows), batch_size):
                cursor.executemany(sql, article_rows[offset:offset + batch_size])
        self._replace_mesh_assignments(
            connection=connection,
            articles=article_list,
            batch_size=batch_size,
        )
        return len(article_rows)

    def _replace_mesh_assignments(
        self,
        connection: pymysql.connections.Connection,
        articles: list[ArticleRecord],
        batch_size: int,
    ) -> None:
        pmids = [article.pmid for article in articles]
        if not pmids:
            return

        descriptor_rows: list[tuple[str, str]] = []
        qualifier_rows: list[tuple[str, str]] = []
        mesh_rows: list[tuple[int, str, Optional[str], bool, Optional[bool]]] = []
        seen_descriptors: set[str] = set()
        seen_qualifiers: set[str] = set()

        for article in articles:
            for assignment in article.mesh_assignments:
                self._add_descriptor_row(assignment, descriptor_rows, seen_descriptors)
                self._add_qualifier_row(assignment, qualifier_rows, seen_qualifiers)
                mesh_rows.append(
                    (
                        article.pmid,
                        assignment.descriptor_ui,
                        assignment.qualifier_ui,
                        assignment.descriptor_major_topic,
                        assignment.qualifier_major_topic,
                    )
                )

        with connection.cursor() as cursor:
            for offset in range(0, len(pmids), batch_size):
                chunk = pmids[offset:offset + batch_size]
                placeholders = ", ".join(["%s"] * len(chunk))
                cursor.execute(f"DELETE FROM pubmed_mesh WHERE pmid IN ({placeholders})", chunk)

            self._executemany(
                cursor=cursor,
                sql="""
                    INSERT INTO mesh_descriptor (ui, name)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE name = VALUES(name)
                """,
                rows=descriptor_rows,
                batch_size=batch_size,
            )
            self._executemany(
                cursor=cursor,
                sql="""
                    INSERT INTO mesh_qualifier (ui, name)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE name = VALUES(name)
                """,
                rows=qualifier_rows,
                batch_size=batch_size,
            )
            self._executemany(
                cursor=cursor,
                sql="""
                    INSERT INTO pubmed_mesh (
                        pmid, descriptor_ui, qualifier_ui,
                        descriptor_major_topic, qualifier_major_topic
                    )
                    VALUES (%s, %s, %s, %s, %s)
                """,
                rows=mesh_rows,
                batch_size=batch_size,
            )

    @staticmethod
    def _add_descriptor_row(
        assignment: MeshAssignment,
        descriptor_rows: list[tuple[str, str]],
        seen_descriptors: set[str],
    ) -> None:
        if assignment.descriptor_ui in seen_descriptors:
            return
        seen_descriptors.add(assignment.descriptor_ui)
        descriptor_rows.append((assignment.descriptor_ui, assignment.descriptor_name))

    @staticmethod
    def _add_qualifier_row(
        assignment: MeshAssignment,
        qualifier_rows: list[tuple[str, str]],
        seen_qualifiers: set[str],
    ) -> None:
        if assignment.qualifier_ui is None or assignment.qualifier_name is None:
            return
        if assignment.qualifier_ui in seen_qualifiers:
            return
        seen_qualifiers.add(assignment.qualifier_ui)
        qualifier_rows.append((assignment.qualifier_ui, assignment.qualifier_name))

    @staticmethod
    def _executemany(
        cursor,
        sql: str,
        rows: list[tuple],
        batch_size: int,
    ) -> None:
        if not rows:
            return
        for offset in range(0, len(rows), batch_size):
            cursor.executemany(sql, rows[offset:offset + batch_size])

    def delete_articles(
        self,
        connection: pymysql.connections.Connection,
        pmids: Iterable[int],
        batch_size: int,
    ) -> int:
        pmid_list = list(pmids)
        if not pmid_list:
            return 0

        deleted = 0
        with connection.cursor() as cursor:
            for offset in range(0, len(pmid_list), batch_size):
                chunk = pmid_list[offset:offset + batch_size]
                placeholders = ", ".join(["%s"] * len(chunk))
                cursor.execute(f"DELETE FROM pubmed WHERE id IN ({placeholders})", chunk)
                deleted += cursor.rowcount
        return deleted

    def fetch_status(self) -> dict:
        with self._connect() as connection:
            with connection.cursor(DictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS article_count,
                        MAX(pub_year) AS max_pub_year,
                        MAX(fetch_date) AS latest_fetch_date
                    FROM pubmed
                    """
                )
                article_status = cursor.fetchone()
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS tracked_archives,
                        SUM(CASE WHEN status = 'processed' THEN 1 ELSE 0 END) AS processed_archives,
                        SUM(CASE WHEN status IN ('failed', 'checksum_failed') THEN 1 ELSE 0 END) AS failed_archives,
                        MAX(processed_at) AS latest_processed_at
                    FROM pubmed_mirror_file_state
                    """
                )
                file_status = cursor.fetchone()
        return {
            "article_count": article_status["article_count"],
            "max_pub_year": article_status["max_pub_year"],
            "latest_fetch_date": article_status["latest_fetch_date"],
            "tracked_archives": file_status["tracked_archives"],
            "processed_archives": file_status["processed_archives"] or 0,
            "failed_archives": file_status["failed_archives"] or 0,
            "latest_processed_at": file_status["latest_processed_at"],
        }

    def _connect(
        self,
        database: Optional[str] = None,
        use_schema: bool = True,
    ) -> pymysql.connections.Connection:
        connection_kwargs = {
            "host": self.credentials.url,
            "user": self.credentials.user,
            "password": self.credentials.password,
            "port": self.credentials.port or 3306,
            "charset": "utf8mb4",
            "cursorclass": DictCursor,
            "autocommit": False,
        }
        if database is not None:
            connection_kwargs["database"] = database
        elif use_schema:
            connection_kwargs["database"] = self.config.schema_name
        return pymysql.connect(**connection_kwargs)

    @staticmethod
    def _split_sql_statements(schema_sql: str) -> list[str]:
        return [statement.strip() for statement in schema_sql.split(";") if statement.strip()]

    @staticmethod
    def _ensure_pubmed_columns(cursor) -> None:
        required_columns = {
            "pmc_id": "ALTER TABLE `pubmed` ADD COLUMN `pmc_id` VARCHAR(16) NULL",
            "doi": "ALTER TABLE `pubmed` ADD COLUMN `doi` VARCHAR(255) NULL",
            "publication_status": "ALTER TABLE `pubmed` ADD COLUMN `publication_status` VARCHAR(64) NULL",
            "publication_type": "ALTER TABLE `pubmed` ADD COLUMN `publication_type` TEXT NULL",
            "language": "ALTER TABLE `pubmed` ADD COLUMN `language` VARCHAR(32) NULL",
        }
        cursor.execute("SHOW COLUMNS FROM `pubmed`")
        existing_columns = {row["Field"] for row in cursor.fetchall()}
        for column_name, statement in required_columns.items():
            if column_name in existing_columns:
                continue
            cursor.execute(statement)
