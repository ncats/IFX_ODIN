from __future__ import annotations

import gzip
from datetime import datetime, timezone
from pathlib import Path

from scripts.pubmed_mirror.config import MirrorConfig
from scripts.pubmed_mirror.parser import PubMedParser
from scripts.pubmed_mirror.repository import PubMedRepository
from scripts.pubmed_mirror.source_client import PubMedSourceClient, RemoteArchive
from scripts.pubmed_mirror.state_repository import PubMedMirrorStateRepository


class PubMedMirrorService:
    def __init__(
        self,
        config: MirrorConfig,
        source_client: PubMedSourceClient | None = None,
        repository: PubMedRepository | None = None,
        state_repository: PubMedMirrorStateRepository | None = None,
    ):
        self.config = config
        self.source_client = source_client or PubMedSourceClient()
        self.repository = repository or PubMedRepository(config)
        self.state_repository = state_repository or PubMedMirrorStateRepository()

    def init(self) -> None:
        self.config.ensure_directories()
        self.repository.ensure_schema()

    def rebuild(self) -> None:
        self.init()
        self.repository.truncate_all()
        baseline_archives = self.source_client.list_archives(self.config.baseline_url, archive_group="baseline")
        self._process_archives(baseline_archives, self.config.baseline_dir)

    def update(self) -> None:
        self.init()
        update_archives = self.source_client.list_archives(self.config.update_url, archive_group="update")
        self._process_archives(update_archives, self.config.update_dir)

    def status(self) -> dict:
        self.init()
        return self.repository.fetch_status()

    def _process_archives(self, archives: list[RemoteArchive], target_dir: Path) -> None:
        remaining = self.config.limit_archives

        for archive in sorted(archives, key=lambda item: item.name):
            if remaining is not None and remaining <= 0:
                break
            archive_path: Path | None = None
            try:
                with self.repository.transaction() as connection:
                    if self.state_repository.is_processed(archive.name, connection):
                        continue
                    archive_path, md5_value = self.source_client.download_archive(archive, target_dir)
                    self.state_repository.mark_downloaded(archive, md5_value, connection)
                    self._process_archive_file(connection, archive, archive_path, md5_value)
                    self.state_repository.mark_processed(archive, md5_value, connection)
                    if remaining is not None:
                        remaining -= 1
                if archive_path is not None:
                    self._cleanup_downloaded_archive(archive_path)
            except ValueError as exc:
                with self.repository.transaction() as connection:
                    self.state_repository.mark_failure(
                        archive=archive,
                        status="checksum_failed",
                        error_message=str(exc),
                        connection=connection,
                    )
                raise
            except Exception as exc:
                with self.repository.transaction() as connection:
                    self.state_repository.mark_failure(
                        archive=archive,
                        status="failed",
                        error_message=str(exc),
                        connection=connection,
                    )
                raise

    def _process_archive_file(
        self,
        connection,
        archive: RemoteArchive,
        archive_path: Path,
        md5_value: str,
    ) -> None:
        xml_text = self._read_archive_text(archive_path)
        articles = PubMedParser.parse_articles(xml_text)
        deleted_pmids = PubMedParser.parse_deleted_pmids(xml_text)
        fetch_date = self._archive_fetch_date(archive)

        inserted = self.repository.upsert_articles(
            connection=connection,
            articles=articles,
            fetch_date=fetch_date,
            source_file=archive.name,
            batch_size=self.config.batch_size,
        )
        deleted = self.repository.delete_articles(
            connection=connection,
            pmids=deleted_pmids,
            batch_size=self.config.batch_size,
        )
        print(
            f"Processed {archive.name}: "
            f"{inserted} article upserts, {deleted} deletions, md5={md5_value}"
        )

    @staticmethod
    def _read_archive_text(archive_path: Path) -> str:
        with gzip.open(archive_path, "rt", encoding="utf-8") as handle:
            return handle.read()

    @staticmethod
    def _archive_fetch_date(archive: RemoteArchive) -> datetime:
        if archive.remote_last_modified is not None:
            return archive.remote_last_modified.astimezone(timezone.utc).replace(tzinfo=None)
        return datetime.now(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _cleanup_downloaded_archive(archive_path: Path) -> None:
        archive_path.unlink(missing_ok=True)
        archive_path.with_name(f"{archive_path.name}.md5").unlink(missing_ok=True)
