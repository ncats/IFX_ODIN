from pathlib import Path

from scripts.pubmed_mirror.service import PubMedMirrorService
from scripts.pubmed_mirror.source_client import RemoteArchive


class DummyConfig:
    limit_archives = None


class DummyRepository:
    def __init__(self):
        self.entered_transactions = 0

    def transaction(self):
        return self

    def __enter__(self):
        self.entered_transactions += 1
        return object()

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyStateRepository:
    def __init__(self, processed: bool = False):
        self.processed = processed
        self.calls = []

    def is_processed(self, archive_name: str, connection) -> bool:
        self.calls.append(("is_processed", archive_name))
        return self.processed

    def mark_downloaded(self, archive: RemoteArchive, md5_value: str, connection) -> None:
        self.calls.append(("mark_downloaded", archive.name, md5_value))

    def mark_processed(self, archive: RemoteArchive, md5_value: str, connection) -> None:
        self.calls.append(("mark_processed", archive.name, md5_value))

    def mark_failure(self, archive: RemoteArchive, status: str, error_message: str, connection) -> None:
        self.calls.append(("mark_failure", archive.name, status, error_message))


class DummySourceClient:
    def __init__(self, archive_path: Path, md5_value: str = "abc123"):
        self.archive_path = archive_path
        self.md5_value = md5_value

    def download_archive(self, archive: RemoteArchive, target_dir: Path):
        return self.archive_path, self.md5_value


def test_process_archives_removes_downloaded_files_after_success(tmp_path):
    archive = RemoteArchive(
        name="pubmed26n1335.xml.gz",
        group="update",
        url="https://example.test/pubmed26n1335.xml.gz",
        md5_url="https://example.test/pubmed26n1335.xml.gz.md5",
        remote_last_modified=None,
    )
    archive_path = tmp_path / archive.name
    archive_path.write_text("payload", encoding="utf-8")
    archive_path.with_name(f"{archive.name}.md5").write_text("checksum", encoding="utf-8")

    service = PubMedMirrorService(
        config=DummyConfig(),
        source_client=DummySourceClient(archive_path),
        repository=DummyRepository(),
        state_repository=DummyStateRepository(),
    )
    processed = []
    service._process_archive_file = lambda connection, archive, archive_path, md5_value: processed.append(
        (archive.name, archive_path, md5_value)
    )

    service._process_archives([archive], tmp_path)

    assert processed == [(archive.name, archive_path, "abc123")]
    assert not archive_path.exists()
    assert not archive_path.with_name(f"{archive.name}.md5").exists()


def test_process_archives_keeps_downloaded_files_on_failure(tmp_path):
    archive = RemoteArchive(
        name="pubmed26n1335.xml.gz",
        group="update",
        url="https://example.test/pubmed26n1335.xml.gz",
        md5_url="https://example.test/pubmed26n1335.xml.gz.md5",
        remote_last_modified=None,
    )
    archive_path = tmp_path / archive.name
    archive_path.write_text("payload", encoding="utf-8")
    archive_path.with_name(f"{archive.name}.md5").write_text("checksum", encoding="utf-8")

    state_repository = DummyStateRepository()
    service = PubMedMirrorService(
        config=DummyConfig(),
        source_client=DummySourceClient(archive_path),
        repository=DummyRepository(),
        state_repository=state_repository,
    )

    def fail(*args, **kwargs):
        raise RuntimeError("boom")

    service._process_archive_file = fail

    try:
        service._process_archives([archive], tmp_path)
    except RuntimeError as exc:
        assert str(exc) == "boom"
    else:
        raise AssertionError("Expected RuntimeError")

    assert archive_path.exists()
    assert archive_path.with_name(f"{archive.name}.md5").exists()
    assert ("mark_failure", archive.name, "failed", "boom") in state_repository.calls
