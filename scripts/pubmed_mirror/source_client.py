from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import List, Optional
from urllib import request


ARCHIVE_NAME_PATTERN = re.compile(r'href="([^"]+\.gz)"')
MD5_PATTERN = re.compile(r"=\s*([0-9a-fA-F]+)")


@dataclass(frozen=True)
class RemoteArchive:
    name: str
    group: str
    url: str
    md5_url: str
    remote_last_modified: Optional[datetime]


class PubMedSourceClient:
    def list_archives(self, base_url: str, archive_group: str) -> List[RemoteArchive]:
        response = request.urlopen(base_url)
        html = response.read().decode("utf-8")
        last_modified = self._parse_last_modified(response.headers.get("Last-Modified"))
        archive_names = sorted(set(ARCHIVE_NAME_PATTERN.findall(html)))
        return [
            RemoteArchive(
                name=archive_name,
                group=archive_group,
                url=f"{base_url}{archive_name}",
                md5_url=f"{base_url}{archive_name}.md5",
                remote_last_modified=last_modified,
            )
            for archive_name in archive_names
        ]

    def download_archive(self, archive: RemoteArchive, target_dir: Path) -> tuple[Path, str]:
        target_dir.mkdir(parents=True, exist_ok=True)
        archive_path = target_dir / archive.name
        md5_path = target_dir / f"{archive.name}.md5"
        request.urlretrieve(archive.url, archive_path)
        request.urlretrieve(archive.md5_url, md5_path)
        md5_value = self._read_expected_md5(md5_path)
        actual_md5 = self._calculate_md5(archive_path)
        if md5_value.lower() != actual_md5.lower():
            raise ValueError(
                f"Checksum mismatch for {archive.name}: expected {md5_value}, got {actual_md5}"
            )
        return archive_path, actual_md5

    @staticmethod
    def _read_expected_md5(md5_path: Path) -> str:
        match = MD5_PATTERN.search(md5_path.read_text(encoding="utf-8"))
        if not match:
            raise ValueError(f"Could not parse MD5 from {md5_path}")
        return match.group(1)

    @staticmethod
    def _calculate_md5(archive_path: Path) -> str:
        digest = hashlib.md5()
        with archive_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _parse_last_modified(raw_value: Optional[str]) -> Optional[datetime]:
        if not raw_value:
            return None
        parsed = parsedate_to_datetime(raw_value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
