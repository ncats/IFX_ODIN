from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


DEFAULT_BASELINE_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
DEFAULT_UPDATE_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"


@dataclass(frozen=True)
class MirrorConfig:
    credentials_yaml: Path
    schema_name: str
    data_dir: Path
    baseline_url: str = DEFAULT_BASELINE_URL
    update_url: str = DEFAULT_UPDATE_URL
    batch_size: int = 1000
    limit_archives: int | None = None

    @property
    def baseline_dir(self) -> Path:
        return self.data_dir / "baseline"

    @property
    def update_dir(self) -> Path:
        return self.data_dir / "updatefiles"

    def ensure_directories(self) -> None:
        self.baseline_dir.mkdir(parents=True, exist_ok=True)
        self.update_dir.mkdir(parents=True, exist_ok=True)
