from dataclasses import dataclass
from datetime import date


@dataclass
class DatasourceVersionInfo:
    version: str = None
    version_date: date = None
    download_date: date = None
