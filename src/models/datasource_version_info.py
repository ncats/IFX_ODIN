from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

def parse_to_date(iso_format_str: str) -> Optional[date]:
    if iso_format_str is None:
        return None
    if iso_format_str == 'None':
        return None
    if len(iso_format_str) > 10:
        dt = datetime.fromisoformat(iso_format_str)
        return date(dt.year, dt.month, dt.day)
    return date.fromisoformat(iso_format_str)

@dataclass
class DatasourceVersionInfo:
    version: str = None
    version_date: date = None
    download_date: date = None


@dataclass
class DataSourceDetails:
    name: str
    version: Optional[str]
    version_date: Optional[date]
    download_date: Optional[date]

    @staticmethod
    def parse_tsv(tsv_str: str) -> "DataSourceDetails":
        name, version, version_date, download_date = tsv_str.split('\t')
        dsv = DataSourceDetails(
            name=name,
            version=version if version else None,
            version_date=parse_to_date(version_date),
            download_date=parse_to_date(download_date)
        )
        return dsv

    def to_tsv(self):
        return f"{self.name}\t{self.version}\t{self.version_date}\t{self.download_date}"