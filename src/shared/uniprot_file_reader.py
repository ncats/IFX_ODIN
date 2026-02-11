import os
import gzip
import json
from datetime import datetime, date

from src.models.datasource_version_info import DatasourceVersionInfo


class UniProtFileReader:
    file_path: str
    raw_entries: list | None
    download_date: date

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.raw_entries = None

        self.download_date = datetime.fromtimestamp(os.path.getmtime(file_path)).date()

    def get_version_info(self) -> DatasourceVersionInfo:
        if self.version_info is None:
            self.read_uniprot_file()
        return self.version_info

    def next(self):
        if self.raw_entries is None:
            self.read_uniprot_file()

        for entry in self.raw_entries:
            yield entry

    def read_uniprot_file(self):
        path = os.path.expanduser(self.file_path)
        print(f"reading file at {path}")
        with gzip.open(path, 'rb') as gzip_file:
            self.version_info = DatasourceVersionInfo(download_date=self.download_date)
            content = json.loads(gzip_file.read().decode('utf-8'))
            self.raw_entries = content["results"]
