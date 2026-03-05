import os
import gzip
import json
from datetime import datetime, date

class UniProtFileReader:
    file_path: str
    raw_entries: list | None
    download_date: date

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.raw_entries = None

        self.download_date = datetime.fromtimestamp(os.path.getmtime(file_path)).date()

    def next(self):
        if self.raw_entries is None:
            self.read_uniprot_file()

        for entry in self.raw_entries:
            yield entry

    def read_uniprot_file(self):
        path = os.path.expanduser(self.file_path)
        print(f"reading file at {path}")
        with gzip.open(path, 'rb') as gzip_file:
            content = json.loads(gzip_file.read().decode('utf-8'))
            self.raw_entries = content["results"]
