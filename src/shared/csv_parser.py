import csv
import os
from datetime import datetime, date


class CSVParser:
    file_path: str
    download_date: date

    def __init__(self, file_path):
        self.file_path = file_path
        self.download_date = datetime.fromtimestamp(os.path.getmtime(file_path)).date()

    @staticmethod
    def parse_excel_date(excel_date: str):
        formats = [
            "%Y-%m-%d %H:%M:%S.%f",  # First try: most specific (with microseconds)
            "%Y-%m-%d %H:%M:%S",     # Second: standard format with seconds
            "%Y-%m-%d %H:%M"         # Third: basic format with only minutes
        ]
        for date_format in formats:
            try:
                return datetime.strptime(excel_date, date_format)
            except ValueError:
                continue  # Try the next format if current fails
        raise ValueError(f"Date format for '{excel_date}' is not recognized")

    def all_rows(self):
        path = os.path.expanduser(self.file_path)
        with open(path, 'r') as gene_file:
            reader: csv.DictReader = csv.DictReader(gene_file)
            for line in reader:
                yield {k: v for k, v in line.items() if v != ''}
