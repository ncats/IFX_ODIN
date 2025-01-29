import csv
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List


class CSVParser(ABC):
    file_path: str

    @abstractmethod
    def get_required_columns(self) -> List[str]:
        raise Exception("Derived classes must define the required_columns used for parsing")

    def __init__(self, file_path):
        self.file_path = file_path
        # self.validate_csv()

    def validate_csv(self):
        required_columns = self.get_required_columns()
        first_obj = next(self.all_rows())

        missing_columns = []
        extra_columns = []

        for column in required_columns:
            if column not in first_obj:
                missing_columns.append(column)

        for prop in first_obj:
            if prop not in required_columns:
                extra_columns.append(prop)

        if len(missing_columns) > 0:
            print(f"the following columns are missing from the input file")
            print(missing_columns)
        if len(extra_columns) > 0:
            print(f"the following columns are not used by the parser")
            print(extra_columns)
        if len(missing_columns) > 0:
            raise Exception("File format may have changed - there are missing required columns")

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
        print(f"reading file at {path}")
        with open(path, 'r') as gene_file:
            reader: csv.DictReader = csv.DictReader(gene_file)
            for line in reader:
                yield {k: v for k, v in line.items() if v != ''}
