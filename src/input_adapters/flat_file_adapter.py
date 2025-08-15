import os
from abc import ABC
from datetime import date, datetime

from src.interfaces.input_adapter import InputAdapter


class FlatFileAdapter(InputAdapter, ABC):
    file_path: str
    download_date: date

    def __init__(self, file_path):
        InputAdapter.__init__(self)
        self.file_path = file_path
        self.download_date = datetime.fromtimestamp(os.path.getmtime(file_path)).date()
