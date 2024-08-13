import datetime
import time
from dataclasses import dataclass
from typing import List
import humanize
from src.interfaces.input_adapter import InputAdapter
from src.interfaces.labeler import Labeler
from src.interfaces.output_adapter import OutputAdapter


@dataclass
class ETL:
    input_adapters: List[InputAdapter]
    output_adapters: List[OutputAdapter]
    labeler: Labeler = Labeler()

    def set_labeler(self, labeler: Labeler):
        self.labeler = labeler
        return self

    def create_or_truncate_datastores(self):
        for output_adapter in self.output_adapters:
            if not output_adapter.create_or_truncate_datastore():
                raise Exception("operation cancelled")

    def do_etl(self):
        total_start_time = time.time()
        for input_adapter in self.input_adapters:
            print(f"Running: {input_adapter.name}")

            normalized_list = input_adapter.get_normalized_and_provenanced_list()
            self.labeler.assign_all_labels(normalized_list)

            for output_adapter in self.output_adapters:
                output_adapter.store(normalized_list)

        total_elapsed_time = time.time() - total_start_time
        elapsed_timedelta = datetime.timedelta(seconds=total_elapsed_time)

        # Format the elapsed time using humanize
        formatted_time = humanize.precisedelta(elapsed_timedelta, format='%0.0f')

        print(f"\tTotal elapsed time: {formatted_time}")
