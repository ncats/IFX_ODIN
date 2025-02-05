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

    def do_etl(self, testing = False):
        total_start_time = time.time()
        for input_adapter in self.input_adapters:
            print(f"Running: {input_adapter.get_name()}")

            resolved_list = input_adapter.get_resolved_and_provenanced_list()
            if testing:
                resolved_list = resolved_list[0:20000]

            self.labeler.assign_all_labels(resolved_list)

            for output_adapter in self.output_adapters:
                output_adapter.store(resolved_list)

        for output_adapter in self.output_adapters:
            output_adapter.do_post_processing()

        total_elapsed_time = time.time() - total_start_time
        elapsed_timedelta = datetime.timedelta(seconds=total_elapsed_time)

        # Format the elapsed time using humanize
        formatted_time = humanize.precisedelta(elapsed_timedelta, format='%0.0f')

        print(f"\tTotal elapsed time: {formatted_time}")
