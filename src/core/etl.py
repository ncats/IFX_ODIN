import time
from dataclasses import dataclass
from typing import List

from src.interfaces.input_adapter import InputAdapter
from src.interfaces.output_adapter import OutputAdapter


@dataclass
class ETL:
    input_adapters: List[InputAdapter]
    output_adapters: List[OutputAdapter]

    def create_or_truncate_datastores(self):
        for output_adapter in self.output_adapters:
            if not output_adapter.create_or_truncate_datastore():
                raise Exception("operation cancelled")

    def do_etl(self):
        total_start_time = time.time()
        for input_adapter in self.input_adapters:
            print(f"Running: {input_adapter.name}")

            normalized_list = input_adapter.get_normalized_and_provenanced_list()
            for output_adapter in self.output_adapters:
                output_adapter.store(normalized_list)

        total_elapsed_time = time.time() - total_start_time
        print(f"\tTotal elapsed time: {total_elapsed_time:.4f} seconds")
