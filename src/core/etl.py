import datetime
import time
from dataclasses import dataclass, field
from typing import List, Dict
import humanize

from src.interfaces.id_resolver import IdResolver
from src.interfaces.input_adapter import InputAdapter
from src.interfaces.labeler import Labeler
from src.interfaces.output_adapter import OutputAdapter


@dataclass
class ETL:
    input_adapters: List[InputAdapter]
    output_adapters: List[OutputAdapter]
    resolver_map: Dict[str, IdResolver] = field(default_factory=dict)
    labeler: Labeler = Labeler()

    def set_labeler(self, labeler: Labeler):
        self.labeler = labeler
        return self

    def create_or_truncate_datastores(self):
        for output_adapter in self.output_adapters:
            if not output_adapter.create_or_truncate_datastore():
                raise Exception("operation cancelled")

    def do_etl(self, do_post_processing = True):
        total_start_time = time.time()
        for input_adapter in self.input_adapters:
            start_time = time.time()
            print(f"Running: {input_adapter.get_name()}")
            count = 0
            for resolved_list in input_adapter.get_resolved_and_provenanced_list(resolver_map = self.resolver_map):
                count += len(resolved_list)
                for output_adapter in self.output_adapters:
                    resolved_list = output_adapter.preprocess_objects(resolved_list)
                    self.labeler.assign_all_labels(resolved_list)
                    output_adapter.store(resolved_list)

            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"\tElapsed time: {elapsed_time:.4f} seconds merging {count} records")

        if do_post_processing:
            for output_adapter in self.output_adapters:
                output_adapter.do_post_processing()

        total_elapsed_time = time.time() - total_start_time
        elapsed_timedelta = datetime.timedelta(seconds=total_elapsed_time)

        # Format the elapsed time using humanize
        formatted_time = humanize.precisedelta(elapsed_timedelta, format='%0.0f')

        print(f"\tTotal elapsed time: {formatted_time}")
